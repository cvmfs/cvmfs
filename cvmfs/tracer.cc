/**
 * This file is part of the CernVM File System.
 */


#include "tracer.h"

#include <pthread.h>

#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "util/atomic.h"
#include "util/concurrency.h"
#include "util/posix.h"
#include "util/string.h"

using namespace std;  // NOLINT


void Tracer::Activate(const int buffer_size,
                      const int flush_threshold,
                      const string &trace_file) {
  trace_file_ = trace_file;
  buffer_size_ = buffer_size;
  flush_threshold_ = flush_threshold;
  assert(buffer_size_ > 1 && flush_threshold_ >= 0
         && flush_threshold_ < buffer_size_);

  ring_buffer_ = new BufferEntry[buffer_size_];
  commit_buffer_ = new atomic_int32[buffer_size_];
  for (int i = 0; i < buffer_size_; i++)
    atomic_init32(&commit_buffer_[i]);

  int retval;
  retval = pthread_cond_init(&sig_continue_trace_, NULL);
  retval |= pthread_mutex_init(&sig_continue_trace_mutex_, NULL);
  retval |= pthread_cond_init(&sig_flush_, NULL);
  retval |= pthread_mutex_init(&sig_flush_mutex_, NULL);
  assert(retval == 0);

  active_ = true;
}


/**
 * Trace a message.  This is usually a lock-free procedure that just
 * requires two fetch_and_add operations and a gettimeofday syscall.
 * There are two exceptions:
 *   -# If the ring buffer is full, the function blocks until the flush
 *      thread made some space.  Avoid that by carefully choosing size
 *      and threshold.
 *   -# If this message reaches the threshold, the flush thread gets
 *      signaled.
 *
 * \param[in] event Arbitrary code, for consistency applications should use one
 *            of the TraceEvents constants. Negative codes are reserved
 *            for internal use.
 * \param[in] id Arbitrary id, for example file name or module name which is
 *            doing the trace.
 * \return The sequence number which was used to trace the record
 */
int32_t Tracer::DoTrace(const int event,
                        const PathString &path,
                        const string &msg) {
  const int32_t my_seq_no = atomic_xadd32(&seq_no_, 1);
  timeval now;
  gettimeofday(&now, NULL);
  const int pos = my_seq_no % buffer_size_;

  while (my_seq_no - atomic_read32(&flushed_) >= buffer_size_) {
    timespec timeout;
    int retval;
    GetTimespecRel(25, &timeout);
    retval = pthread_mutex_lock(&sig_continue_trace_mutex_);
    retval |= pthread_cond_timedwait(&sig_continue_trace_,
                                     &sig_continue_trace_mutex_, &timeout);
    retval |= pthread_mutex_unlock(&sig_continue_trace_mutex_);
    assert(retval == ETIMEDOUT || retval == 0);
  }

  ring_buffer_[pos].time_stamp = now;
  ring_buffer_[pos].code = event;
  ring_buffer_[pos].path = path;
  ring_buffer_[pos].msg = msg;
  atomic_inc32(&commit_buffer_[pos]);

  if (my_seq_no - atomic_read32(&flushed_) == flush_threshold_) {
    const MutexLockGuard m(&sig_flush_mutex_);
    const int err_code
        __attribute__((unused)) = pthread_cond_signal(&sig_flush_);
    assert(err_code == 0 && "Could not signal flush thread");
  }

  return my_seq_no;
}


void Tracer::Flush() {
  if (!active_)
    return;

  const int32_t save_seq_no = DoTrace(kEventFlush, PathString("Tracer", 6),
                                      "flushed ring buffer");
  while (atomic_read32(&flushed_) <= save_seq_no) {
    timespec timeout;
    int retval;

    atomic_cas32(&flush_immediately_, 0, 1);
    {
      const MutexLockGuard m(&sig_flush_mutex_);
      retval = pthread_cond_signal(&sig_flush_);
      assert(retval == 0);
    }

    GetTimespecRel(250, &timeout);
    retval = pthread_mutex_lock(&sig_continue_trace_mutex_);
    retval |= pthread_cond_timedwait(&sig_continue_trace_,
                                     &sig_continue_trace_mutex_, &timeout);
    retval |= pthread_mutex_unlock(&sig_continue_trace_mutex_);
    assert(retval == ETIMEDOUT || retval == 0);
  }
}


void Tracer::GetTimespecRel(const int64_t ms, timespec *ts) {
  timeval now;
  gettimeofday(&now, NULL);
  int64_t nsecs = now.tv_usec * 1000 + (ms % 1000) * 1000 * 1000;
  int carry = 0;
  if (nsecs >= 1000 * 1000 * 1000) {
    carry = 1;
    nsecs -= 1000 * 1000 * 1000;
  }
  ts->tv_sec = now.tv_sec + ms / 1000 + carry;
  ts->tv_nsec = nsecs;
}


void *Tracer::MainFlush(void *data) {
  Tracer *tracer = reinterpret_cast<Tracer *>(data);
  int retval;
  const MutexLockGuard m(&tracer->sig_flush_mutex_);
  FILE *f = fopen(tracer->trace_file_.c_str(), "a");
  assert(f != NULL && "Could not open trace file");
  struct timespec timeout;

  do {
    while (
        (atomic_read32(&tracer->terminate_flush_thread_) == 0)
        && (atomic_read32(&tracer->flush_immediately_) == 0)
        && (atomic_read32(&tracer->seq_no_) - atomic_read32(&tracer->flushed_)
            <= tracer->flush_threshold_)) {
      tracer->GetTimespecRel(2000, &timeout);
      retval = pthread_cond_timedwait(&tracer->sig_flush_,
                                      &tracer->sig_flush_mutex_, &timeout);
      assert(retval != EINVAL);
    }

    const int base = atomic_read32(&tracer->flushed_) % tracer->buffer_size_;
    int pos, i = 0;
    while ((i <= tracer->flush_threshold_)
           && (atomic_read32(
                   &tracer->commit_buffer_[pos = ((base + i)
                                                  % tracer->buffer_size_)])
               == 1)) {
      string tmp;
      tmp = StringifyTimeval(tracer->ring_buffer_[pos].time_stamp);
      retval = tracer->WriteCsvFile(f, tmp);
      retval |= fputc(',', f) - ',';
      tmp = StringifyInt(tracer->ring_buffer_[pos].code);
      retval = tracer->WriteCsvFile(f, tmp);
      retval |= fputc(',', f) - ',';
      retval |= tracer->WriteCsvFile(f,
                                     tracer->ring_buffer_[pos].path.ToString());
      retval |= fputc(',', f) - ',';
      retval |= tracer->WriteCsvFile(f, tracer->ring_buffer_[pos].msg);
      retval |= (fputc(13, f) - 13) | (fputc(10, f) - 10);
      assert(retval == 0);

      atomic_dec32(&tracer->commit_buffer_[pos]);
      ++i;
    }
    retval = fflush(f);
    assert(retval == 0);
    atomic_xadd32(&tracer->flushed_, i);
    atomic_cas32(&tracer->flush_immediately_, 1, 0);

    {
      const MutexLockGuard l(&tracer->sig_continue_trace_mutex_);
      retval = pthread_cond_broadcast(&tracer->sig_continue_trace_);
      assert(retval == 0);
    }
  } while (
      (atomic_read32(&tracer->terminate_flush_thread_) == 0)
      || (atomic_read32(&tracer->flushed_) < atomic_read32(&tracer->seq_no_)));

  retval = fclose(f);
  assert(retval == 0);
  return NULL;
}


void Tracer::Spawn() {
  if (active_) {
    const int retval = pthread_create(&thread_flush_, NULL, MainFlush, this);
    assert(retval == 0);

    spawned_ = true;
    DoTrace(kEventStart, PathString("Tracer", 6), "Trace buffer created");
  }
}


Tracer::Tracer()
    : active_(false)
    , spawned_(false)
    , buffer_size_(0)
    , flush_threshold_(0)
    , ring_buffer_(NULL)
    , commit_buffer_(NULL) {
  memset(&thread_flush_, 0, sizeof(thread_flush_));
  atomic_init32(&seq_no_);
  atomic_init32(&flushed_);
  atomic_init32(&terminate_flush_thread_);
  atomic_init32(&flush_immediately_);
}


Tracer::~Tracer() {
  if (!active_)
    return;
  int retval;

  if (spawned_) {
    DoTrace(kEventStop, PathString("Tracer", 6), "Destroying trace buffer...");

    // Trigger flushing and wait for it
    atomic_inc32(&terminate_flush_thread_);
    {
      const MutexLockGuard m(&sig_flush_mutex_);
      retval = pthread_cond_signal(&sig_flush_);
      assert(retval == 0);
    }
    retval = pthread_join(thread_flush_, NULL);
    assert(retval == 0);
  }

  retval = pthread_cond_destroy(&sig_continue_trace_);
  retval |= pthread_mutex_destroy(&sig_continue_trace_mutex_);
  retval |= pthread_cond_destroy(&sig_flush_);
  retval |= pthread_mutex_destroy(&sig_flush_mutex_);
  assert(retval == 0);

  delete[] ring_buffer_;
  delete[] commit_buffer_;
}


int Tracer::WriteCsvFile(FILE *fp, const string &field) {
  if (fp == NULL)
    return 0;

  int retval;

  if ((retval = fputc('"', fp)) != '"')
    return retval;

  for (unsigned i = 0, l = field.length(); i < l; ++i) {
    if (field[i] == '"') {
      if ((retval = fputc('"', fp)) != '"')
        return retval;
    }
    if ((retval = fputc(field[i], fp)) != field[i])
      return retval;
  }

  if ((retval = fputc('"', fp)) != '"')
    return retval;

  return 0;
}
