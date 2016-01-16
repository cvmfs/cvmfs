/**
 * This file is part of the CernVM File System.
 *
 * Tracer is a thread-safe tracing module.  It uses a ring buffer
 * and spawns a helper thread, that flushes the messages onto the disk
 * when necessary.  The output file is in csv format.  Usually tracing
 * a message is a lock-free process, which therefore should have minimal
 * overhead.
 *
 * This is _not_ supposed to be a debugging system.  It is optimized for
 * speed and does not try to gather any additional information (like
 * threadid, status of variables, etc.) and it's in no way "intelligent".
 * But -- most importantly -- if the thing crashes, all messages in the
 * ring buffer go to hell as well.
 *
 * Csv output is adapted from libcsv.
 *
 * \todo Currently, if anything goes wrong, the whole thing breaks
 *       down on assertion.  This might be not desired behavior.
 */

#include "cvmfs_config.h"
#include "tracer.h"

#include <pthread.h>
#include <sys/time.h>

#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <string>

#include "atomic.h"
#include "util.h"

using namespace std;  // NOLINT

namespace tracer {

/**
 * Contents of a trace line.
 * TODO(jblomer): memory alignment
 */
struct BufferEntry {
  /**
   * This is currently with milliseconds precision (using gettimeofday).
   */
  timeval time_stamp;
  /**
   * arbitrary code, negative codes are reserved for internal use.
   */
  int code;
  PathString path;  /**< The path that is subject to the trace */
  std::string msg;
};

struct FlushThreadStartData {
  pthread_cond_t *sig_flush;
  pthread_mutex_t *sig_flush_mutex;
  pthread_cond_t *sig_continue_trace;
  pthread_mutex_t *sig_continue_trace_mutex;
  BufferEntry *ring_buffer;
  atomic_int32 *commit_buffer;
  atomic_int32 *seq_no;
  atomic_int32 *flushed;
  atomic_int32 *terminate;
  atomic_int32 *flush_immediately;
  int size;
  int threshold;
  string filename;
};

bool active_ = false;
std::string filename_;
int buffer_size_;
int flush_threshold_;
/**
 * Starts with 0 and gets incremented by each call to trace.  Contains the first
 * non-used sequence number.
 */
atomic_int32 seq_no_;
/**
 * Starts with 0 and gets incremented by the flush thread.  Points to the first
 * non-flushed message. flushed <= seq_no holds.
 */
atomic_int32 flushed_;
atomic_int32 terminate_flush_thread_;
atomic_int32 flush_immediately_;
BufferEntry *ring_buffer_;
/**
 * Has the same size as the ring buffer.  If a message is actually copied to the
 * ring buffer memory, the respective flag is set to 1.  Flags are reset to 0 by
 * the flush thread.
 */
atomic_int32 *commit_buffer_;
pthread_t thread_flush_;
pthread_cond_t sig_flush_;
pthread_mutex_t sig_flush_mutex_;
pthread_cond_t sig_continue_trace_;
pthread_mutex_t sig_continue_trace_mutex_;


/**
 * Returns a timestamp at now+ms.
 */
static void GetTimespecRel(const int64_t ms, timespec *ts) {
  timeval now;
  gettimeofday(&now, NULL);
  int64_t nsecs = now.tv_usec * 1000 + (ms % 1000)*1000*1000;
  int carry = 0;
  if (nsecs >= 1000*1000*1000) {
    carry = 1;
    nsecs -= 1000*1000*1000;
  }
  ts->tv_sec = now.tv_sec + ms/1000 + carry;
  ts->tv_nsec = nsecs;
}


static int WriteCsvFile(FILE *fp, const string &field) {
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


static void *MainFlush(void *data) {
  FlushThreadStartData *start_data =
    reinterpret_cast<FlushThreadStartData *>(data);
  int retval;
  LockMutex(start_data->sig_flush_mutex);
  FILE *f = fopen(start_data->filename.c_str(), "a");
  assert(f != NULL && "Could not open trace file");
  struct timespec timeout;

  do {
    while ((atomic_read32(start_data->terminate) == 0) &&
           (atomic_read32(start_data->flush_immediately) == 0) &&
           (atomic_read32(start_data->seq_no) -
              atomic_read32(start_data->flushed)
              <= start_data->threshold))
    {
      GetTimespecRel(2000, &timeout);
      retval = pthread_cond_timedwait(start_data->sig_flush,
                                      start_data->sig_flush_mutex, &timeout);
      assert(retval != EINVAL && "Error while waiting on flush signal");
    }

    int base = atomic_read32(start_data->flushed) % start_data->size;
    int pos, i = 0;
    while ((i <= start_data->threshold) &&
           (atomic_read32(&start_data->commit_buffer[
             pos = ((base + i) % start_data->size)]) == 1))
    {
      string tmp;
      tmp = StringifyTimeval(start_data->ring_buffer[pos].time_stamp);
      retval = WriteCsvFile(f, tmp);
      retval |= fputc(',', f) - ',';
      tmp = StringifyInt(start_data->ring_buffer[pos].code);
      retval = WriteCsvFile(f, tmp);
      retval |= fputc(',', f) - ',';
      retval |= WriteCsvFile(f, start_data->ring_buffer[pos].path.ToString());
      retval |= fputc(',', f) - ',';
      retval |= WriteCsvFile(f, start_data->ring_buffer[pos].msg);
      retval |= (fputc(13, f) - 13) | (fputc(10, f) - 10);
      retval |= fflush(f);
      assert(retval == 0 && "Error while writing into trace file");

      atomic_dec32(&start_data->commit_buffer[pos]);
      ++i;
    }
    atomic_xadd32(start_data->flushed, i);
    atomic_cas32(start_data->flush_immediately, 1, 0);

    LockMutex(start_data->sig_continue_trace_mutex);
    retval = pthread_cond_broadcast(start_data->sig_continue_trace);
    assert(retval == 0 && "Could not signal trace threads");
    UnlockMutex(start_data->sig_continue_trace_mutex);
  } while ((atomic_read32(start_data->terminate) == 0) ||
           (atomic_read32(start_data->flushed) <
             atomic_read32(start_data->seq_no)));

  UnlockMutex(start_data->sig_flush_mutex);
  retval = fclose(f);
  assert(retval == 0 && "Could not gracefully close trace file");
  delete start_data;
  return NULL;
}

/**
 * Initialize module and spawns the helper thread for flushing.
 * @param[in] buffer_size The number of messages that are kept at maximum in the
 *            internal ring buffer.
 * @param[in] flush_threshold Threshold for the flushing thread.  Messages are
 *            flushed when more then t+1 messages are pending in the ring
 *            buffer. 0 <= flush_threshold < buffer_size must hold.
 * @param[in] filename File name of the trace log on the disk.  The file will
 *            be opened in 'a' mode, i.e. messages are appended.
 */
void Init(const int buffer_size, const int flush_threshold,
          const string &filename)
{
  active_ = true;
  filename_ = filename;
  buffer_size_ = buffer_size;
  flush_threshold_ = flush_threshold;
  assert(buffer_size_ > 1 && "Invalid size");
  assert(0 <= flush_threshold_ && flush_threshold_ < buffer_size_ &&
         "Invalid threshold");

  atomic_init32(&seq_no_);
  atomic_init32(&flushed_);
  atomic_init32(&terminate_flush_thread_);
  atomic_init32(&flush_immediately_);
  ring_buffer_ = new BufferEntry[buffer_size_];
  commit_buffer_ = new atomic_int32[buffer_size_];
  for (int i = 0; i < buffer_size_; i++)
    atomic_init32(&commit_buffer_[i]);

  int retval;
  retval = pthread_cond_init(&sig_continue_trace_, NULL);
  assert(retval == 0 && "Could not create continue-trace signal");
  retval = pthread_mutex_init(&sig_continue_trace_mutex_, NULL);
  assert(retval == 0 && "Could not create mutex for continue-trace signal");
  retval = pthread_cond_init(&sig_flush_, NULL);
  assert(retval == 0 && "Could not create flush signal");
  retval = pthread_mutex_init(&sig_flush_mutex_, NULL);
  assert(retval == 0 && "Could not create mutex for flush signal");

  FlushThreadStartData *start_data = new FlushThreadStartData;
  start_data->sig_flush = &sig_flush_;
  start_data->sig_flush_mutex = &sig_flush_mutex_;
  start_data->sig_continue_trace = &sig_continue_trace_;
  start_data->sig_continue_trace_mutex = &sig_continue_trace_mutex_;
  start_data->ring_buffer = ring_buffer_;
  start_data->commit_buffer = commit_buffer_;
  start_data->seq_no = &seq_no_;
  start_data->flushed = &flushed_;
  start_data->terminate = &terminate_flush_thread_;
  start_data->flush_immediately = &flush_immediately_;
  start_data->size = buffer_size_;
  start_data->threshold = flush_threshold_;
  start_data->filename = filename_;
  retval = pthread_create(&thread_flush_, NULL, MainFlush,
                          reinterpret_cast<void *> (start_data));
  assert(retval == 0 && "Could not create flush thread");

  TraceInternal(-1, PathString("Tracer", 6), "Trace buffer created");
}


/**
 * Turns the tracer off.
 */
void InitNull() {
  active_ = false;
}


/**
 * Destroys everything and terminates the flush thread.  Flushes
 * all pending messages from the ring buffer.  Be sure that all trace
 * functions have returned before destroying.
 */
void Fini() {
  if (!active_) return;

  TraceInternal(-2, PathString("Tracer", 6), "Destroying trace buffer...");

  // Trigger flushing and wait for it
  int retval;
  atomic_inc32(&terminate_flush_thread_);
  LockMutex(&sig_flush_mutex_);
  retval = pthread_cond_signal(&sig_flush_);
  assert(retval == 0 && "Could not signal flush thread");
  UnlockMutex(&sig_flush_mutex_);
  retval = pthread_join(thread_flush_, NULL);
  assert(retval == 0 && "Flush thread not gracefully terminated");

  retval = pthread_cond_destroy(&sig_continue_trace_);
  assert(retval == 0 && "Continue-trace signal could not be destroyed");
  retval = pthread_mutex_destroy(&sig_continue_trace_mutex_);
  assert(retval == 0 &&
         "Mutex for continue-trace signal could not be destroyed");
  retval = pthread_cond_destroy(&sig_flush_);
  assert(retval == 0 && "Flush signal could not be destroyed");
  retval = pthread_mutex_destroy(&sig_flush_mutex_);
  assert(retval == 0);

  delete[] ring_buffer_;
  delete[] commit_buffer_;
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
int32_t TraceInternal(const int event, const PathString &path,
                      const string &msg)
{
  int32_t my_seq_no = atomic_xadd32(&seq_no_, 1);
  timeval now;
  gettimeofday(&now, NULL);
  int pos = my_seq_no % buffer_size_;

  while (my_seq_no - atomic_read32(&flushed_) >= buffer_size_) {
    timespec timeout;
    int retval;
    GetTimespecRel(25, &timeout);
    retval = pthread_mutex_lock(&sig_continue_trace_mutex_);
    retval |= pthread_cond_timedwait(&sig_continue_trace_,
                                     &sig_continue_trace_mutex_, &timeout);
    retval |= pthread_mutex_unlock(&sig_continue_trace_mutex_);
    assert((retval == ETIMEDOUT || retval == 0) &&
           "Error while waiting to continue tracing");
  }

  ring_buffer_[pos].time_stamp = now;
  ring_buffer_[pos].code = event;
  ring_buffer_[pos].path = path;
  ring_buffer_[pos].msg = msg;
  atomic_inc32(&commit_buffer_[pos]);

  if (my_seq_no - atomic_read32(&flushed_) == flush_threshold_) {
    LockMutex(&sig_flush_mutex_);
    int err_code __attribute__((unused)) = pthread_cond_signal(&sig_flush_);
    assert(err_code == 0 && "Could not signal flush thread");
    UnlockMutex(&sig_flush_mutex_);
  }

  return my_seq_no;
}


/**
 * Flushes the ring buffer immediately at least up to the current seq_no.
 * It blocks until the flush thread finished the work.  It does not
 * affect further tracing during its execution.
 */
void Flush() {
  if (!active_) return;

  int32_t save_seq_no = TraceInternal(-3, PathString("Tracer", 6),
                                      "flushed ring buffer");
  while (atomic_read32(&flushed_) <= save_seq_no) {
    timespec timeout;
    int retval;

    atomic_cas32(&flush_immediately_, 0, 1);
    LockMutex(&sig_flush_mutex_);
    retval = pthread_cond_signal(&sig_flush_);
    assert(retval == 0 && "Could not signal flush thread");
    UnlockMutex(&sig_flush_mutex_);

    GetTimespecRel(250, &timeout);
    retval = pthread_mutex_lock(&sig_continue_trace_mutex_);
    retval |= pthread_cond_timedwait(&sig_continue_trace_,
                                       &sig_continue_trace_mutex_, &timeout);
    retval |= pthread_mutex_unlock(&sig_continue_trace_mutex_);
    assert((retval == ETIMEDOUT || retval == 0) &&
           "Error while waiting in flush ()");
  }
}

}  // namespace tracer
