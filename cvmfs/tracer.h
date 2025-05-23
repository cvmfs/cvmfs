/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_TRACER_H_
#define CVMFS_TRACER_H_ 1

#include <inttypes.h>
#include <pthread.h>
#include <sys/time.h>

#include <cstdio>
#include <string>

#include "shortstring.h"
#include "util/atomic.h"
#include "util/single_copy.h"

/**
 * Tracer is a thread-safe logging helper.  It uses a ring buffer
 * and spawns a helper thread, that flushes the messages onto the disk
 * when necessary.  The output file is in csv format.  Usually tracing
 * a message is a lock-free process, which therefore should have minimal
 * overhead.
 *
 * This is _not_ supposed to be a debugging system.  It is optimized for
 * speed and does not try to gather any additional information (like
 * threadid, status of variables, etc.).
 *
 * Csv output is adapted from libcsv.
 *
 * \todo If anything goes wrong, the whole thing breaks down on assertion.  This
 * might be not desired behavior.
 */
class Tracer : SingleCopy {
 public:
  enum TraceEvents {
    kEventOpen = 1,
    kEventOpenDir,
    kEventReadlink,
    kEventLookup,
    kEventStatFs,
    kEventGetAttr,
    kEventListAttr,
    kEventGetXAttr
  };

  Tracer();
  ~Tracer();

  void Activate(const int buffer_size, const int flush_threshold,
                const std::string &trace_file);
  void Spawn();
  void Flush();
  void inline __attribute__((used)) Trace(const int event,
                                          const PathString &path,
                                          const std::string &msg) {
    if (active_)
      DoTrace(event, path, msg);
  }

  bool inline __attribute__((used)) IsActive() { return active_; }

 private:
  /**
   * Code of the first log line in the trace file.
   */
  static const int kEventStart = -1;
  /**
   * Code of the last log line in the trace file.
   */
  static const int kEventStop = -2;
  /**
   * Manual flush
   */
  static const int kEventFlush = -3;

  /**
   * Contents of a single log line.
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
    PathString path; /**< The path that is subject to the trace */
    std::string msg;
  };

  static void *MainFlush(void *data);
  void GetTimespecRel(const int64_t ms, timespec *ts);
  int WriteCsvFile(FILE *fp, const std::string &field);
  int32_t DoTrace(const int event,
                  const PathString &path,
                  const std::string &msg);

  bool active_;
  bool spawned_;
  std::string trace_file_;
  int buffer_size_;
  int flush_threshold_;
  BufferEntry *ring_buffer_;
  /**
   * Has the same size as the ring buffer.  If a message is actually copied to
   * the ring buffer memory, the respective flag is set to 1.  Flags are reset
   * to 0 by the flush thread.
   */
  atomic_int32 *commit_buffer_;
  pthread_t thread_flush_;
  pthread_cond_t sig_flush_;
  pthread_mutex_t sig_flush_mutex_;
  pthread_cond_t sig_continue_trace_;
  pthread_mutex_t sig_continue_trace_mutex_;
  /**
   * Starts with 0 and gets incremented by each call to trace.  Contains the
   * first non-used sequence number.
   */
  atomic_int32 seq_no_;
  /**
   * Starts with 0 and gets incremented by the flush thread.  Points to the
   * first non-flushed message. flushed <= seq_no holds.
   */
  atomic_int32 flushed_;
  atomic_int32 terminate_flush_thread_;
  atomic_int32 flush_immediately_;
};

#endif  // CVMFS_TRACER_H_
