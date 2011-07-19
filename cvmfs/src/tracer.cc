/**
 * \file
 * cTracer is a thread-safe tracing module.  It uses a ring-buffer
 * and spwans a helper thread, that flushes the messages onto the disk
 * when necessary.  The output file is in csv format.  Usually tracing
 * a message is a lock-free process, which therefore should have minimal
 * overhead.
 *
 * This is _not_ supposed to be a debugging system.  It is optimized for 
 * speed and does not try to gather any additional information (like 
 * threadid, status of variables, etc.) and is in no way "intelligent".
 * But -- most importantly -- if the whole thing dies, all pending messages
 * in the ring buffer go to hell as well.
 *
 * For atomic instructions, a part of Intel's thread building blocks
 * is used.  In principal, gcc's __sync_-intrinsics do the job. But
 * they require a gcc4 and they are not available on all platforms.  
 * We require just fetch_and_add (and memory fences, if they are not
 * implicit.)
 *
 * Created: March 2009
 *
 * Authors:
 *     Jakob Blomer
 */

#include "tracer.h"
#include "atomic.h"
#include <cstdlib>
#include <cstdio>
#include <cassert>
#include <pthread.h>
#include <cerrno>
#include <sstream>
#include <sys/time.h>
#include <iostream>
#include <iomanip>

using namespace std;

namespace {
   /**
    * Contents of a trace line.
    * \todo memory alignment
    */
   struct buffer_entry {
      timeval time_stamp; ///< This is currently with milliseconds precision (using gettimeofday).
      int code;           ///< arbitrary code, negative codes are reserved for internal use.
      std::string id;     ///< arbitrary id, for example file name or module name which is doing the trace.
      std::string msg;
   };
   typedef struct buffer_entry buffer_entry_t;

   struct flush_thread_start_data {
      pthread_cond_t *sig_flush;
      pthread_cond_t *sig_continue_trace;
      buffer_entry_t *ring_buffer;
      atomic_int *commit_buffer;
      atomic_int *seq_no;
      atomic_int *flushed;
      atomic_int *terminate;
      atomic_int *flush_immediately;
      int size;
      int threshold;
      string file_name;
   };    
   typedef struct flush_thread_start_data flush_thread_start_data_t;
   
   void get_timespec_rel(timespec *ts, long ms) {
      timeval now;
      gettimeofday(&now, NULL);
      long nsecs = now.tv_usec * 1000 + (ms % 1000)*1000*1000; 
      int carry = 0;
      if (nsecs >= 1000*1000*1000) {
         carry = 1;
         nsecs -= 1000*1000*1000;
      }
      ts->tv_sec = now.tv_sec + ms/1000 + carry;
      ts->tv_nsec = nsecs;
   }
   
   string stringify(int v) {
      ostringstream o;
      if (!(o << v))
         assert (false || "Could not convert int to string");
      return o.str();
      
   }
   
   string stringify(timeval v) {
      ostringstream o;
      long msec = v.tv_sec * 1000;
      msec += v.tv_usec / 1000;
      if (!(o << msec << "." << setw(3) << setfill('0') << v.tv_usec % 1000))
         assert (false || "Could not convert timeval to string");
      return o.str();
   }
   
   int csv_write_fp(FILE *fp, string *src)
   {      
      if (fp == NULL || src == NULL)
         return 0;
      
      int err;
      
      if ((err = fputc('"', fp)) != '"')
         return err;
      
      for (unsigned i = 0; i < src->length(); ++i) {
         if ((*src)[i] == '"') {
            if ((err = fputc('"', fp)) != '"')
               return err;
         }
         if ((err = fputc((*src)[i], fp)) != (*src)[i])
            return err;
      }
      
      if ((err = fputc('"', fp)) != '"')
         return err;
      
      return 0;
   }
   
   extern "C" void *tf_flush(void *data) {
      flush_thread_start_data_t *start_data = 
      reinterpret_cast<flush_thread_start_data_t *> (data);
      pthread_mutex_t sig_flush_mutex;
      int err_code;
      err_code = pthread_mutex_init (&sig_flush_mutex, NULL);
      assert (err_code == 0 && "Could not initialize mutex for flush signal");
      err_code = pthread_mutex_lock (&sig_flush_mutex);
      assert (err_code == 0 && "Could not lock mutex for flush signal");
      FILE *f = fopen(start_data->file_name.c_str(), "a");
      assert(f != NULL && "Could not open trace file");
      struct timespec timeout;
      
      do {
         while ((atomic_read(start_data->terminate) == 0) && (atomic_read(start_data->flush_immediately) == 0) && 
                (atomic_read(start_data->seq_no) - atomic_read(start_data->flushed) <= start_data->threshold)) 
         {
            get_timespec_rel(&timeout, 2000);
            err_code = pthread_cond_timedwait(start_data->sig_flush, &sig_flush_mutex, &timeout);
            assert (err_code != EINVAL && "Error while waiting on flush signal");
         }
         
         int base = atomic_read(start_data->flushed) % start_data->size;
         int pos, i = 0;
         while ((i <= start_data->threshold) && 
                (atomic_read(&start_data->commit_buffer[pos = ((base + i) % start_data->size)]) == 1)) {
            
            string tmp;
            tmp = stringify(start_data->ring_buffer[pos].time_stamp);
            err_code = csv_write_fp(f, &tmp);
            err_code |= fputc(',', f) - ',';
            tmp = stringify(start_data->ring_buffer[pos].code);
            err_code = csv_write_fp(f, &tmp);
            err_code |= fputc(',', f) - ',';
            err_code |= csv_write_fp(f, &start_data->ring_buffer[pos].id);
            err_code |= fputc(',', f) - ',';
            err_code |= csv_write_fp(f, &start_data->ring_buffer[pos].msg);
            err_code |= (fputc(13, f) - 13) | (fputc(10, f) - 10);
            err_code |= fflush(f);
            assert (err_code == 0 && "Error while writing into trace file"); 
            
            atomic_dec(&start_data->commit_buffer[pos]);
            ++i;
         }
         //cout << "increasing by " << i << " " << atomic_read(start_data->flushed) << endl;
         atomic_xadd(start_data->flushed, i);
         atomic_cas(start_data->flush_immediately, 1, 0);
         
         err_code = pthread_cond_broadcast(start_data->sig_continue_trace);
         assert(err_code == 0 && "Could not signal trace threads");
      } while ((atomic_read(start_data->terminate) == 0) || 
               (atomic_read(start_data->flushed) < atomic_read(start_data->seq_no)));
      
      err_code = fclose(f);
      assert(err_code == 0 && "Could not gracefully close trace file");
      pthread_mutex_unlock(&sig_flush_mutex);
      err_code = pthread_mutex_destroy(&sig_flush_mutex);
      assert(err_code == 0 && "Could not gracefully destroy mutex for flush signal");
      delete start_data;
      return NULL;
   }
   
} //anonymous namespace

namespace Tracer {
   bool active = false;
   std::string file_name;      
   int size;                   
   int threshold;              
   atomic_int seq_no;                  ///< Starts with 0 and gets incremented by each call to trace.  Contains the first non-used sequence number.
   atomic_int flushed;                 ///< Starts with 0 and gets incremented by the flush thread.  Points to the first non-flushed message.  flushed <= seq_no holds.
   atomic_int terminate_flush_thread;
   atomic_int flush_immediately;
   buffer_entry_t *ring_buffer;
   atomic_int *commit_buffer;           ///< Has the same size as the ring buffer.  If a message is actually copied to the ring buffer memory, the respective flag is set to 1.  Flags are reset to 0 by the flush thread.
   pthread_t thread_flush;
   pthread_cond_t sig_flush;
   pthread_cond_t sig_continue_trace;
   pthread_mutex_t sig_continue_trace_mutex;
   
   
   void init(const int s, const int t, const string &f) {
      active = true;
      
      file_name = f;
      size = s;
      threshold = t;
      assert(size > 1 && "Invalid size");
      assert(0 <= threshold && threshold < size && "Invalid threshold");
      
      atomic_init(&seq_no);
      atomic_init(&flushed);
      atomic_init(&terminate_flush_thread);
      atomic_init(&flush_immediately);
      ring_buffer = new buffer_entry_t[size];
      commit_buffer = new atomic_int[size];
      for (int i = 0; i < size; i++)
         atomic_init(&commit_buffer[i]);
      
      int err_code;
      err_code = pthread_cond_init(&sig_continue_trace, NULL);
      assert(err_code == 0 && "Could not create continue-trace signal");
      err_code = pthread_mutex_init(&sig_continue_trace_mutex, NULL);
      assert(err_code == 0 && "Could not create mutex for continue-trace signal");
      err_code = pthread_cond_init(&sig_flush, NULL);
      assert(err_code == 0 && "Could not create flush signal");
      
      // TODO: low-priority thread
      flush_thread_start_data_t *start_data = new flush_thread_start_data_t;
      start_data->sig_flush = &sig_flush;
      start_data->sig_continue_trace = &sig_continue_trace;
      start_data->ring_buffer = ring_buffer;
      start_data->commit_buffer = commit_buffer;
      start_data->seq_no = &seq_no;
      start_data->flushed = &flushed;
      start_data->terminate = &terminate_flush_thread;
      start_data->flush_immediately = &flush_immediately;
      start_data->size = size;
      start_data->threshold = threshold;
      start_data->file_name = file_name;
      err_code = pthread_create(&thread_flush, NULL, tf_flush, reinterpret_cast<void *> (start_data));
      assert(err_code == 0 && "Could not create flush thread");
      
      trace_internal(-1, "cTracer", "Trace buffer created");
   }
   
   void init_null() {
      active = false;
   }
   
   void fini() {
      if (!active) return;
      
      trace_internal(-2, "Tracer", "Destroying trace buffer...");
      int err_code;
      
      /* trigger flushing and wait for it */
      atomic_inc(&terminate_flush_thread);
      err_code = pthread_cond_signal(&sig_flush);
      assert(err_code == 0 && "Could not signal flush thread");
      err_code = pthread_join(thread_flush, NULL);
      assert(err_code == 0 && "Flush thread not gracefully terminated");
      
      err_code = pthread_cond_destroy(&sig_continue_trace);
      assert(err_code == 0 && "Continue-trace signal could not be destroyed");
      err_code = pthread_mutex_destroy(&sig_continue_trace_mutex);
      assert(err_code == 0 && "Mutex for continue-trace signal could not be destroyed");
      err_code = pthread_cond_destroy(&sig_flush);
      assert(err_code == 0 && "Flush signal could not be destroyed");
      delete[] ring_buffer;
      delete[] commit_buffer;
   }
   
   int32_t trace_internal(const int c, const string &id, const string &msg) {
      int32_t my_seq_no = atomic_xadd(&seq_no, 1);
      timeval now;
      gettimeofday(&now, NULL);
      int pos = my_seq_no % size;
      
      while (my_seq_no - atomic_read(&flushed) >= size) {
         timespec timeout;
         int err_code;
         get_timespec_rel(&timeout, 250);
         err_code = pthread_mutex_lock(&sig_continue_trace_mutex);
         err_code |= pthread_cond_timedwait(&sig_continue_trace, &sig_continue_trace_mutex, &timeout);
         err_code |= pthread_mutex_unlock(&sig_continue_trace_mutex);
         assert((err_code == ETIMEDOUT || err_code == 0) && "Error while waiting to continue tracing"); 
      }
      
      ring_buffer[pos].time_stamp = now;
      ring_buffer[pos].code = c;
      ring_buffer[pos].id = id;
      ring_buffer[pos].msg = msg;
      //mem_fence(); fetch_and_add has default full fence in tbb
      //assert (commit_buffer[pos] == 0);
      atomic_inc(&commit_buffer[pos]);
      
      if (my_seq_no - atomic_read(&flushed) == threshold) {
         int err_code __attribute__((unused)) = pthread_cond_signal(&sig_flush);
         assert(err_code == 0 && "Could not signal flush thread");
      }
      
      return my_seq_no;
   }
   
   void flush() {
      if (!active) return;
      
      int32_t save_seq_no = trace_internal(-3, "Tracer", "flushed ring buffer");
      while (atomic_read(&flushed) <= save_seq_no) {
         //cout << atomic_read(&flushed) << " " << save_seq_no << endl;
         timespec timeout;
         int err_code;
         
         atomic_cas(&flush_immediately, 0, 1);
         err_code = pthread_cond_signal(&sig_flush);
         assert(err_code == 0 && "Could not signal flush thread");
         
         get_timespec_rel(&timeout, 250);
         err_code = pthread_mutex_lock(&sig_continue_trace_mutex);
         err_code |= pthread_cond_timedwait(&sig_continue_trace, &sig_continue_trace_mutex, &timeout);
         err_code |= pthread_mutex_unlock(&sig_continue_trace_mutex);
         assert((err_code == ETIMEDOUT || err_code == 0) && "Error while waiting in flush ()");
      }
   }
   
}


