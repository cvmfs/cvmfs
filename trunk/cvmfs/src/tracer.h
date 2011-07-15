/**
 * \file
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
 *
 * Created: March 2009
 *
 * Authors:
 *     Jakob Blomer
 */

#ifndef _TRACER_H
#define _TRACER_H 1

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "atomic.h"
#include <string>
#include <sys/time.h>
#include <pthread.h>

namespace Tracer {

   extern bool active;
   
   const int FUSE_OPEN =       1;
   const int FUSE_LS =         2;
   const int FUSE_READ =       3;
   const int FUSE_WRITE =      4;
   const int FUSE_CREATE =     5;
   const int FUSE_MKDIR =      6;
   const int FUSE_READLINK =   7;
   const int FUSE_KCACHE =     8; 
   const int FUSE_STAT =       9;
   const int CROWD =          10;
   
   /**
    * Initialize module and spawns the helper thread for flushing.
    * \param s The number of messages that are kept at maximum in the internal ring buffer.
    * \param t Threshold for the flushing thread.  Messages are flushed when more then t+1 messages are pending in the ring buffer.  0 <= t < s must hold.
    * \param f File name of the trace log on the disk.  The file will be opened in 'a' mode, i.e. messages are appended.
    */
   void init(const int s, const int t, const std::string &f);
   void init_null();
   
   /**
    * Destroys everything and terminates the flush thread.  Flushes
    * all pending messages from the ring buffer.  Be sure that all trace
    * functions have returned before destroying.
    */
   void fini();
   
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
    * \param c Arbitrary code, negative codes are reserved for internal use.
    * \param id Arbitrary id, for example file name or module name which is doing the trace.
    * \return The sequence number which was used to trace the record
    */
   int32_t trace_internal(const int c, const std::string &id, const std::string &msg);
   
   /**
    * Flushes the ring buffer immediately at least up to the current seq_no.
    * It blocks until the flush thread finished the work.  It does not 
    * affect further tracing during its execution.
    */
   void flush();
   
   void inline __attribute__((used)) trace(const int c, const std::string &id, const std::string &msg) {
      if (active) trace_internal(c, id, msg);
   }
}

#endif
