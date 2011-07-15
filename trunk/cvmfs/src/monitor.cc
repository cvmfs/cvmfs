/**
 * \file monitor.cc
 * \namespace monitor
 *
 * This module forks a watchdog process that listens on
 * a pipe and prints a stackstrace into syslog, when cvmfs
 * fails.
 *
 * Developed by Jakob Blomer 2010 at CERN
 * jakob.blomer@cern.ch
 */
 
#include "config.h"
#include "monitor.h"

#include <string>
#include <iostream>
#include <sstream>

#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <signal.h>
#include <sys/resource.h>
#include <execinfo.h>
#include <ucontext.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <errno.h>
#include <sys/wait.h>
#include <pthread.h>
#include <time.h>

extern "C" {
   #include "log.h"
}
   
using namespace std;


namespace monitor {

   string cache_dir;
   unsigned nofiles;
   const unsigned MIN_OPEN_FILES = 8192;
   const unsigned MAX_BACKTRACE = 64;
   int pipe_wd[2];
   pthread_spinlock_t lock_handler;
   bool spawned = false;


   /**
    * Signal handler for bad things.  Send debug information to watchdog.
    */
   static void send_trace(int signal, 
                          siginfo_t *siginfo __attribute__((unused)),
                          void *context) 
   {
      int send_errno = errno;
      if (pthread_spin_trylock(&lock_handler) != 0) {
         /* concurrent call, wait for the first one to exit the process */
         while (true) {}
      }
      
      void *adr_buf[MAX_BACKTRACE];
      char cflow = 'S';
      if (write(pipe_wd[1], &cflow, 1) != 1) _exit(1);
      
      if (write(pipe_wd[1], &signal, sizeof(int)) != sizeof(int)) _exit(1);
      if (write(pipe_wd[1], &send_errno, sizeof(int)) != sizeof(int)) _exit(1);

      int stack_size = backtrace(adr_buf, MAX_BACKTRACE);
      /* fix around sigaction */
      if (stack_size > 1) {
         ucontext_t *uc;
         uc = (ucontext_t *)context;
#ifdef __x86_64__
         adr_buf[1] = (void *)uc->uc_mcontext.gregs[REG_RIP]; 
#else
         adr_buf[1] = (void *)uc->uc_mcontext.gregs[REG_EIP];
#endif
      }
      if (write(pipe_wd[1], &stack_size, sizeof(int)) != sizeof(int)) _exit(1);
      backtrace_symbols_fd(adr_buf, stack_size, pipe_wd[1]);
      
      cflow = 'Q';
      (void)write(pipe_wd[1], &cflow, 1);
      
      _exit(1);
   }
   
   
   /**
    * Log a string to syslog and into $cachedir/stacktrace.
    * We expect ideally nothing to be logged, so that file is created on demand. 
    */
   static void log_emerg(string msg) {
      FILE *fp = fopen((cache_dir + "/stacktrace").c_str(), "a");
      if (fp) {
         time_t now = time(NULL);
         msg += "\nTimestamp: " + string(ctime(&now));
         if (fwrite(&msg[0], 1, msg.length(), fp) != msg.length())
            msg += " (failed to report into log file in cache directory)";
         fclose(fp);
      } else {
         msg += " (failed to open log file in cache directory)";
      }
      logmsg(msg.c_str());
   }
   
   
   /**
    * Read a line from the pipe.  
    * Quite inefficient but good enough for the purpose.
    */
   static string read_line() {
      string result = "";
      char next;
      while (read(pipe_wd[0], &next, 1) == 1) {
         result += next;
         if (next == '\n') break;
      }
      return result;
   }
   
   
   /**
    * Generates useful information from the backtrace log in the pipe.
    */
   static string report_stacktrace() {
      int stack_size;
      string debug = "--\n";
      ostringstream convert;
      
      int recv_signal;
      if (read(pipe_wd[0], &recv_signal, sizeof(int)) < (int)sizeof(int))
         return "failure while reading signal number";
      convert << recv_signal;
      debug += "Signal: " + convert.str();
      convert.clear();
      
      int recv_errno;
      if (read(pipe_wd[0], &recv_errno, sizeof(int)) < (int)sizeof(int))
         return "failure while reading errno";
      convert << recv_errno;
      debug += ", errno: " + convert.str() + "\n";
      
      debug += "version: " + string(VERSION) + "\n"; 
      
      if (read(pipe_wd[0], &stack_size, sizeof(int)) < (int)sizeof(int))
         return "failure while reading stacktrace";
      
      for (int i = 0; i < stack_size; ++i) {
         debug += read_line();
      }
      return debug;
   }
    
        
   /**
    * Listens on the pipe and logs the stacktrace or quits silently
    */
   static void watchdog() {     
      char cflow;
      int num_read;
      
      while ((num_read = read(pipe_wd[0], &cflow, 1)) > 0) {
         if (cflow == 'S') {
            const string debug = report_stacktrace();
            log_emerg(debug);
         } else if (cflow == 'Q') {
            break;
         } else {
            log_emerg("unexpected error");
            break;
         }
      }
      if (num_read <= 0) log_emerg("unexpected termination");
      
      close(pipe_wd[0]); 
   }

   
   
   bool init(const string cache_dir, const bool check_nofiles) {
      monitor::cache_dir = cache_dir;
      if (pthread_spin_init(&lock_handler, 0) != 0) return false;
   
      /* check number of open files */
      if (check_nofiles) {
         struct rlimit rpl;
         memset(&rpl, 0, sizeof(rpl));
         getrlimit(RLIMIT_NOFILE, &rpl);
         if (rpl.rlim_cur < MIN_OPEN_FILES) {
            cout << "Warning: current limits for number of open files are " << 
                     rpl.rlim_cur << "/" << rpl.rlim_max << endl;
            cout << "CernVM-FS is likely to run out of file descriptors, set ulimit -n to at least " << 
                    MIN_OPEN_FILES << endl;
            logmsg("Low maximum number of open files (%lu/%lu)", rpl.rlim_cur, rpl.rlim_max);
         }
         nofiles = rpl.rlim_cur;
      } else {
         nofiles = 0;
      }
      
      /* dummy call to backtrace to load library */
      void *unused = NULL;
      backtrace(&unused, 1);
      if (!unused) return false;
      
      return true;
   }
   
   void fini() {
      if (spawned) {
         char quit = 'Q';
         (void)write(pipe_wd[1], &quit, 1);
         close(pipe_wd[1]);
      }
      pthread_spin_destroy(&lock_handler);
   }
   
   /* fork watchdog */
   void spawn() {
      int retval;
      retval = pipe(pipe_wd);
      assert(retval == 0);
      
      pid_t pid;
      int statloc;
      switch (pid = fork()) {
         case -1: abort();
         case 0:
            /* double fork to avoid zombie */
            switch (fork()) {
               case -1: exit(1);
               case 0: {
                  close(pipe_wd[1]);
                  if (daemon(1, 1) == -1) {
                     logmsg("watchdog failed to deamonize");
                     exit(1);
                  }
                  watchdog();
                  exit(0);
               }
               default:
                  exit(0);
            }
         default:
            close(pipe_wd[0]);
            if (waitpid(pid, &statloc, 0) != pid) abort();
            if (!WIFEXITED(statloc) || WEXITSTATUS(statloc)) abort();
      }
      
      struct sigaction sa;
      memset(&sa, 0, sizeof(sa));
      sa.sa_sigaction = send_trace;
      sa.sa_flags = SA_SIGINFO;
      sigfillset(&sa.sa_mask);
      
      if (sigaction(SIGQUIT, &sa, NULL) ||
          sigaction(SIGILL, &sa, NULL) ||
          sigaction(SIGABRT, &sa, NULL) ||
          sigaction(SIGFPE, &sa, NULL) ||
          sigaction(SIGSEGV, &sa, NULL) ||
          sigaction(SIGBUS, &sa, NULL) ||
          sigaction(SIGXFSZ, &sa, NULL))
      {
         abort();
      }
      spawned = true;
   }
   
   unsigned get_nofiles() {
      return nofiles;
   }

}

