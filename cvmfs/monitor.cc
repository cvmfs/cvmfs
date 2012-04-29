/**
 * This file is part of the CernVM File System.
 *
 * This module forks a watchdog process that listens on
 * a pipe and prints a stackstrace into syslog, when cvmfs
 * fails.
 *
 * Also, it handles getting and setting the maximum number of file descriptors.
 */

#include "cvmfs_config.h"
#include "monitor.h"

#include <signal.h>
#include <sys/resource.h>
#include <execinfo.h>
#ifdef __APPLE__
  #include <sys/ucontext.h>
#else
  #include <ucontext.h>
#endif
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <errno.h>
#include <sys/wait.h>
#include <pthread.h>
#include <time.h>

#include <string>

#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <cassert>

#include "platform.h"
#include "util.h"
#include "logging.h"

using namespace std;  // NOLINT

namespace monitor {

const unsigned kMinOpenFiles = 8192;  /**< minmum threshold for the maximum
                                       number of open files */
const unsigned kMaxBacktrace = 64;  /**< reported stracktrace depth */

string *cache_dir_ = NULL;
bool spawned_ = false;
unsigned max_open_files_;
int pipe_wd_[2];
platform_spinlock lock_handler_;


/**
 * Get the instruction pointer in a platform independant fashion.
 */
static void* GetInstructionPointer(ucontext_t *uc) {
  void *result;

#ifdef __APPLE__
#ifdef __x86_64__
  result = reinterpret_cast<void *>(uc->uc_mcontext->__ss.__rip);
#else
  result = reinterpret_cast<void *>(uc-uc_mcontext->__ss.__eip);
#endif

#else  // Linux

#ifdef __x86_64__
  result = reinterpret_cast<void *>(uc->uc_mcontext.gregs[REG_RIP]);
#else
  result = reinterpret_cast<void *>(uc->uc_mcontext.gregs[REG_EIP]);
#endif
#endif

  return result;
}


/**
 * Signal handler for bad things.  Send debug information to watchdog.
 */
static void SendTrace(int signal,
                      siginfo_t *siginfo __attribute__((unused)),
                      void *context)
{
  int send_errno = errno;
  if (platform_spinlock_trylock(&lock_handler_) != 0) {
    // Concurrent call, wait for the first one to exit the process
    while (true) {}
  }

  void *adr_buf[kMaxBacktrace];
  char cflow = 'S';
  if (write(pipe_wd_[1], &cflow, 1) != 1)
    _exit(1);

  if (write(pipe_wd_[1], &signal, sizeof(signal)) != sizeof(signal))
    _exit(1);
  if (write(pipe_wd_[1], &send_errno, sizeof(send_errno)) != sizeof(send_errno))
    _exit(1);

  int stack_size = backtrace(adr_buf, kMaxBacktrace);
  // Fix around sigaction
  if (stack_size > 1) {
    ucontext_t *uc = reinterpret_cast<ucontext_t *>(context);
    adr_buf[1] = GetInstructionPointer(uc);
  }
  if (write(pipe_wd_[1], &stack_size, sizeof(stack_size)) != sizeof(stack_size))
    _exit(1);
  backtrace_symbols_fd(adr_buf, stack_size, pipe_wd_[1]);

  cflow = 'Q';
  (void)write(pipe_wd_[1], &cflow, 1);

  _exit(1);
}


/**
 * Log a string to syslog and into $cachedir/stacktrace.
 * We expect ideally nothing to be logged, so that file is created on demand.
 */
static void LogEmergency(string msg) {
  char ctime_buffer[32];

  FILE *fp = fopen((*cache_dir_ + "/stacktrace").c_str(), "a");
  if (fp) {
    time_t now = time(NULL);
    msg += "\nTimestamp: " + string(ctime_r(&now, ctime_buffer));
    if (fwrite(&msg[0], 1, msg.length(), fp) != msg.length())
      msg += " (failed to report into log file in cache directory)";
    fclose(fp);
  } else {
    msg += " (failed to open log file in cache directory)";
  }
  LogCvmfs(kLogMonitor, kLogSyslog, "%s", msg.c_str());
}


/**
 * Read a line from the pipe.
 * Quite inefficient but good enough for the purpose.
 */
static string ReadLineFromPipe() {
  string result = "";
  char next;
  while (read(pipe_wd_[0], &next, 1) == 1) {
    result += next;
    if (next == '\n') break;
  }
  return result;
}


/**
 * Generates useful information from the backtrace log in the pipe.
 */
static string ReportStacktrace() {
  int stack_size;
  string debug = "--\n";

  int recv_signal;
  if (read(pipe_wd_[0], &recv_signal, sizeof(recv_signal)) <
      int(sizeof(recv_signal)))
  {
    return "failure while reading signal number";
  }
  debug += "Signal: " + StringifyInt(recv_signal);

  int recv_errno;
  if (read(pipe_wd_[0], &recv_errno, sizeof(recv_errno)) <
      int(sizeof(recv_errno)))
  {
    return "failure while reading errno";
  }
  debug += ", errno: " + StringifyInt(errno) + "\n";

  debug += "version: " + string(VERSION) + "\n";

  if (read(pipe_wd_[0], &stack_size, sizeof(stack_size)) <
      int(sizeof(stack_size)))
  {
    return "failure while reading stacktrace";
  }

  for (int i = 0; i < stack_size; ++i) {
    debug += ReadLineFromPipe();
  }
  return debug;
}


/**
 * Listens on the pipe and logs the stacktrace or quits silently.
 */
static void Watchdog() {
  char cflow;
  int num_read;

  while ((num_read = read(pipe_wd_[0], &cflow, 1)) > 0) {
    if (cflow == 'S') {
      const string debug = ReportStacktrace();
      LogEmergency(debug);
    } else if (cflow == 'Q') {
      break;
    } else {
      LogEmergency("unexpected error");
      break;
    }
  }
  if (num_read <= 0) LogEmergency("unexpected termination");

  close(pipe_wd_[0]);
}


bool Init(const string cache_dir, const bool check_max_open_files) {
  monitor::cache_dir_ = new string(cache_dir);
  if (platform_spinlock_init(&lock_handler_, 0) != 0) return false;

  /* check number of open files */
  if (check_max_open_files) {
    unsigned int soft_limit = 0;
    int hard_limit = 0;

    struct rlimit rpl;
    memset(&rpl, 0, sizeof(rpl));
    getrlimit(RLIMIT_NOFILE, &rpl);
    soft_limit = rpl.rlim_cur;

#ifdef __APPLE__
    hard_limit = sysconf(_SC_OPEN_MAX);
    if (hard_limit < 0) {
      LogCvmfs(kLogMonitor, kLogStdout, "Warning: could not retrieve "
               "hard limit for the number of open files");
    }
#else
    hard_limit = rpl.rlim_max;
#endif

    if (soft_limit < kMinOpenFiles) {
      LogCvmfs(kLogMonitor, kLogSyslog | kLogStdout,
               "Warning: current limits for number of open files are "
               "(%lu/%lu)\n"
               "CernVM-FS is likely to run out of file descriptors, "
               "set ulimit -n to at least %lu",
               soft_limit, hard_limit, kMinOpenFiles);
    }
    max_open_files_ = soft_limit;
  } else {
    max_open_files_ = 0;
  }

  // Dummy call to backtrace to load library
  void *unused = NULL;
  backtrace(&unused, 1);
  if (!unused) return false;

  return true;
}


void Fini() {
  delete cache_dir_;
  cache_dir_ = NULL;
  if (spawned_) {
    char quit = 'Q';
    (void)write(pipe_wd_[1], &quit, 1);
    close(pipe_wd_[1]);
  }
  platform_spinlock_destroy(&lock_handler_);
}

/**
 * Fork watchdog.
 */
void Spawn() {
  MakePipe(pipe_wd_);

  pid_t pid;
  int statloc;
  switch (pid = fork()) {
    case -1: abort();
    case 0:
      // Double fork to avoid zombie
      switch (fork()) {
        case -1: exit(1);
        case 0: {
          close(pipe_wd_[1]);
          Daemonize();
          Watchdog();
          exit(0);
        }
        default:
          exit(0);
      }
    default:
      close(pipe_wd_[0]);
      if (waitpid(pid, &statloc, 0) != pid) abort();
      if (!WIFEXITED(statloc) || WEXITSTATUS(statloc)) abort();
  }

  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_sigaction = SendTrace;
  sa.sa_flags = SA_SIGINFO;
  sigfillset(&sa.sa_mask);

  if (sigaction(SIGQUIT, &sa, NULL) ||
      sigaction(SIGILL, &sa, NULL) ||
      sigaction(SIGABRT, &sa, NULL) ||
      sigaction(SIGFPE, &sa, NULL) ||
      sigaction(SIGSEGV, &sa, NULL) ||
      sigaction(SIGBUS, &sa, NULL) ||
      sigaction(SIGPIPE, &sa, NULL) ||
      sigaction(SIGXFSZ, &sa, NULL))
  {
    abort();
  }
  spawned_ = true;
}

unsigned GetMaxOpenFiles() {
  return max_open_files_;
}

}  // namespace monitor
