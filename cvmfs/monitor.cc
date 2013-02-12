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
#include "smalloc.h"

using namespace std;  // NOLINT

namespace monitor {

const unsigned kMinOpenFiles = 8192;  /**< minmum threshold for the maximum
                                       number of open files */
const unsigned kMaxBacktrace = 64;  /**< reported stracktrace depth */
const unsigned kSignalHandlerStacksize = 2*1024*1024;  /**< 2 MB */

string *cache_dir_ = NULL;
string *process_name_ = NULL;
bool spawned_ = false;
unsigned max_open_files_;
int pipe_wd_[2];
platform_spinlock lock_handler_;
stack_t sighandler_stack_;


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

  char cflow = 'S';
  if (write(pipe_wd_[1], &cflow, 1) != 1)
    _exit(1);

  // write the occured signal
  if (write(pipe_wd_[1], &signal, sizeof(signal)) != sizeof(signal))
    _exit(1);

  // write the last error number
  if (write(pipe_wd_[1], &send_errno, sizeof(send_errno)) != sizeof(send_errno))
    _exit(1);

  // write the PID
  pid_t pid = getpid();
  if (write(pipe_wd_[1], &pid, sizeof(pid_t)) != sizeof(pid_t))
    _exit(1);

  // write the exe path
  const char   *exe_path        = platform_getexepath();
  const size_t  exe_path_length = strlen(exe_path) + 1; // null termination
  if (write(pipe_wd_[1], &exe_path_length, sizeof(size_t)) != sizeof(size_t))
    _exit(1);
  if (write(pipe_wd_[1], exe_path, exe_path_length) != (int)exe_path_length)
    _exit(1);

  cflow = 'Q';
  (void)write(pipe_wd_[1], &cflow, 1);

  // do not die before the stack trace was generated
  // kill -9 <pid> will finish this
  while(true) {}

  _exit(1);
}


/**
 * Log a string to syslog and into $cachedir/stacktrace.
 * We expect ideally nothing to be logged, so that file is created on demand.
 */
static void LogEmergency(string msg) {
  char ctime_buffer[32];

  FILE *fp = fopen((*cache_dir_ + "/stacktrace." + *process_name_).c_str(),
                   "a");
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
 * Uses an external shell and gdb to create a full stack trace of the dying
 * cvmfs client. The same shell is used to force-quit the client afterwards.
 */
static string GenerateStackTraceAndKill(const string &exe_path, const pid_t pid) {
  int retval;

  int fd_stdin;
  int fd_stdout;
  int fd_stderr;
  retval = Shell(&fd_stdin, &fd_stdout, &fd_stderr);
  assert(retval);

  // Let the shell execute the stacktrace extraction script
  const string bt_cmd = "/etc/cvmfs/gdb-stacktrace.sh " + exe_path + " " +
                     StringifyInt(pid) + "\n";
  WritePipe(fd_stdin, bt_cmd.data(), bt_cmd.length());

  // give the dying cvmfs client the finishing stroke
  string result = "";
  if (kill(pid, SIGKILL) != 0) {
    result += "Failed to kill cvmfs client!\n\n";
  }

  // close the standard in to close the shell
  close(fd_stdin);

  // read the stack trace from the stdout of our shell
  const int bytes_at_a_time = 2;
  char *read_buffer = NULL;
  int buffer_size = 0;
  int buffer_offset = 0;
  int chars_io;
  while (1) {
    if (buffer_offset + bytes_at_a_time > buffer_size) {
      buffer_size = bytes_at_a_time + buffer_size * 2;
      read_buffer = (char*)realloc(read_buffer, buffer_size);
      assert (read_buffer);
    }

    chars_io = read(fd_stdout,
                    read_buffer + buffer_offset,
                    bytes_at_a_time);
    if (chars_io <= 0) break;
    buffer_offset += chars_io;
  }

  // close the pipes to the shell
  close(fd_stderr);
  close(fd_stdout);

  // good bye...
  if (chars_io < 0) {
    result += "failed to read stack traces";
    return result;
  } else {
    return result + read_buffer;
  }
}


/**
 * Generates useful information from the backtrace log in the pipe.
 */
static string ReportStacktrace() {
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
  debug += ", errno: " + StringifyInt(errno);

  debug += ", version: " + string(VERSION);

  pid_t pid;
  if (read(pipe_wd_[0], &pid, sizeof(pid_t)) < int(sizeof(pid_t))) {
    return "failure while reading PID";
  }
  debug += ", PID: " + StringifyInt(pid) + "\n";

  size_t exe_path_length;
  if (read(pipe_wd_[0], &exe_path_length, sizeof(size_t)) < int(sizeof(size_t))) {
    return "failure while reading length of exe path";
  }

  char exe_path[kMaxPathLength];
  if (read(pipe_wd_[0], exe_path, exe_path_length) < int(exe_path_length)) {
    return "failure while reading executable path";
  }
  std::string s_exe_path(exe_path);
  debug += "Executable path: " + s_exe_path + "\n";

  debug += GenerateStackTraceAndKill(s_exe_path, pid);

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


bool Init(const string &cache_dir, const std::string &process_name,
          const bool check_max_open_files)
{
  monitor::cache_dir_ = new string(cache_dir);
  monitor::process_name_ = new string(process_name);
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
      LogCvmfs(kLogMonitor, kLogSyslog | kLogDebug,
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
  // Reset signal handlers
  if (spawned_) {
    signal(SIGQUIT, SIG_DFL);
    signal(SIGILL, SIG_DFL);
    signal(SIGABRT, SIG_DFL);
    signal(SIGFPE, SIG_DFL);
    signal(SIGSEGV, SIG_DFL);
    signal(SIGBUS, SIG_DFL);
    signal(SIGPIPE, SIG_DFL);
    signal(SIGXFSZ, SIG_DFL);
    free(sighandler_stack_.ss_sp);
    sighandler_stack_.ss_size = 0;
  }

  delete process_name_;
  delete cache_dir_;
  process_name_ = NULL;
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

  // Extra stack for signal handlers
  int stack_size = kSignalHandlerStacksize;  // 2 MB
  sighandler_stack_.ss_sp = smalloc(stack_size);
  sighandler_stack_.ss_size = stack_size;
  sighandler_stack_.ss_flags = 0;
  if (sigaltstack(&sighandler_stack_, NULL) != 0)
    abort();

  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_sigaction = SendTrace;
  sa.sa_flags = SA_SIGINFO | SA_ONSTACK;
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
