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

#include <errno.h>
#include <execinfo.h>
#include <pthread.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/types.h>
#ifdef __APPLE__
  #include <sys/ucontext.h>
#else
  #include <ucontext.h>
#endif
#include <sys/uio.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <map>
#include <string>
#include <vector>

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "cvmfs.h"
#include "logging.h"
#include "platform.h"
#include "smalloc.h"
#include "util.h"

using namespace std;  // NOLINT

// Used for address offset calculation
#ifndef CVMFS_LIBCVMFS
extern loader::CvmfsExports *g_cvmfs_exports;
#endif

namespace monitor {

/**
 * Minmum threshold for the maximum number of open files
 */
const unsigned kMinOpenFiles = 8192;
const unsigned kMaxBacktrace = 64;  /**< reported stracktrace depth */
const unsigned kSignalHandlerStacksize = 2*1024*1024;  /**< 2 MB */

string *cache_dir_ = NULL;
string *process_name_ = NULL;
string *exe_path_ = NULL;
bool spawned_ = false;
Pipe *pipe_watchdog_ = NULL;
platform_spinlock lock_handler_;
stack_t sighandler_stack_;
pid_t watchdog_pid_ = 0;
void (*on_crash_)(void) = NULL;

typedef std::map<int, struct sigaction> SigactionMap;
SigactionMap old_signal_handlers_;

struct CrashData {
  int   signal;
  int   sys_errno;
  pid_t pid;
};

struct ControlFlow {
  enum Flags {  // TODO(rmeusel): C++11 (type safe enum)
    kProduceStacktrace = 0,
    kQuit,
    kUnknown,
  };
};


pid_t GetPid() {
  if (!spawned_) {
    return cvmfs::pid_;
  }

  return watchdog_pid_;
}


/**
 * Sets the signal handlers of the current process according to the ones
 * defined in the given SigactionMap.
 *
 * @param signal_handlers  a map of SIGNAL -> struct sigaction
 * @return                 a SigactionMap containing the old handlers
 */
static SigactionMap SetSignalHandlers(const SigactionMap &signal_handlers) {
  SigactionMap old_signal_handlers;
  SigactionMap::const_iterator i     = signal_handlers.begin();
  SigactionMap::const_iterator iend  = signal_handlers.end();
  for (; i != iend; ++i) {
    struct sigaction old_signal_handler;
    if (sigaction(i->first, &i->second, &old_signal_handler) != 0) {
      abort();
    }
    old_signal_handlers[i->first] = old_signal_handler;
  }

  return old_signal_handlers;
}


/**
 * Signal handler for signals that indicate a cvmfs crash.
 * Sends debug information to watchdog.
 */
static void SendTrace(int sig,
                      siginfo_t *siginfo __attribute__((unused)),
                      void *context)
{
  int send_errno = errno;
  if (platform_spinlock_trylock(&lock_handler_) != 0) {
    // Concurrent call, wait for the first one to exit the process
    while (true) {}
  }

  // Set the original signal handler for the raised signal in
  // SIGQUIT (watchdog process will raise SIGQUIT)
  (void) sigaction(SIGQUIT, &old_signal_handlers_[sig], NULL);

  // Inform the watchdog that CernVM-FS crashed
  if (!pipe_watchdog_->Write(ControlFlow::kProduceStacktrace)) {
    _exit(1);
  }

  // Send crash information to the watchdog
  CrashData crash_data;
  crash_data.signal     = sig;
  crash_data.sys_errno  = send_errno;
  crash_data.pid        = getpid();
  if (!pipe_watchdog_->Write(crash_data)) {
    _exit(1);
  }

  // Do not die before the stack trace was generated
  // kill -SIGQUIT <pid> will finish this
  int counter = 0;
  while (true) {
    SafeSleepMs(100);
    // quit anyway after 30 seconds
    if (++counter == 300) {
      LogCvmfs(kLogCvmfs, kLogSyslogErr, "stack trace generation failed");
      // Last attempt to log something useful
#ifndef CVMFS_LIBCVMFS
      LogCvmfs(kLogCvmfs, kLogSyslogErr, "Signal %d, errno %d",
               sig, send_errno);
      void *addr[kMaxBacktrace];
      // Note: this doesn't work due to the signal stack on OS X (it works on
      // Linux).  Since anyway lldb is supposed to produce the backtrace, we
      // consider it more important to protect cvmfs against stack overflows.
      int num_addr = backtrace(addr, kMaxBacktrace);
      char **symbols = backtrace_symbols(addr, num_addr);
      string backtrace = "Backtrace (" + StringifyInt(num_addr) +
                         " symbols):\n";
      for (int i = 0; i < num_addr; ++i)
        backtrace += string(symbols[i]) + "\n";
      LogCvmfs(kLogCvmfs, kLogSyslogErr, "%s", backtrace.c_str());
      LogCvmfs(kLogCvmfs, kLogSyslogErr, "address of g_cvmfs_exports: %p",
               &g_cvmfs_exports);
#endif

      _exit(1);
    }
  }

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
  LogCvmfs(kLogMonitor, kLogSyslogErr, "%s", msg.c_str());
}


/**
 * reads from the file descriptor until the specific gdb prompt
 * is reached or the pipe gets broken
 *
 * @param fd_pipe  the file descriptor of the pipe to be read
 * @return         the data read from the pipe
 */
static string ReadUntilGdbPrompt(int fd_pipe) {
#ifdef __APPLE__
  static const string gdb_prompt = "(lldb)";
#else
  static const string gdb_prompt = "\n(gdb) ";
#endif

  string        result;
  char          mini_buffer;
  int           chars_io;
  unsigned int  ring_buffer_pos = 0;

  // read from stdout of gdb until gdb prompt occures --> (gdb)
  while (1) {
    chars_io = read(fd_pipe, &mini_buffer, 1);

    // in case something goes wrong...
    if (chars_io <= 0) break;

    result += mini_buffer;

    // find the gdb_promt in the stdout data
    if (mini_buffer == gdb_prompt[ring_buffer_pos]) {
      ++ring_buffer_pos;
      if (ring_buffer_pos == gdb_prompt.size()) {
        break;
      }
    } else {
      ring_buffer_pos = 0;
    }
  }

  return result;
}



/**
 * Uses an external shell and gdb to create a full stack trace of the dying
 * cvmfs client. The same shell is used to force-quit the client afterwards.
 */
static string GenerateStackTrace(const string &exe_path,
                                 const pid_t   pid) {
  int retval;
  string result = "";

  // re-gain root permissions to allow for ptrace of died cvmfs2 process
  const bool retrievable = true;
  if (!SwitchCredentials(0, getgid(), retrievable)) {
    result += "failed to re-gain root permissions... still give it a try\n";
  }

  // run gdb and attach to the dying cvmfs2 process
  int fd_stdin;
  int fd_stdout;
  int fd_stderr;
  vector<string> argv;  // TODO(jblomer): C++11 initializer list...
#ifndef __APPLE__
  argv.push_back("-q");
  argv.push_back("-n");
  argv.push_back(exe_path);
#else
  argv.push_back("-p");
#endif
  argv.push_back(StringifyInt(pid));
  pid_t gdb_pid = 0;
  const bool double_fork = false;
  retval = ExecuteBinary(&fd_stdin,
                         &fd_stdout,
                         &fd_stderr,
#ifdef __APPLE__
                          "lldb",
#else
                          "gdb",
#endif
                          argv,
                          double_fork,
                         &gdb_pid);
  assert(retval);


  // Skip the gdb startup output
  ReadUntilGdbPrompt(fd_stdout);

  // Send stacktrace command to gdb
#ifdef __APPLE__
  const string gdb_cmd = "bt all\n" "quit\n";
#else
  const string gdb_cmd = "thread apply all bt\n" "quit\n";
#endif
  // The execve can have failed, which can't be detected in ExecuteBinary.
  // Instead, writing to the pipe will fail.
  ssize_t nbytes = write(fd_stdin, gdb_cmd.data(), gdb_cmd.length());
  if ((nbytes < 0) || (static_cast<unsigned>(nbytes) != gdb_cmd.length())) {
    result += "failed to start gdb/lldb (" + StringifyInt(nbytes) + " bytes "
              "written, errno " + StringifyInt(errno) + ")\n";
    return result;
  }

  // Read the stack trace from the stdout of our gdb process
#ifdef __APPLE__
  // lldb has one more prompt
  result += ReadUntilGdbPrompt(fd_stdout);
#endif
  result += ReadUntilGdbPrompt(fd_stdout) + "\n\n";

  // Close the connection to the terminated gdb process
  close(fd_stderr);
  close(fd_stdout);
  close(fd_stdin);

  // Make sure gdb has terminated (wait for it for a short while)
  unsigned int timeout = 15;
  int statloc;
  while (timeout > 0 && waitpid(gdb_pid, &statloc, WNOHANG) != gdb_pid) {
    --timeout;
    SafeSleepMs(1000);
  }

  // when the timeout expired, gdb probably hangs... we need to kill it
  if (timeout == 0) {
    result += "gdb did not exit as expected. sending SIGKILL... ";
    result += (kill(gdb_pid, SIGKILL) != 0) ? "failed\n" : "okay\n";
  }

  return result;
}


/**
 * Generates useful information from the backtrace log in the pipe.
 */
static string ReportStacktrace() {
  // Re-activate µSyslog, if necessary
  SetLogMicroSyslog(GetLogMicroSyslog());

  CrashData crash_data;
  if (!pipe_watchdog_->Read(&crash_data)) {
    return "failed to read crash data (" + StringifyInt(errno) + ")";
  }

  string debug = "--\n";
  debug += "Signal: "    + StringifyInt(crash_data.signal);
  debug += ", errno: "   + StringifyInt(crash_data.sys_errno);
  debug += ", version: " + string(VERSION);
  debug += ", PID: "     + StringifyInt(crash_data.pid) + "\n";
  debug += "Executable path: " + *exe_path_ + "\n";

  debug += GenerateStackTrace(*exe_path_, crash_data.pid);

  // Give the dying cvmfs client the finishing stroke
  if (kill(crash_data.pid, SIGKILL) != 0) {
    debug += "Failed to kill cvmfs client! (";
    switch (errno) {
      case EINVAL:
        debug += "invalid signal";
        break;
      case EPERM:
        debug += "permission denied";
        break;
      case ESRCH:
        debug += "no such process";
        break;
      default:
        debug += "unknown error " + StringifyInt(errno);
    }
    debug += ")\n\n";
  }

  return debug;
}


/**
 * Listens on the pipe and logs the stacktrace or quits silently.
 */
static void Watchdog() {
  signal(SIGPIPE, SIG_IGN);
  ControlFlow::Flags control_flow;

  if (!pipe_watchdog_->Read(&control_flow)) {
    // Re-activate µSyslog, if necessary
    SetLogMicroSyslog(GetLogMicroSyslog());
    LogEmergency("unexpected termination (" + StringifyInt(control_flow) + ")");
    if (on_crash_) on_crash_();
  } else {
    switch (control_flow) {
      case ControlFlow::kProduceStacktrace:
        LogEmergency(ReportStacktrace());
        if (on_crash_) on_crash_();
        break;

      case ControlFlow::kQuit:
        break;

      default:
        // Re-activate µSyslog, if necessary
        SetLogMicroSyslog(GetLogMicroSyslog());
        LogEmergency("unexpected error");
        break;
    }
  }

  close(pipe_watchdog_->read_end);
}


bool Init(const string &cache_dir, const std::string &process_name,
          const bool check_max_open_files)
{
  monitor::cache_dir_ = new string(cache_dir);
  monitor::process_name_ = new string(process_name);
  monitor::exe_path_ = new string(platform_getexepath());
  if (platform_spinlock_init(&lock_handler_, 0) != 0) return false;

  return true;
}


void Fini() {
  on_crash_ = NULL;

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

  if (spawned_) {
    pipe_watchdog_->Write(ControlFlow::kQuit);
    close(pipe_watchdog_->write_end);
  }
  delete pipe_watchdog_;
  delete process_name_;
  delete cache_dir_;
  delete exe_path_;
  pipe_watchdog_ = NULL;
  process_name_ = NULL;
  cache_dir_ = NULL;
  exe_path_ = NULL;
  platform_spinlock_destroy(&lock_handler_);
  LogCvmfs(kLogMonitor, kLogDebug, "monitor stopped");
}

/**
 * Fork watchdog.
 */
void Spawn() {
  Pipe pipe_pid;
  pipe_watchdog_ = new Pipe();

  pid_t pid;
  int statloc;
  int max_fd = sysconf(_SC_OPEN_MAX);
  assert(max_fd >= 0);
  switch (pid = fork()) {
    case -1: abort();
    case 0:
      // Double fork to avoid zombie
      switch (fork()) {
        case -1: exit(1);
        case 0: {
          close(pipe_watchdog_->write_end);
          Daemonize();
          // send the watchdog PID to cvmfs
          pid_t watchdog_pid = getpid();
          pipe_pid.Write(watchdog_pid);
          close(pipe_pid.write_end);
          // Close all unused file descriptors
          // close also usyslog, only get it back if necessary
          // string usyslog_save = GetLogMicroSyslog();
          string debuglog_save = GetLogDebugFile();
          // SetLogMicroSyslog("");
          SetLogDebugFile("");
          for (int fd = 0; fd < max_fd; fd++) {
            if (fd != pipe_watchdog_->read_end)
              close(fd);
          }
          // SetLogMicroSyslog(usyslog_save);  // no-op if usyslog not used
          SetLogDebugFile(debuglog_save);  // no-op if debug log not used
          Watchdog();
          exit(0);
        }
        default:
          exit(0);
      }
    default:
      close(pipe_watchdog_->read_end);
      if (waitpid(pid, &statloc, 0) != pid) abort();
      if (!WIFEXITED(statloc) || WEXITSTATUS(statloc)) abort();
  }

  // retrieve the watchdog PID from the pipe
  close(pipe_pid.write_end);
  pipe_pid.Read(&watchdog_pid_);
  close(pipe_pid.read_end);

  // lower restrictions for ptrace
  if (!platform_allow_ptrace(watchdog_pid_)) {
    LogCvmfs(kLogMonitor, kLogSyslogWarn,
             "failed to allow ptrace() for watchdog (PID: %d). "
             "Post crash stacktrace of the CernVM-FS client might not work",
             watchdog_pid_);
  }

  // Extra stack for signal handlers
  int stack_size = kSignalHandlerStacksize;  // 2 MB
  sighandler_stack_.ss_sp = smalloc(stack_size);
  sighandler_stack_.ss_size = stack_size;
  sighandler_stack_.ss_flags = 0;
  if (sigaltstack(&sighandler_stack_, NULL) != 0)
    abort();

  // define our crash signal handler
  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_sigaction = SendTrace;
  sa.sa_flags = SA_SIGINFO | SA_ONSTACK;
  sigfillset(&sa.sa_mask);

  SigactionMap signal_handlers;
  signal_handlers[SIGQUIT] = sa;
  signal_handlers[SIGILL]  = sa;
  signal_handlers[SIGABRT] = sa;
  signal_handlers[SIGFPE]  = sa;
  signal_handlers[SIGSEGV] = sa;
  signal_handlers[SIGBUS]  = sa;
  signal_handlers[SIGPIPE] = sa;
  signal_handlers[SIGXFSZ] = sa;
  old_signal_handlers_ = SetSignalHandlers(signal_handlers);

  spawned_ = true;
}


void RegisterOnCrash(void (*CleanupOnCrash)(void)) {
  on_crash_ = CleanupOnCrash;
}


unsigned GetMaxOpenFiles() {
  static unsigned max_open_files;
  static bool     already_done = false;

  /* check number of open files (lazy evaluation) */
  if (!already_done) {
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
      LogCvmfs(kLogMonitor, kLogSyslogWarn | kLogDebug,
               "Warning: current limits for number of open files are "
               "(%lu/%lu)\n"
               "CernVM-FS is likely to run out of file descriptors, "
               "set ulimit -n to at least %lu",
               soft_limit, hard_limit, kMinOpenFiles);
    }
    max_open_files = soft_limit;
    already_done   = true;
  }

  return max_open_files;
}

}  // namespace monitor
