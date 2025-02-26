/**
 * This file is part of the CernVM File System.
 *
 * This module forks a watchdog process that listens on
 * a pipe and prints a stackstrace into syslog, when cvmfs
 * fails.
 *
 * Also, it handles getting and setting the maximum number of file descriptors.
 */


#include "monitor.h"

#include <errno.h>
#include <execinfo.h>
#include <poll.h>
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
#include <syslog.h>
#include <time.h>
#include <unistd.h>

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <set>
#include <string>
#include <vector>

#if defined(CVMFS_FUSE_MODULE)
#include "cvmfs.h"
#endif
#include "util/exception.h"
#include "util/logging.h"
#include "util/platform.h"
#include "util/posix.h"
#include "util/smalloc.h"
#include "util/string.h"

// Used for address offset calculation
#if defined(CVMFS_FUSE_MODULE)
extern loader::CvmfsExports *g_cvmfs_exports;
#endif

using namespace std;  // NOLINT

Watchdog *Watchdog::instance_ = NULL;

int Watchdog::g_suppressed_signals[] = { SIGHUP, SIGINT, SIGQUIT,
                                SIGILL, SIGABRT, SIGBUS,
                                SIGFPE, SIGUSR1, SIGSEGV,
                                SIGUSR2, SIGTERM, SIGXCPU,
                                SIGXFSZ};

int Watchdog::g_crash_signals[] = { SIGQUIT, SIGILL, SIGABRT,
                                  SIGFPE, SIGSEGV, SIGBUS,
                                  SIGPIPE, SIGXFSZ };

Watchdog *Watchdog::Create(FnOnCrash on_crash) {
  assert(instance_ == NULL);
  instance_ = new Watchdog(on_crash);
  instance_->Fork();
  return instance_;
}


/**
 * Uses an external shell and gdb to create a full stack trace of the dying
 * process. The same shell is used to force-quit the client afterwards.
 */
string Watchdog::GenerateStackTrace(pid_t pid) {
  int retval;
  string result = "";

  // re-gain root permissions to allow for ptrace of died cvmfs2 process
  const bool retrievable = true;
  if (!SwitchCredentials(0, getgid(), retrievable)) {
    result += "failed to re-gain root permissions... still give it a try\n";
  }

  // run gdb and attach to the dying process
  int fd_stdin;
  int fd_stdout;
  int fd_stderr;
  vector<string> argv;
  argv.push_back("-p");
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

  // Check for output on stderr
  string result_err;
  Block2Nonblock(fd_stderr);
  char cbuf;
  while (read(fd_stderr, &cbuf, 1) == 1)
    result_err.push_back(cbuf);
  if (!result_err.empty())
    result += "\nError output:\n" + result_err + "\n";

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


pid_t Watchdog::GetPid() {
  if (instance_ != NULL) {
    return instance_->watchdog_pid_;
  }
  return getpid();
}

/**
 * Log a string to syslog and into the crash dump file.
 * We expect ideally nothing to be logged, so that file is created on demand.
 */
void Watchdog::LogEmergency(string msg) {
  char ctime_buffer[32];

  if (!crash_dump_path_.empty()) {
    FILE *fp = fopen(crash_dump_path_.c_str(), "a");
    if (fp) {
      time_t now = time(NULL);
      msg += "\nTimestamp: " + string(ctime_r(&now, ctime_buffer));
      if (fwrite(&msg[0], 1, msg.length(), fp) != msg.length()) {
        msg +=
            " (failed to report into crash dump file " + crash_dump_path_ + ")";
      } else {
        msg += "\n Crash logged also on file: " + crash_dump_path_ + "\n";
      }
      fclose(fp);
    } else {
      msg += " (failed to open crash dump file " + crash_dump_path_ + ")";
    }
  }
  LogCvmfs(kLogMonitor, kLogSyslogErr, "%s", msg.c_str());
}

/**
 * Reads from the file descriptor until the specific gdb prompt is reached or
 * the pipe gets broken.
 *
 * @param fd_pipe  the file descriptor of the pipe to be read
 * @return         the data read from the pipe
 */
string Watchdog::ReadUntilGdbPrompt(int fd_pipe) {
#ifdef __APPLE__
  static const string gdb_prompt = "(lldb)";
#else
  static const string gdb_prompt = "\n(gdb) ";
#endif

  string        result;
  char          mini_buffer;
  int           chars_io;
  unsigned int  ring_buffer_pos = 0;

  // read from stdout of gdb until gdb prompt occurs --> (gdb)
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
 * Generates useful information from the backtrace log in the pipe.
 */
string Watchdog::ReportStacktrace() {
  CrashData crash_data;
  if (!pipe_watchdog_->TryRead<CrashData>(&crash_data)) {
    return "failed to read crash data (" + StringifyInt(errno) + ")";
  }

  string debug = "--\n";
  debug += "Signal: "    + StringifyInt(crash_data.signal);
  debug += ", errno: "   + StringifyInt(crash_data.sys_errno);
  debug += ", version: " + string(CVMFS_VERSION);
  debug += ", PID: "     + StringifyInt(crash_data.pid) + "\n";
  debug += "Executable path: " + exe_path_ + "\n";

  debug += GenerateStackTrace(crash_data.pid);

  // Give the dying process the finishing stroke
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


void Watchdog::ReportSignalAndTerminate(
  int sig, siginfo_t *siginfo, void * /* context */)
{
  LogCvmfs(kLogMonitor, kLogSyslogErr,
           "watchdog: received unexpected signal %d from PID %d / UID %d",
           sig, siginfo->si_pid, siginfo->si_uid);
  _exit(1);
}


void Watchdog::SendTrace(int sig, siginfo_t *siginfo, void *context) {
  int send_errno = errno;
  if (platform_spinlock_trylock(&Me()->lock_handler_) != 0) {
    // Concurrent call, wait for the first one to exit the process
    while (true) {}
  }

  // Set the original signal handler for the raised signal in
  // SIGQUIT (watchdog process will raise SIGQUIT)
  (void) sigaction(SIGQUIT, &(Me()->old_signal_handlers_[sig]), NULL);

  // Inform the watchdog that CernVM-FS crashed
  if (!Me()->pipe_watchdog_->Write(ControlFlow::kProduceStacktrace)) {
    _exit(1);
  }

  // Send crash information to the watchdog
  CrashData crash_data;
  crash_data.signal     = sig;
  crash_data.sys_errno  = send_errno;
  crash_data.pid        = getpid();
  if (!Me()->pipe_watchdog_->Write<CrashData>(crash_data)) {
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
#if defined(CVMFS_FUSE_MODULE)
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
 * Sets the signal handlers of the current process according to the ones
 * defined in the given SigactionMap.
 *
 * @param signal_handlers  a map of SIGNAL -> struct sigaction
 * @return                 a SigactionMap containing the old handlers
 */
Watchdog::SigactionMap Watchdog::SetSignalHandlers(
  const SigactionMap &signal_handlers)
{
  SigactionMap old_signal_handlers;
  SigactionMap::const_iterator i     = signal_handlers.begin();
  SigactionMap::const_iterator iend  = signal_handlers.end();
  for (; i != iend; ++i) {
    struct sigaction old_signal_handler;
    if (sigaction(i->first, &i->second, &old_signal_handler) != 0) {
      PANIC(NULL);
    }
    old_signal_handlers[i->first] = old_signal_handler;
  }

  return old_signal_handlers;
}


/**
 * Fork the watchdog process and put it on hold until Spawn() is called.
 */
void Watchdog::Fork() {
  Pipe<kPipeWatchdogPid> pipe_pid;
  pipe_watchdog_ = new Pipe<kPipeWatchdog>();
  pipe_listener_ = new Pipe<kPipeWatchdogSupervisor>();

  pid_t pid;
  int statloc;
  switch (pid = fork()) {
    case -1: PANIC(NULL);
    case 0:
      // Double fork to avoid zombie
      switch (fork()) {
        case -1: _exit(1);
        case 0: {
          pipe_watchdog_->CloseWriteFd();
          Daemonize();
          // send the watchdog PID to the supervisee
          pid_t watchdog_pid = getpid();
          pipe_pid.Write(watchdog_pid);
          pipe_pid.CloseWriteFd();
          // Close all unused file descriptors
          // close also usyslog, only get it back if necessary
          // string usyslog_save = GetLogMicroSyslog();
          string debuglog_save = GetLogDebugFile();
          SetLogDebugFile("");
          string usyslog_save = GetLogMicroSyslog();
          SetLogMicroSyslog("");
          // Gracefully close the syslog before closing all fds. The next call
          // to syslog will reopen it.
          closelog();
          // Let's keep stdin, stdout, stderr open at /dev/null (daemonized)
          // in order to prevent accidental outputs from messing with another
          // file descriptor
          std::set<int> preserve_fds;
          preserve_fds.insert(0);
          preserve_fds.insert(1);
          preserve_fds.insert(2);
          preserve_fds.insert(pipe_watchdog_->GetReadFd());
          preserve_fds.insert(pipe_listener_->GetWriteFd());
          CloseAllFildes(preserve_fds);
          SetLogMicroSyslog(usyslog_save);  // no-op if usyslog not used
          SetLogDebugFile(debuglog_save);  // no-op if debug log not used

          if (WaitForSupervisee())
            Supervise();

          pipe_watchdog_->CloseReadFd();
          pipe_listener_->CloseWriteFd();
          exit(0);
        }
        default:
          _exit(0);
      }
    default:
      pipe_watchdog_->CloseReadFd();
      pipe_listener_->CloseWriteFd();
      pipe_pid.CloseWriteFd();
      if (waitpid(pid, &statloc, 0) != pid) PANIC(NULL);
      if (!WIFEXITED(statloc) || WEXITSTATUS(statloc)) PANIC(NULL);
  }

  // retrieve the watchdog PID from the pipe
  pipe_pid.Read(&watchdog_pid_);
  pipe_pid.CloseReadFd();
}


bool Watchdog::WaitForSupervisee() {
  // We want broken pipes not to raise a signal but handle the error in the
  // read/write code
  platform_sighandler_t rv_sig = signal(SIGPIPE, SIG_IGN);
  assert(rv_sig != SIG_ERR);

  // The watchdog is not supposed to receive signals. If it does, report it.
  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_sigaction = ReportSignalAndTerminate;
  sa.sa_flags = SA_SIGINFO;
  sigfillset(&sa.sa_mask);

  SigactionMap signal_handlers;
  for (size_t i = 0; i < sizeof(g_suppressed_signals)/sizeof(int); i++) {
    signal_handlers[g_suppressed_signals[i]] = sa;
  }
  SetSignalHandlers(signal_handlers);

  ControlFlow::Flags control_flow = ControlFlow::kUnknown;

  if (!pipe_watchdog_->TryRead(&control_flow)) {
    LogCvmfs(kLogMonitor, kLogDebug, "supervisee canceled watchdog");
    return false;
  }

  switch (control_flow) {
    case ControlFlow::kQuit:
      return false;
    case ControlFlow::kSupervise:
      break;
    default:
      LogEmergency("Internal error: invalid control flow");
      return false;
  }

  size_t size;
  pipe_watchdog_->Read(&size);
  crash_dump_path_.resize(size);
  if (size > 0) {
    pipe_watchdog_->Read(&crash_dump_path_[0], size);

    int retval = chdir(GetParentPath(crash_dump_path_).c_str());
    if (retval != 0) {
      LogEmergency(std::string("Cannot change to crash dump directory: ") +
                   crash_dump_path_);
      return false;
    }
    crash_dump_path_ = GetFileName(crash_dump_path_);
  }
  return true;
}

/**
 * Set up the signal handling and kick off the supervision.
 */
void Watchdog::Spawn(const std::string &crash_dump_path) {
  // lower restrictions for ptrace
  if (!platform_allow_ptrace(watchdog_pid_)) {
    LogCvmfs(kLogMonitor, kLogSyslogWarn,
             "failed to allow ptrace() for watchdog (PID: %d). "
             "Post crash stacktrace might not work",
             watchdog_pid_);
  }

  // Extra stack for signal handlers
  int stack_size = kSignalHandlerStacksize;  // 2 MB
  sighandler_stack_.ss_sp = smalloc(stack_size);
  sighandler_stack_.ss_size = stack_size;
  sighandler_stack_.ss_flags = 0;
  if (sigaltstack(&sighandler_stack_, NULL) != 0)
    PANIC(NULL);

  // define our crash signal handler
  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_sigaction = SendTrace;
  sa.sa_flags = SA_SIGINFO | SA_ONSTACK;
  sigfillset(&sa.sa_mask);

  SigactionMap signal_handlers;
  for (size_t i = 0; i < sizeof(g_crash_signals)/sizeof(int); i++) {
    signal_handlers[g_crash_signals[i]] = sa;
  }
  old_signal_handlers_ = SetSignalHandlers(signal_handlers);

  pipe_terminate_ = new Pipe<kPipeThreadTerminator>();
  int retval =
    pthread_create(&thread_listener_, NULL, MainWatchdogListener, this);
  assert(retval == 0);

  pipe_watchdog_->Write(ControlFlow::kSupervise);
  size_t path_size = crash_dump_path.size();
  pipe_watchdog_->Write(path_size);
  if (path_size > 0) {
    pipe_watchdog_->Write(crash_dump_path.data(), path_size);
  }

  spawned_ = true;
}


void *Watchdog::MainWatchdogListener(void *data) {
  Watchdog *watchdog = static_cast<Watchdog *>(data);
  LogCvmfs(kLogMonitor, kLogDebug, "starting watchdog listener");

  struct pollfd watch_fds[2];
  watch_fds[0].fd = watchdog->pipe_listener_->GetReadFd();
  watch_fds[0].events = 0;  // Only check for POLL[ERR,HUP,NVAL] in revents
  watch_fds[0].revents = 0;
  watch_fds[1].fd = watchdog->pipe_terminate_->GetReadFd();
  watch_fds[1].events = POLLIN | POLLPRI;
  watch_fds[1].revents = 0;
  while (true) {
    int retval = poll(watch_fds, 2, -1);
    if (retval < 0) {
      continue;
    }

    // Terminate I/O thread
    if (watch_fds[1].revents)
      break;

    if (watch_fds[0].revents) {
      if ((watch_fds[0].revents & POLLERR) ||
          (watch_fds[0].revents & POLLHUP) ||
          (watch_fds[0].revents & POLLNVAL))
      {
        LogCvmfs(kLogMonitor, kLogDebug | kLogSyslogErr,
                 "watchdog disappeared, disabling stack trace reporting "
                 "(revents: %d / %d|%d|%d)",
                 watch_fds[0].revents, POLLERR, POLLHUP, POLLNVAL);
        watchdog->SetSignalHandlers(watchdog->old_signal_handlers_);
        PANIC(kLogDebug | kLogSyslogErr, "watchdog disappeared, aborting");
      }
      PANIC(NULL);
    }
  }

  LogCvmfs(kLogMonitor, kLogDebug, "stopping watchdog listener");
  return NULL;
}


void Watchdog::Supervise() {
  ControlFlow::Flags control_flow = ControlFlow::kUnknown;

  if (!pipe_watchdog_->TryRead<ControlFlow::Flags>(&control_flow)) {
    LogEmergency("watchdog: unexpected termination (" +
                 StringifyInt(control_flow) + ")");
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
        LogEmergency("watchdog: unexpected error");
        break;
    }
  }
}


Watchdog::Watchdog(FnOnCrash on_crash)
  : spawned_(false)
  , exe_path_(string(platform_getexepath()))
  , watchdog_pid_(0)
  , on_crash_(on_crash)
{
  int retval = platform_spinlock_init(&lock_handler_, 0);
  assert(retval == 0);
  memset(&sighandler_stack_, 0, sizeof(sighandler_stack_));
}


Watchdog::~Watchdog() {
  if (spawned_) {
    // Reset signal handlers
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

    pipe_terminate_->Write(ControlFlow::kQuit);
    pthread_join(thread_listener_, NULL);
    pipe_terminate_->Close();
  }

  pipe_watchdog_->Write(ControlFlow::kQuit);
  pipe_watchdog_->CloseWriteFd();
  pipe_listener_->CloseReadFd();

  platform_spinlock_destroy(&lock_handler_);
  LogCvmfs(kLogMonitor, kLogDebug, "monitor stopped");
  instance_ = NULL;
}
