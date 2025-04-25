/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_MONITOR_H_
#define CVMFS_MONITOR_H_

#include <pthread.h>
#include <signal.h>
#include <sys/types.h>
#include <unistd.h>

#include <map>
#include <string>

#include "util/pipe.h"
#include "util/platform.h"
#include "util/pointer.h"
#include "util/single_copy.h"

/**
 * This class can fork a watchdog process that listens on a pipe and prints a
 * stackstrace into syslog, when cvmfs fails.  The crash dump is also appended
 * to the crash dump file, if the path is not empty.  Singleton.
 *
 * The watchdog process if forked on Create and put on hold.  Spawn() will start
 * the supervision and set the crash dump path. It should be called from the
 * final supervisee pid (after daemon etc.) but preferably before any threads
 * are started.
 *
 * Note: logging should be set up before calling Create()
 */
class Watchdog : SingleCopy {
 public:
  /**
   * Crash cleanup handler signature.
   */
  typedef void (*FnOnCrash)(void);

  static Watchdog *Create(FnOnCrash on_crash);
  static pid_t GetPid();
  ~Watchdog();
  void Spawn(const std::string &crash_dump_path);

  /**
   * Signals that watchdog should not receive. If it does, report and exit.
   */
  static int g_suppressed_signals[13];
  /**
   * Signals used by crash signal handler. If received, create a stack trace.
   */
  static int g_crash_signals[8];

 private:
  typedef std::map<int, struct sigaction> SigactionMap;

  struct CrashData {
    int signal;
    int sys_errno;
    pid_t pid;
  };

  struct ControlFlow {
    enum Flags {
      kProduceStacktrace = 0,
      kQuit,
      kSupervise,
      kUnknown,
    };
  };

  /**
   * Preallocated memory block to make sure that signal handler don't run into
   * stack overflows.
   */
  static const unsigned kSignalHandlerStacksize = 2 * 1024 * 1024;  // 2 MB
  /**
   * If the GDB/LLDB method of generating a stack trace fails, fall back to
   * libc's backtrace with a maximum depth.
   */
  static const unsigned kMaxBacktrace = 64;

  static Watchdog *instance_;
  static Watchdog *Me() { return instance_; }

  static void *MainWatchdogListener(void *data);

  static void ReportSignalAndContinue(int sig, siginfo_t *siginfo,
                                       void *context);
  static void SendTrace(int sig, siginfo_t *siginfo, void *context);

  explicit Watchdog(FnOnCrash on_crash);
  void Fork();
  bool WaitForSupervisee();
  SigactionMap SetSignalHandlers(const SigactionMap &signal_handlers);
  void Supervise();
  void LogEmergency(std::string msg);
  std::string ReportStacktrace();
  std::string GenerateStackTrace(pid_t pid);
  std::string ReadUntilGdbPrompt(int fd_pipe);

  bool spawned_;
  std::string crash_dump_path_;
  std::string exe_path_;
  pid_t watchdog_pid_;
  UniquePtr<Pipe<kPipeWatchdog> > pipe_watchdog_;
  /// The supervisee makes sure its watchdog does not die
  UniquePtr<Pipe<kPipeWatchdogSupervisor> > pipe_listener_;
  /// Send the terminate signal to the listener
  UniquePtr<Pipe<kPipeThreadTerminator> > pipe_terminate_;
  pthread_t thread_listener_;
  FnOnCrash on_crash_;
  platform_spinlock lock_handler_;
  stack_t sighandler_stack_;
  SigactionMap old_signal_handlers_;
};

#endif  // CVMFS_MONITOR_H_
