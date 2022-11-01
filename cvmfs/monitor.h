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

#include "util/platform.h"

#if defined(CVMFS_FUSE_MODULE)
#include "mountpoint.h"
#endif

struct Pipe;

/**
 * This class can fork a watchdog process that listens on a pipe and prints a
 * stackstrace into syslog, when cvmfs fails.  The crash dump is also appended
 * to the crash dump file, if the path is not empty.  Singleton.
 */
class Watchdog {
 public:
  static Watchdog *Create(const std::string &crash_dump_path);
  static pid_t GetPid();
  ~Watchdog();
#if defined(CVMFS_FUSE_MODULE)
  void Spawn( MountPoint *mountpoint_ );
#else
  void Spawn();
#endif
  void RegisterOnCrash(void (*CleanupOnCrash)(void));

  static void *MainWatchdogListener(void *data);

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

  static void ReportSignalAndTerminate(int sig, siginfo_t *siginfo,
                                       void *context);
  static void SendTrace(int sig, siginfo_t *siginfo, void *context);

  explicit Watchdog(const std::string &crash_dump_path);
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
  Pipe *pipe_watchdog_;
  /// The supervisee makes sure its watchdog does not die
  Pipe *pipe_listener_;
  /// Send the terminate signal to the listener
  Pipe *pipe_terminate_;
  pthread_t thread_listener_;
  void (*on_crash_)(void);
  platform_spinlock lock_handler_;
  stack_t sighandler_stack_;
  SigactionMap old_signal_handlers_;
};

namespace monitor {
// TODO(jblomer): move me
unsigned GetMaxOpenFiles();
}  // namespace monitor

#endif  // CVMFS_MONITOR_H_
