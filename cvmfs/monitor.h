/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_MONITOR_H_
#define CVMFS_MONITOR_H_

#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <sys/types.h>
#include <unistd.h>

#include <map>
#include <memory>
#include <string>

#include "util/pipe.h"
#include "util/platform.h"
#include "util/single_copy.h"


/**
 * Information for the FUSE module to communicate with the watchdog process
 * that needs to be preserved through reloads.
 */
class WatchdogState {
  friend class Watchdog;

 public:
  WatchdogState()
      : version(0)
      , watchdog_write_fd(-1)
      , listener_read_fd(-1)
      , spawned(false)
      , pid(0) { }

 private:
  unsigned version;
  int watchdog_write_fd;
  int listener_read_fd;
  bool spawned;
  pid_t pid;
};


/**
 * The passthrough broker protocol, one request and one reply per call over a
 * SOCK_SEQPACKET socket.  Exposed for the unit tests.
 */
namespace backing_broker {
enum Op {
  kSetFuseDevice = 1,
  kBackingOpen,
  kBackingClose,
};
/**
 * Sends op with arg and fd (-1 for none); returns the reply, or -errno when
 * the socket fails or times out.
 */
int Call(int sock, int op, int arg, int fd);
/**
 * Answers requests on sock until the peer closes it.
 */
void Serve(int sock);
}  // namespace backing_broker


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
  typedef void (*FnOnExit)(const bool crashed);

  static Watchdog *Create(FnOnExit on_exit,
                          bool needs_read_environ,
                          WatchdogState *saved_state = 0,
                          bool passthrough_broker = false);
  static pid_t GetPid();
  ~Watchdog();
  void Spawn(const std::string &crash_dump_path);
  void ClearOnExitFn() { on_exit_ = NULL; }
  void EnterMaintenanceMode() { maintenance_mode_ = true; }
  void SaveState(WatchdogState *state);

  /**
   * FUSE passthrough needs CAP_SYS_ADMIN for registering backing files.  The
   * client drops it (#3730); the watchdog of the FUSE module keeps it for the
   * unmount.  With passthrough_broker set, the client hands that watchdog the
   * /dev/fuse descriptor once and then each backing file, and the watchdog
   * issues the ioctl.  Callers serialize the calls; the FUSE module does so
   * under its passthrough tracker lock.  Each returns 0, a backing id, or
   * -errno.  A failed or timed-out exchange closes the broker for good.
   */
  bool HasBroker() const { return broker_fd_ >= 0; }
  int BrokerSetFuseDevice(int fuse_fd);
  int BrokerBackingOpen(int fd);
  int BrokerBackingClose(int backing_id);

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
      kQuitWithExit,
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
  static void *MainBackingBroker(void *data);
  int BrokerCall(int op, int arg, int fd);

  static void ReportSignalAndContinue(int sig, siginfo_t *siginfo,
                                      void *context);
  static void SendTrace(int sig, siginfo_t *siginfo, void *context);

  explicit Watchdog(FnOnExit on_exit);
  void Fork(bool needs_read_environ, bool passthrough_broker);
  void RestoreState(WatchdogState *saved_state);
  bool WaitForSupervisee();
  SigactionMap SetSignalHandlers(const SigactionMap &signal_handlers);
  void Supervise();
  void LogEmergency(std::string msg);
  std::string ReportStacktrace();
  std::string GenerateStackTrace(pid_t pid);
  std::string ReadUntilGdbPrompt(int fd_pipe);

  bool spawned_;
  bool maintenance_mode_;
  std::string crash_dump_path_;
  std::string exe_path_;
  pid_t watchdog_pid_;
  std::unique_ptr<Pipe<kPipeWatchdog> > pipe_watchdog_;
  /// The supervisee makes sure its watchdog does not die
  std::unique_ptr<Pipe<kPipeWatchdogSupervisor> > pipe_listener_;
  /// Send the terminate signal to the listener
  std::unique_ptr<Pipe<kPipeThreadTerminator> > pipe_terminate_;
  pthread_t thread_listener_;
  int broker_fd_;       /**< Client end of the passthrough broker, or -1 */
  int broker_peer_fd_;  /**< Watchdog end, only in the watchdog process */
  FnOnExit on_exit_;
  platform_spinlock lock_handler_;
  stack_t sighandler_stack_;
  SigactionMap old_signal_handlers_;
};

#endif  // CVMFS_MONITOR_H_
