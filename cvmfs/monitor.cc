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
#include <map>

#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <cassert>

#include "platform.h"
#include "util.h"
#include "logging.h"
#include "smalloc.h"

#include "cvmfs.h"

using namespace std;  // NOLINT

namespace monitor {

const unsigned kMinOpenFiles = 8192;  /**< minmum threshold for the maximum
                                       number of open files */
const unsigned kMaxBacktrace = 64;  /**< reported stracktrace depth */
const unsigned kSignalHandlerStacksize = 2*1024*1024;  /**< 2 MB */

string *cache_dir_ = NULL;
string *process_name_ = NULL;
string *exe_path_ = NULL;
bool spawned_ = false;
int pipe_wd_[2];
platform_spinlock lock_handler_;
stack_t sighandler_stack_;
pid_t watchdog_pid_ = 0;

typedef std::map<int, struct sigaction> SigactionMap;
SigactionMap old_signal_handlers_;

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
static void SendTrace(int signal,
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
  (void) sigaction(SIGQUIT, &old_signal_handlers_[signal], NULL);

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

  cflow = 'Q';
  (void)write(pipe_wd_[1], &cflow, 1);

  // do not die before the stack trace was generated
  // kill -SIGQUIT <pid> will finish this
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
 * reads from the file descriptor until the specific gdb prompt
 * is reached or the pipe gets broken
 *
 * @param fd_pipe  the file descriptor of the pipe to be read
 * @return         the data read from the pipe
 */
static string ReadUntilGdbPrompt(int fd_pipe) {
  static const string gdb_prompt = "\n(gdb) ";

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
  if (! SwitchCredentials(0, getgid(), retrievable)) {
    result += "failed to re-gain root permissions... still give it a try\n";
  }

  // run gdb and attach to the dying cvmfs2 process
  int fd_stdin;
  int fd_stdout;
  int fd_stderr;
  std::vector<std::string> argv; // TODO: C++11 initializer list...
  argv.push_back("-q");
  argv.push_back("-n");
  argv.push_back(exe_path);
  argv.push_back(StringifyInt(pid));
  retval = ExecuteBinary(&fd_stdin, &fd_stdout, &fd_stderr, "gdb", argv);
  assert(retval);

  // skip the gdb startup rubbish
  ReadUntilGdbPrompt(fd_stdout);

  // send stacktrace command to gdb
  const string gdb_cmd = "thread apply all bt\n" // backtrace all threads
                         "quit\n";               // stop gdb
  WritePipe(fd_stdin, gdb_cmd.data(), gdb_cmd.length());

  // read the stack trace from the stdout of our gdb process
  result += ReadUntilGdbPrompt(fd_stdout) + "\n\n";
  close(fd_stderr);
  close(fd_stdout);
  close(fd_stdin);

  return result;
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

  debug += "Executable path: " + *exe_path_ + "\n";

  debug += GenerateStackTrace(*exe_path_, pid);

  // give the dying cvmfs client the finishing stroke
  if (kill(pid, SIGQUIT) != 0) {
    debug += "Failed to kill cvmfs client!\n\n";
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
    char quit = 'Q';
    (void)write(pipe_wd_[1], &quit, 1);
    close(pipe_wd_[1]);
  }
  delete process_name_;
  delete cache_dir_;
  delete exe_path_;
  process_name_ = NULL;
  cache_dir_ = NULL;
  exe_path_ = NULL;
  platform_spinlock_destroy(&lock_handler_);
}

/**
 * Fork watchdog.
 */
void Spawn() {
  int pipe_pid[2];
  MakePipe(pipe_wd_);
  MakePipe(pipe_pid);

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
          close(pipe_wd_[1]);
          Daemonize();
          // send the watchdog PID to cvmfs
          pid_t watchdog_pid = getpid();
          WritePipe(pipe_pid[1], (const void*)&watchdog_pid, sizeof(pid_t));
          close(pipe_pid[1]);
          // Close all unused file descriptors
          for (int fd = 0; fd < max_fd; fd++) {
            if (fd != pipe_wd_[0])
              close(fd);
          }
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

  // retrieve the watchdog PID from the pipe
  ReadPipe(pipe_pid[0], (void*)&watchdog_pid_, sizeof(pid_t));
  close(pipe_pid[0]);

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

unsigned GetMaxOpenFiles() {
  static unsigned max_open_files;
  static bool     already_done = false;

  /* check number of open files (lazy evaluation) */
  if (! already_done) {
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
    max_open_files = soft_limit;
    already_done   = true;
  }

  return max_open_files;
}

}  // namespace monitor
