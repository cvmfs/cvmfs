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
string *helper_script_path_ = NULL;
string *helper_script_gdb_cmd_path_;
bool spawned_ = false;
unsigned max_open_files_;
int pipe_wd_[2];
platform_spinlock lock_handler_;
stack_t sighandler_stack_;
pid_t watchdog_pid_ = 0;

pid_t GetPid() {
  if (!spawned_) {
    return cvmfs::pid_;
  }

  return watchdog_pid_;
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
static string GenerateStackTraceAndKill(const string &exe_path,
                                        const pid_t pid)
{
  int retval;

  int fd_stdin;
  int fd_stdout;
  int fd_stderr;
  retval = Shell(&fd_stdin, &fd_stdout, &fd_stderr);
  assert(retval);

  // Let the shell execute the stacktrace extraction script
  const string bt_cmd = *helper_script_path_ + " " + exe_path + " " +
                        StringifyInt(pid) + " " +
                        *helper_script_gdb_cmd_path_ + " 2>&1\n";
  WritePipe(fd_stdin, bt_cmd.data(), bt_cmd.length());

  // close the standard in to close the shell
  close(fd_stdin);

  // read the stack trace from the stdout of our shell
  string result = "";
  char mini_buffer;
  int chars_io;
  while (1) {
    chars_io = read(fd_stdout, &mini_buffer, 1);
    if (chars_io <= 0) break;
    result += mini_buffer;
  }

  // check if the stacktrace readout went fine
  if (chars_io < 0) {
    result += "failed to read stack traces";
  }

  // close the pipes to the shell
  close(fd_stderr);
  close(fd_stdout);

  // give the dying cvmfs client the finishing stroke
  if (kill(pid, SIGKILL) != 0) {
    result += "Failed to kill cvmfs client!\n\n";
  }

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

  debug += GenerateStackTraceAndKill(*exe_path_, pid);

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


static bool CreateStacktraceScript(const std::string &cache_dir,
                                   const std::string &process_name) {
  const string script =
"#!/bin/sh\n"
"\n"
"# This script is almost identical to /usr/bin/gstack.\n"
"# It is used by monitor.cc on Linux and MacOS X.\n"
"\n"
"# Note: this script was taken from the ROOT svn repository\n"
"#       and slightly adapted to print a stacktrace to stdout instead\n"
"#       of an output file.\n"
"\n"
"tempname=`basename $0 .sh`\n"
"\n"
"if test $# -lt 3; then\n"
"   echo \"Usage: ${tempname} <executable> <process-id> <gdb cmd file>\" 1>&2\n"
"   exit 1\n"
"fi\n"
"\n"
"if [ `uname -s` = \"Darwin\" ]; then\n"
"\n"
"   if test ! -x $1; then\n"
"      echo \"${tempname}: process $1 not found.\" 1>&2\n"
"      exit 1\n"
"   fi\n"
"\n"
"   GDB=${GDB:-gdb}\n"
"\n"
"   # Run GDB, strip out unwanted noise.\n"
"   $GDB -q -batch -x $3 -n $1 $2 2>&1  < /dev/null |\n"
"   /usr/bin/sed -n \\\n"
"    -e 's/^(gdb) //' \\\n"
"    -e '/^#/p' \\\n"
"    -e 's/\\(^Thread.*\\)/@\1/p' | tr \"@\" \"\\n\" > /dev/stdout\n"
"\n"
"   rm -f $TMPFILE\n"
"\n"
"else\n"
"\n"
"   if test ! -r /proc/$2; then\n"
"      echo \"${tempname}: process $2 not found.\" 1>&2\n"
"      exit 1\n"
"   fi\n"
"\n"
"   GDB=${GDB:-gdb}\n"
"\n"
"   # Run GDB, strip out unwanted noise.\n"
"   $GDB -q -batch -x $3 -n $1 $2 |\n"
"   /bin/sed -n \\\n"
"      -e 's/^(gdb) //' \\\n"
"      -e '/^#/p' \\\n"
"      -e '/^   /p' \\\n"
"      -e 's/\\(^Thread.*\\)/@\1/p' | tr '@' '\\n' > /dev/stdout\n"
"fi\n"
"";

  const string gdb = "thread apply all bt\n";

  //
  // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  //

  FILE *fp = fopen(helper_script_path_->c_str(), "w");

  if (!fp ||
      fwrite(script.data(), 1, script.length(), fp) != script.length() ||
      (chmod(helper_script_path_->c_str(), S_IRUSR | S_IWUSR | S_IXUSR) != 0))
  {
    LogCvmfs(kLogMonitor, kLogStderr, "Failed to create gdb helper script (%d)",
             errno);
    goto create_stacktrace_fail;
  }
  fclose(fp);

  fp = fopen(helper_script_gdb_cmd_path_->c_str(), "w");
  if (!fp ||
      fwrite(gdb.data(), 1, gdb.length(), fp) != gdb.length() ||
      (chmod(helper_script_gdb_cmd_path_->c_str(), S_IRUSR | S_IWUSR) != 0))
  {
    LogCvmfs(kLogMonitor, kLogStderr,
             "Failed to create gdb helper helper script (%d)");
    goto create_stacktrace_fail;
  }
  fclose(fp);

  return true;

 create_stacktrace_fail:
  if (fp)
    fclose(fp);
  return false;
}


bool Init(const string &cache_dir, const std::string &process_name,
          const bool check_max_open_files)
{
  monitor::cache_dir_ = new string(cache_dir);
  monitor::process_name_ = new string(process_name);
  monitor::exe_path_ = new string(platform_getexepath());
  monitor::helper_script_path_ =
    new string(cache_dir + "/gdb-helper." + process_name + ".sh");
  monitor::helper_script_gdb_cmd_path_ =
    new string(cache_dir + "/gdb-helper-gdb-cmd." + process_name);
  if (platform_spinlock_init(&lock_handler_, 0) != 0) return false;

  // create stacktrace retrieve script in cache directory
  if (!CreateStacktraceScript(cache_dir, process_name)) {
    delete monitor::cache_dir_;
    delete monitor::exe_path_;
    delete monitor::process_name_;
    delete monitor::helper_script_path_;
    delete monitor::helper_script_gdb_cmd_path_;
    monitor::cache_dir_ = NULL;
    monitor::process_name_ = NULL;
    monitor::exe_path_ = NULL;
    monitor::helper_script_path_ = NULL;
    monitor::helper_script_gdb_cmd_path_ = NULL;
    return false;
  }

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
    unlink(helper_script_path_->c_str());
    unlink(helper_script_gdb_cmd_path_->c_str());
  }
  delete process_name_;
  delete cache_dir_;
  delete exe_path_;
  delete helper_script_path_;
  delete helper_script_gdb_cmd_path_;
  process_name_ = NULL;
  cache_dir_ = NULL;
  exe_path_ = NULL;
  helper_script_path_ = NULL;
  helper_script_gdb_cmd_path_ = NULL;
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
