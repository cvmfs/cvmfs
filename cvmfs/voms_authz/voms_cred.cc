/**
 * This file is part of the CernVM File System.
 *
 * This file implements the parent-side portion of the communication with the
 * cvmfs_cred_fetcher.
 */

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>

#ifndef __APPLE__
#include <sys/syscall.h>
#endif

#include <cstring>
#include <string>
#include <vector>

#include "../logging.h"
#include "../util_concurrency.h"
#include "voms_cred.h"

static void
ReportChildDeath(pid_t pid, int flags) {
  int status = 0;
  int retval = waitpid(pid, &status, flags);
  if (-1 == retval) {
    LogCvmfs(kLogVoms, kLogWarning, "Failed to get child process %d final "
             "status: %s (errno=%d)", pid, strerror(errno), errno);
    return;
  } else if (0 == retval) {
    LogCvmfs(kLogVoms, kLogWarning, "Internal state error: child process %d "
             "has closed its socket but OS thinks the process is still alive.",
             pid);
    return;
  }
  if (WIFEXITED(status)) {
    int exitcode = WEXITSTATUS(status);
    LogCvmfs(kLogVoms, exitcode ? kLogWarning : kLogDebug,
             "Child process terminated normally with status %d.", exitcode);
  } else if (WIFSIGNALED(status)) {
    int exitsig = WTERMSIG(status);
    LogCvmfs(kLogVoms, kLogWarning, "Child process terminated by signal %s"
             " (%d).", strsignal(exitsig), exitsig);
  }
}


/**
 * Manage helper forked process
 *
 * TODO(jblomer): member naming: no m_ prefix, trailing underscore
 * TODO(jblomer): put into anonymous namespace
 */
struct ProxyHelper {
  // TODO(jblomer): remove magic number
  ProxyHelper() : m_subprocess(-1), m_max_files(1024) {
    pthread_mutex_init(&m_helper_mutex, NULL);

    // TODO(jblomer): make a utility function, this code is also used in the
    // monitor
    rlimit rlim;
    if (-1 != getrlimit(RLIMIT_NOFILE, &rlim)) {
      if ((rlim.rlim_cur != RLIM_INFINITY) && (m_max_files < rlim.rlim_cur)) {
        m_max_files = rlim.rlim_cur;
      }
      if ((rlim.rlim_max != RLIM_INFINITY) && (m_max_files < rlim.rlim_max)) {
        m_max_files = rlim.rlim_max;
      }
    }

    const char *path = getenv("PATH");
    if (path && (path[0] == '\0')) {
      path = NULL;
    }

    char buf[PATH_MAX], buf2[PATH_MAX];
    if ((path == NULL) && (confstr(_CS_PATH, buf, PATH_MAX) < PATH_MAX)) {
      path = buf;
    } else if (path == NULL) {
      return;
    }
    // TODO(jblomer): use SplitString from util
    const char *next_delim;
    while ((next_delim = strstr(path, ":"))) {
      size_t copy_size = (next_delim-path) < PATH_MAX ? next_delim-path
                                                      : PATH_MAX-1;
      strncpy(buf2, path, copy_size);
      buf2[copy_size] = '\0';
      m_paths.push_back(buf2);
      path = next_delim + 1;
    }
    if (path && (path[0] != '\0')) {
      m_paths.push_back(path);
    }
  }


  ~ProxyHelper() {
    MutexLockGuard guard(m_helper_mutex);
    if (m_subprocess > 0) {
      // Tell child process to exit with status 0.
      InformChild(CredentialsFetcher::kCmdChildExit, 0);
      // TODO(bbockelm): It may be beneficial to have a timeout here.
      ReportChildDeath(m_subprocess, 0);
    }
  }


  bool CheckHelperLaunched() {
    MutexLockGuard guard(m_helper_mutex);
    if (m_subprocess != -1) {
      return m_subprocess != 0;
    }
    LogCvmfs(kLogVoms, kLogDebug, "About to launch credential fetcher.");

    m_subprocess = 0;

    // Pre-sockets to communicate with helper process
    int socks[2];
    if (-1 == socketpair(AF_UNIX, SOCK_STREAM, 0, socks)) {
      LogCvmfs(kLogVoms, kLogWarning, "Failed to create socket pair for "
               "fetcher process communication: %s (errno=%d)",
               strerror(errno), errno);
    }

    pid_t fork_pid = fork();
    if (fork_pid == -1) {
      LogCvmfs(kLogVoms, kLogWarning, "Failed to launch credential fetcher "
               "process: %s (errno=%d)", strerror(errno), errno);
      m_subprocess = 0;
      return false;
    } else if (!fork_pid) {  // Child
      close(socks[0]);
      signal(SIGABRT, SIG_DFL);
      ExecFetcher(socks[1]);
      abort();  // Above function should *never* return.
    }
    LogCvmfs(kLogVoms, kLogDebug, "Launched credential fetcher process %d",
             fork_pid);
    close(socks[1]);
    m_sock = socks[0];
    m_subprocess = fork_pid;
    return true;
  }


  void ExecFetcher(int unix_sock) {
    // TODO(jblomer): remove magic number
    dup2(unix_sock, 3);  // Always lives on FD 3.
    for (rlim_t idx=4; idx < m_max_files; idx++) {
      close(idx);
    }
    char *args[3];
    char executable_name[] = "cvmfs2";
    char process_flavor[] = "__cred_fetcher__";
    args[0] = executable_name;
    args[1] = process_flavor;
    args[2] = NULL;
    // NOTE: We have forked from a threaded process.  We do
    // not know the status of any other mutex in the program - particularly,
    // those related to formatting strings or logging.  Hence, we CANNOT
    // call LogCvmfs from here (or allocate any memory on the heap).
    char full_path[PATH_MAX];
    for (std::vector<std::string>::const_iterator it = m_paths.begin();
         it != m_paths.end();
         it++)
    {
      if (it->size() + 20 > PATH_MAX) {continue;}
      memcpy(full_path, it->c_str(), it->size());
      full_path[it->size()] = '/';
      strcpy(full_path+it->size()+1, executable_name); // NOLINT
      execv(full_path, args);
    }
    struct msghdr msg;
    memset(&msg, '\0', sizeof(msg));
    struct iovec iov[2];
    int command = 1;
    iov[0].iov_base = &command;
    iov[0].iov_len = sizeof(command);
    int value = errno;
    iov[1].iov_base = &value;
    iov[1].iov_len = sizeof(value);
    msg.msg_iov = iov;
    msg.msg_iovlen = 2;
    sendmsg(3, &msg, MSG_NOSIGNAL);  // Ignore error handling.
    abort();
  }


  void InformChild(int cmd, int val) {
    struct msghdr msg_send;
    memset(&msg_send, '\0', sizeof(msg_send));
    struct iovec iov[2];
    iov[0].iov_base = &cmd;
    iov[0].iov_len = sizeof(cmd);
    iov[1].iov_base = &val;
    iov[1].iov_len = sizeof(val);
    msg_send.msg_iov = iov;
    msg_send.msg_iovlen = 2;
    errno = 0;
    while (-1 == sendmsg(m_sock, &msg_send, MSG_NOSIGNAL) && errno == EINTR) {}
    if (errno) {
      int result = errno;
      // Socket is disconnected; child has died.
      if (errno == ENOTCONN || errno == EPIPE) {
        ReportChildDeath(m_subprocess, WNOHANG);
        m_subprocess = -1;
      }
      LogCvmfs(kLogVoms, kLogWarning, "Failed to send messaage (command %d, "
               "value %d) to child: %s (errno=%d)", cmd, val, strerror(result),
               result);
    }
  }


  // TODO(jblomer): more error handling here: if the user proxy certificate
  // does not exist, it is hard to figure out.
  FILE *GetProxyFile(pid_t pid, uid_t uid, gid_t gid) {
    if (!CheckHelperLaunched()) {return NULL;}

    MutexLockGuard guard(m_helper_mutex);
    LogCvmfs(kLogVoms, kLogDebug, "Sending request to child for pid=%d, "
             "UID=%d, GID=%d", pid, uid, gid);
    // Send a credential request to child.
    struct msghdr msg_send;
    memset(&msg_send, '\0', sizeof(msg_send));
    struct iovec iov[4];
    int command = CredentialsFetcher::kCmdCredReq;
    iov[0].iov_base = &command;
    iov[0].iov_len = sizeof(command);
    iov[1].iov_base = &pid;
    iov[1].iov_len = sizeof(pid);
    iov[2].iov_base = &uid;
    iov[2].iov_len = sizeof(uid);
    iov[3].iov_base = &gid;
    iov[3].iov_len = sizeof(gid);
    msg_send.msg_iov = iov;
    msg_send.msg_iovlen = 4;
    errno = 0;
    while (-1 == sendmsg(m_sock, &msg_send, MSG_NOSIGNAL) && errno == EINTR) {}
    if (errno) {
      int result = errno;
      // Socket is disconnected; child has died.
      if (errno == ENOTCONN || errno == EPIPE) {
        ReportChildDeath(m_subprocess, WNOHANG);
        m_subprocess = -1;
      }
      LogCvmfs(kLogVoms, kLogWarning, "Failed to send messaage to child: %s"
               " (errno=%d)", strerror(result), result);
      return NULL;
    }

    // Hang around waiting for a response.
    struct msghdr msg_recv;
    memset(&msg_recv, '\0', sizeof(msg_recv));
    command = 0;
    int result = 0;
    iov[0].iov_base = &command;
    iov[0].iov_len = sizeof(command);
    iov[1].iov_base = &result;
    iov[1].iov_len = sizeof(result);
    msg_recv.msg_iov = iov;
    msg_recv.msg_iovlen = 2;
    int fd = -1;
    char cbuf[CMSG_SPACE(sizeof(fd))];
    memset(cbuf, '\0', CMSG_SPACE(sizeof(fd)));
    msg_recv.msg_control = cbuf;
    msg_recv.msg_controllen = CMSG_SPACE(sizeof(fd));

    errno = 0;
    // TODO(bbockelm): Implement timeouts.
    while (-1 == recvmsg(m_sock, &msg_recv, 0) && errno == EINTR) {}
    if (errno) {
      int result = errno;
      // Socket is disconnected; child has died.
      if (errno == ENOTCONN || errno == EPIPE) {
        MutexLockGuard guard(m_helper_mutex);
        ReportChildDeath(m_subprocess, WNOHANG);
        m_subprocess = -1;
      }
      LogCvmfs(kLogVoms, kLogWarning, "Failed to receive messaage from child:"
               " %s (errno=%d)", strerror(result), result);
    }
    if (command != 4) {
      if (command == 1) {  // Child was unable to exec.
        LogCvmfs(kLogVoms, kLogWarning, "Child process was unable to execute "
                 "cvmfs_cred_fetcher: %s (errno=%d)", strerror(result), result);
        MutexLockGuard guard(m_helper_mutex);
        ReportChildDeath(m_subprocess, 0);
        m_subprocess = -1;
      }
      return NULL;
    }

    struct cmsghdr *cmsg = NULL;
    for (cmsg = CMSG_FIRSTHDR(&msg_recv);
         cmsg != NULL;
         cmsg = CMSG_NXTHDR(&msg_recv, cmsg))
    {
      if ((cmsg->cmsg_level == SOL_SOCKET) &&
          (cmsg->cmsg_type  == SCM_RIGHTS))
      {
        fd = *(reinterpret_cast<int *>(CMSG_DATA(cmsg)));
        LogCvmfs(kLogVoms, kLogDebug, "Credential fetcher send back FD %d", fd);
      }
    }

    if (result) {
      if (fd > -1) {close(fd);}
      LogCvmfs(kLogVoms, kLogWarning, "Credential fetcher process had error: "
               "%s (errno=%d)", strerror(result), result);
      return NULL;
    }
    return fdopen(fd, "r");
  }


  // PID of the subprocess; -1 indicates launch hasn't
  // been attempted.  0 indicates launch was attempted but process failed
  pid_t m_subprocess;
  rlim_t m_max_files;
  int m_sock;
  pthread_mutex_t m_helper_mutex;
  std::vector<std::string> m_paths;
};

// TODO(jblomer): make it a singleton (well-defined initialization time)
static ProxyHelper g_instance;

FILE *
GetProxyFile(pid_t pid, uid_t uid, gid_t gid) {
  return g_instance.GetProxyFile(pid, uid, gid);
}
