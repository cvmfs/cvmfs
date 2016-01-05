/**
 * This file is part of the CernVM File System.
 */

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

#include <cassert>
#include <cstring>

#include "../logging.h"
#include "voms_cred.h"


/**
 * For a given pid, extracts the X509_USER_PROXY path from the foreign
 * process' environment.  Stores the resulting path in the user provided buffer
 * path.
 */
bool CredentialsFetcher::GetProxyFileFromEnv(
  const pid_t pid,
  const size_t path_len,
  char *path)
{
  assert(path_len > 0);
  static const char * const X509_USER_PROXY = "\0X509_USER_PROXY=";
  size_t X509_USER_PROXY_LEN = strlen(X509_USER_PROXY + 1) + 1;

  if (snprintf(path, path_len, "/proc/%d/environ", pid) >=
      static_cast<int>(path_len))
  {
    if (errno == 0) {errno = ERANGE;}
    return false;
  }
  // TODO(jblomer): this shouldn't be necessary anymore.  Since this runs
  // as a separate process, at the beginning the process can acquire root
  // privileges.
  int olduid = geteuid();
  // NOTE: we ignore return values of these syscalls; this code path
  // will work if cvmfs is FUSE-mounted as an unprivileged user.
  seteuid(0);

  FILE *fp = fopen(path, "r");
  if (!fp) {
    LogCvmfs(kLogVoms, kLogSyslogErr | kLogDebug,
             "failed to open environment file for pid %d.", pid);
    seteuid(olduid);  // TODO(jblomer): remove
    return false;
  }

  // Look for X509_USER_PROXY in the environment and store the value in path
  char c = '\0';
  size_t idx = 0, key_idx = 0;
  bool set_env = false;
  while (1) {
    if (c == EOF) {break;}
    if (key_idx == X509_USER_PROXY_LEN) {
      if (idx >= path_len - 1) {break;}
      if (c == '\0') {set_env = true; break;}
      path[idx++] = c;
    } else if (X509_USER_PROXY[key_idx++] != c) {
      key_idx = 0;
    }
    c = fgetc(fp);
  }
  fclose(fp);
  seteuid(olduid);  // TODO(jblomer): remove me

  if (set_env) {path[idx] = '\0';}
  return set_env;
}


/**
 * Opens a read-only file pointer to the proxy certificate as a given user.
 * The path is either taken from X509_USER_PROXY environment from the given pid
 * or it is the default location /tmp/x509up_u<UID>
 */
FILE *CredentialsFetcher::GetProxyFileInternal(pid_t pid, uid_t uid, gid_t gid)
{
  char path[PATH_MAX];
  if (!GetProxyFileFromEnv(pid, PATH_MAX, path)) {
    LogCvmfs(kLogVoms, kLogDebug,
             "could not find proxy in environment; using default location "
             "in /tmp/x509up_u%d.", uid);
    if (snprintf(path, PATH_MAX, "/tmp/x509up_u%d", uid) >= PATH_MAX) {
      if (errno == 0) {errno = ERANGE;}
      return NULL;
    }
  }
  LogCvmfs(kLogVoms, kLogDebug, "looking for proxy in file %s", path);

  int olduid = geteuid();  // TODO(jblomer): remove me
  int oldgid = getegid();
  // NOTE the sequencing: we must be eUID 0
  // to change the UID and GID.
  seteuid(0);
  setegid(gid);
  seteuid(uid);

  FILE *fp = fopen(path, "r");

  seteuid(0);
  setegid(oldgid);
  seteuid(olduid);

  return fp;
}


/**
 * A command-response server.  It reveices the triplet pid, uid, gid and returns
 * a read-only file descriptor for the user's proxy certificate, taking into
 * account the environment of the pid.
 */
int CredentialsFetcher::MainCredentialsFetcher(int argc, char *argv[]) {
  LogCvmfs(kLogVoms, kLogDebug, "starting credentials fetcher");

  // TODO(jblomer): apply logging parameters
  // int foreground = String2Int64(argv[2]);
  // int syslog_level = String2Int64(argv[3]);
  // int syslog_facility = String2Int64(argv[4]);
  // vector<string> logfiles = SplitString(argv[5], ':');
  /*SetLogSyslogLevel(syslog_level);
  SetLogSyslogFacility(syslog_facility);
  if ((logfiles.size() > 0) && (logfiles[0] != ""))
    SetLogDebugFile(logfiles[0] + ".cachemgr");
  if (logfiles.size() > 1)
    SetLogMicroSyslog(logfiles[1]);

  if (!foreground)
    Daemonize();*/

  while (true) {
    struct msghdr msg_recv;
    memset(&msg_recv, '\0', sizeof(msg_recv));
    int command = 0;  // smallest command id is 1
    pid_t value = 0;
    uid_t uid;
    gid_t gid;
    iovec iov[4];
    iov[0].iov_base = &command;
    iov[0].iov_len = sizeof(command);
    iov[1].iov_base = &value;
    iov[1].iov_len = sizeof(value);
    iov[2].iov_base = &uid;
    iov[2].iov_len = sizeof(uid);
    iov[3].iov_base = &gid;
    iov[3].iov_len = sizeof(gid);
    msg_recv.msg_iov = iov;
    msg_recv.msg_iovlen = 4;

    errno = 0;
    // TODO(bbockelm): Implement timeouts.
    while (-1 == recvmsg(kTransportFd, &msg_recv, 0) && errno == EINTR) {}
    if (errno) {
      LogCvmfs(kLogVoms, kLogSyslogErr | kLogDebug,
               "failed to receive messaage from child: %s (errno=%d)",
               strerror(errno), errno);
      return 1;
    }

    if (command == kCmdChildExit) {
      LogCvmfs(kLogVoms, kLogDebug,
               "got exit message from parent; exiting %d.", value);
      return value;
    } else if (command != kCmdCredReq) {
      LogCvmfs(kLogVoms, kLogSyslogErr | kLogDebug, "got unknown command %d",
               command);
      abort();
    }

    // Parent has requested a new credential.
    FILE *fp = GetProxyFileInternal(value, uid, gid);
    int fd;
    if (fp == NULL) {
      fd = -1;
      value = ENOENT;
      if (errno) {value = errno;}
    } else {
      fd = fileno(fp);
      value = 0;
    }
    LogCvmfs(kLogVoms, kLogDebug, "sending FD %d back to parent", fd);

    command = kCmdCredHandle;
    struct msghdr msg_send;
    memset(&msg_send, '\0', sizeof(msg_send));
    struct cmsghdr *cmsg = NULL;
    char cbuf[CMSG_SPACE(sizeof(fd))];
    msg_send.msg_iov = iov;
    msg_send.msg_iovlen = 2;

    if (fd > -1) {
      msg_send.msg_control = cbuf;
      msg_send.msg_controllen = CMSG_SPACE(sizeof(fd));
      cmsg = CMSG_FIRSTHDR(&msg_send);
      cmsg->cmsg_level = SOL_SOCKET;
      cmsg->cmsg_type  = SCM_RIGHTS;
      cmsg->cmsg_len   = CMSG_LEN(sizeof(fd));
      *(reinterpret_cast<int*>(CMSG_DATA(cmsg))) = fd;
    }

    errno = 0;
    while (-1 == sendmsg(3, &msg_send, 0) && errno == EINTR) {}
    if (errno) {
      LogCvmfs(kLogVoms, kLogSyslogErr | kLogDebug,
               "failed to send messaage to parent: %s (errno=%d)",
               strerror(errno), errno);
      return 1;
    }
    if (fp != NULL) {
      fclose(fp);
      fd = -1;
    }
  }  // command loop
}
