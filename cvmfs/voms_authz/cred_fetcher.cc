
/**
 * This file is part of the CernVM File System.
 *
 * This implements the credential fetcher server.
 * Communicating over a dedicated file descriptor (3), this
 * will pull credentials from a given external process.
 */

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

#include <cstring>

#include "voms_cred.h"

static bool
GetProxyFileFromEnv(pid_t pid, char *path, size_t pathLen)
{
    static const char * const X509_USER_PROXY = "\0X509_USER_PROXY=";
    size_t X509_USER_PROXY_LEN = strlen(X509_USER_PROXY+1)+1;

    if (snprintf(path, pathLen, "/proc/%d/environ", pid) >=
        static_cast<int>(pathLen))
    {
        if (errno == 0) {errno = ERANGE;}
        return false;
    }
    int olduid = geteuid();
    // NOTE: we ignore return values of these syscalls; this code path
    // will work if cvmfs is FUSE-mounted as an unprivileged user.
    seteuid(0);

    FILE *fp = fopen(path, "r");
    if (!fp) {
        fprintf(stderr, "Failed to open environment file for pid %d.\n", pid);
        seteuid(olduid);
        return false;
    }

    char c = '\0';
    size_t idx = 0, keyIdx = 0;
    bool set_env = false;
    while (1) {
        if (c == EOF) {break;}
        if (keyIdx == X509_USER_PROXY_LEN) {
            if (idx >= pathLen - 1) {break;}
            if (c == '\0') {set_env = true; break;}
            path[idx++] = c;
        } else if (X509_USER_PROXY[keyIdx++] != c) {
            keyIdx = 0;
        }
        c = fgetc(fp);
    }
    fclose(fp);
    seteuid(olduid);

    if (set_env) {path[idx] = '\0';}
    return set_env;
}


static FILE *
GetProxyFileInternal(pid_t pid, uid_t uid, gid_t gid)
{
    char path[PATH_MAX];
    if (!GetProxyFileFromEnv(pid, path, PATH_MAX))
    {
        fprintf(stderr, "Could not find proxy in environment; using default "
                "location in /tmp/x509up_u%d.\n", uid);
        if (snprintf(path, PATH_MAX, "/tmp/x509up_u%d", uid) >= PATH_MAX)
        {
            if (errno == 0) {errno = ERANGE;}
            return NULL;
        }
    }
    fprintf(stderr, "Looking for proxy in file %s.\n", path);


    int olduid = geteuid();
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


int main(int argc, char *argv[]) {
  while (true) {
    struct msghdr msg_recv;
    memset(&msg_recv, '\0', sizeof(msg_recv));
    int command = 0;
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
    while (-1 == recvmsg(3, &msg_recv, NULL) && errno == EINTR) {}
    if (errno) {
      fprintf(stderr, "Failed to receive messaage from child: %s (errno=%d)\n",
              strerror(errno), errno);
      return 1;
    }

    if (command == kChildExit) {
      fprintf(stderr, "Got exit message from parent; exiting %d.\n", value);
      return value;
    } else if (command != kCredReq) {
      fprintf(stderr, "Got unknown command\n");
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
    fprintf(stderr, "Sending FD %d back to parent.\n", fd);

    command = kCredHandle;
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
    while (-1 == sendmsg(3, &msg_send, NULL) && errno == EINTR) {}
    if (errno) {
      fprintf(stderr, "Failed to send messaage to parent: %s"
              " (errno=%d)\n", strerror(errno), errno);
      return 1;
    }
    if (fd != -1) {
      close(fd);
      fd = -1;
    }
  }
}

