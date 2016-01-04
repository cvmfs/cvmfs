/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_VOMS_AUTHZ_VOMS_CRED_H_
#define CVMFS_VOMS_AUTHZ_VOMS_CRED_H_

#include <sys/types.h>
#include <unistd.h>

#include <cstdio>

// TODO(jblomer): unit test this class

/**
 * This implements the credential fetcher server. Communicating over a dedicated
 * file descriptor (3), this will pull credentials from a given external
 * process.
 *
 * The server sends back a file descriptor to a grid certificate.  The server
 * needs root privileges because it looks into other processes environments.
 */
class CredentialsFetcher {
 public:
  static const int kTransportFd = 3;

  /**
   * Protocol steering between client and server.
   */
  enum FetcherCommands {
    kCmdExecErr = 1,
    kCmdCredReq,
    kCmdChildExit,
    kCmdCredHandle
  };

  static int MainCredentialsFetcher(int argc, char *argv[]);
  static bool GetProxyFileFromEnv(const pid_t pid, const size_t path_len,
                                  char *path);
  static FILE *GetProxyFileInternal(pid_t pid, uid_t uid, gid_t gid);
};

#endif  // CVMFS_VOMS_AUTHZ_VOMS_CRED_H_
