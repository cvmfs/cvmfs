
/**
 * This file is part of the CernVM File System.
 *
 * This contains simple common enums between the credential
 * fetcher and client.
 */

#ifndef CVMFS_VOMS_AUTHZ_VOMS_CRED_H_
#define CVMFS_VOMS_AUTHZ_VOMS_CRED_H_

enum FetcherCommands {
  kExecErr = 1,
  kCredReq,
  kChildExit,
  kCredHandle
};

#endif  // CVMFS_VOMS_AUTHZ_VOMS_CRED_H_
