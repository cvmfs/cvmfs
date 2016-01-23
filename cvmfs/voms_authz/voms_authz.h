/**
 * This file is part of the CernVM File System.
 *
 * Common header functions for determining the VOMS credential associated with a
 * client request.
 */

#ifndef CVMFS_VOMS_AUTHZ_VOMS_AUTHZ_H_
#define CVMFS_VOMS_AUTHZ_VOMS_AUTHZ_H_

#include <sys/types.h>

#include <string>

struct fuse_ctx;

bool CheckVOMSAuthz(const struct fuse_ctx *ctx, const std::string &);

bool ConfigureCurlHandle(void *curl_handle, pid_t pid, uid_t uid, gid_t gid,
                         char **fname, void **data);
void ReleaseCurlHandle(void *curl_handle, void *data);

FILE *GetProxyFile(pid_t pid, uid_t uid, gid_t gid);

#endif  // CVMFS_VOMS_AUTHZ_VOMS_AUTHZ_H_

