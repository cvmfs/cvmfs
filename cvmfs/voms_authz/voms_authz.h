
#ifndef __VOMS_AUTHZ_H_
#define __VOMS_AUTHZ_H_

#include <sys/types.h>

#include <string>

struct fuse_ctx;

bool CheckVOMSAuthz(const struct fuse_ctx *ctx, const std::string &);

bool ConfigureCurlHandle(void *curl_handle, pid_t pid, uid_t uid, gid_t gid, char *&fname);

FILE * GetProxyFile(pid_t pid, uid_t uid, gid_t gid);

#endif

