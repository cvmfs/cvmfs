/**
 * This file is part of the CernVM File System.
 */

#include "x509_helper_req.h"

#include <cstdio>

using namespace std;  // NOLINT

string AuthzRequest::Ident() const {
  char buf_pid[32];
  char buf_uid[32];
  char buf_gid[32];
  snprintf(buf_pid, sizeof(buf_pid), "%d", pid);
  snprintf(buf_uid, sizeof(buf_uid), "%d", uid);
  snprintf(buf_gid, sizeof(buf_gid), "%d", gid);
  return "{pid " + string(buf_pid) + ", " +
          "uid " + string(buf_uid) + ", " +
          "gid " + string(buf_gid) + "}";
}
