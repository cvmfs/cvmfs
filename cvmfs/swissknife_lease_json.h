/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_LEASE_JSON_H_
#define CVMFS_SWISSKNIFE_LEASE_JSON_H_

#include "swissknife_lease_curl.h"

bool ParseAcquireReply(const CurlBuffer& buffer, std::string& session_token);
bool ParseDropReply(const CurlBuffer& buffer);

#endif  // CVMFS_SWISSKNIFE_LEASE_JSON_H_
