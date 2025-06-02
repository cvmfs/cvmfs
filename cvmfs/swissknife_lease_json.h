/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_LEASE_JSON_H_
#define CVMFS_SWISSKNIFE_LEASE_JSON_H_

#include <cstdint>
#include <string>

#include "swissknife_lease_curl.h"

enum LeaseReply {
  kLeaseReplySuccess,
  kLeaseReplyBusy,
  kLeaseReplyFailure
};

LeaseReply ParseAcquireReply(const CurlBuffer &buffer,
                             std::string *session_token);
LeaseReply ParseAcquireReplyWithRevision(const CurlBuffer& buffer,
                                         std::string* session_token,
                                         uint64_t *current_revision,
                                         std::string &current_root_hash);
LeaseReply ParseDropReply(const CurlBuffer &buffer);

#endif  // CVMFS_SWISSKNIFE_LEASE_JSON_H_
