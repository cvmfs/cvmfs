/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_LEASE_CURL_H_
#define CVMFS_SWISSKNIFE_LEASE_CURL_H_

#include <string>

#include "curl/curl.h"

struct CurlBuffer {
  std::string data;
};

bool MakeAcquireRequest(const std::string& key_id, const std::string& secret,
                        const std::string& repo_path,
                        const std::string& repo_service_url,
                        CurlBuffer* buffer);

bool MakeEndRequest(const std::string& method, const std::string& key_id,
                    const std::string& secret, const std::string& session_token,
                    const std::string& repo_service_url,
                    const std::string& request_payload, CurlBuffer* reply);

#endif  // CVMFS_SWISSKNIFE_LEASE_CURL_H_
