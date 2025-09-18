/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_LEASE_CURL_H_
#define CVMFS_SWISSKNIFE_LEASE_CURL_H_

#include <string>

#include "curl/curl.h"

class CurlBuffer {
 public:
  std::string data;
  CurlBuffer() : data("") { }
};

bool MakeAcquireRequest(const std::string &key_id, const std::string &secret,
                        const std::string &repo_path,
                        const std::string &repo_service_url, CurlBuffer *buffer,
                        const std::string &metadata = std::string());

bool MakeEndRequest(const std::string &method, const std::string &key_id,
                    const std::string &secret, const std::string &session_token,
                    const std::string &repo_service_url,
                    const std::string &request_payload, CurlBuffer *reply,
                    bool expect_final_revision = false);

#endif  // CVMFS_SWISSKNIFE_LEASE_CURL_H_
