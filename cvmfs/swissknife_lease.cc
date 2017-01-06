/**
 * This file is part of the CernVM File System.
 */

#include "swissknife_lease.h"

#include <algorithm>
#include <fstream>
#include <sstream>

#include "swissknife_lease_curl.h"
#include "swissknife_lease_json.h"

#include "logging.h"

namespace {

bool CheckParams(const swissknife::CommandLease::Parameters& p) {
  if (p.action != "acquire" && p.action != "drop") {
    return false;
  }

  return true;
}

}  // namespace

namespace swissknife {

enum LeaseError {
  kLeaseSuccess = 0,
  kLeaseUnused = 1,
  kLeaseParamError = 2,
  kLeaseCurlInitError = 3,
  kLeaseFileOpenError = 4,
  kLeaseFileDeleteError = 5,
  kLeaseParseError = 6,
  kLeaseCurlReqError = 7,
};

CommandLease::~CommandLease() {}

ParameterList CommandLease::GetParams() const {
  ParameterList r;
  r.push_back(Parameter::Mandatory('u', "repo service url"));
  r.push_back(Parameter::Mandatory('a', "action (acquire or drop)"));
  r.push_back(Parameter::Mandatory('n', "user name"));
  r.push_back(Parameter::Mandatory('p', "lease path"));
  return r;
}

int CommandLease::Main(const ArgumentList& args) {
  Parameters params;

  params.repo_service_url = *(args.find('u')->second);
  params.action = *(args.find('a')->second);
  params.user_name = *(args.find('n')->second);

  const std::string lease_full_path = *(args.find('p')->second);
  size_t delimiter_position = lease_full_path.find_first_of('/');
  if (delimiter_position != std::string::npos) {
    params.lease_fqdn = lease_full_path.substr(0, delimiter_position);
    params.lease_subpath = lease_full_path.substr(delimiter_position);
  } else {
    params.lease_fqdn = lease_full_path;
    params.lease_subpath = "";
  }

  if (!CheckParams(params)) {
    return kLeaseParamError;
  }

  // Initialize curl
  if (curl_global_init(CURL_GLOBAL_ALL)) {
    return kLeaseCurlInitError;
  }

  LeaseError ret = kLeaseSuccess;
  if (params.action == "acquire") {
    CurlBuffer buffer;
    if (MakeAcquireRequest(params.user_name, params.lease_fqdn,
                           params.repo_service_url, &buffer)) {
      std::string session_token;
      if (buffer.data.size() > 0 && ParseAcquireReply(buffer, &session_token)) {
        // Save session token to
        // /var/spool/cvmfs/<REPO_NAME>/session_token_<SUBPATH>
        // TODO(radu): Is there a special way to access the scratch directory?
        std::string suffix(params.lease_subpath);
        std::replace(suffix.begin(), suffix.end(), '/', '_');
        const std::string token_file_name = "/var/spool/cvmfs/" +
                                            params.lease_fqdn +
                                            "/session_token_" + suffix;
        std::ofstream token_file(token_file_name.c_str(), std::ios_base::out);
        if (token_file.is_open()) {
          token_file << session_token;
        } else {
          LogCvmfs(kLogCvmfs, kLogStderr, "Error opening file: %s",
                   std::strerror(errno));
          ret = kLeaseFileOpenError;
        }
      } else {
        ret = kLeaseParseError;
      }
    } else {
      ret = kLeaseCurlReqError;
    }
  } else if (params.action == "drop") {
    // Try to read session token from repository scratch directory
    std::string session_token;
    std::string suffix(params.lease_subpath);
    std::replace(suffix.begin(), suffix.end(), '/', '_');
    std::string token_file_name =
        "/var/spool/cvmfs/" + params.lease_fqdn + "/session_token_" + suffix;
    std::ifstream token_file(token_file_name.c_str(), std::ios_base::in);
    bool success = false;
    if (token_file.is_open()) {
      std::stringstream sstr;
      sstr << token_file.rdbuf();
      session_token = sstr.str();
      LogCvmfs(kLogCvmfs, kLogStderr, "Read session token from file: %s",
               session_token.c_str());

      CurlBuffer buffer;
      if (MakeDeleteRequest(session_token, params.repo_service_url, &buffer)) {
        if (buffer.data.size() > 0 && ParseDropReply(buffer)) {
          success = true;
        } else {
          LogCvmfs(kLogCvmfs, kLogStderr, "Could not drop active lease");
          ret = kLeaseParseError;
        }
      } else {
        LogCvmfs(kLogCvmfs, kLogStderr, "Error making DELETE request");
        ret = kLeaseCurlReqError;
      }
    } else {
      LogCvmfs(kLogCvmfs, kLogStderr, "Error reading session token from file");
      ret = kLeaseFileOpenError;
    }

    token_file.close();
    if (success && std::remove(token_file_name.c_str())) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Error deleting session token file");
      ret = kLeaseFileDeleteError;
    }
  }

  return ret;
}

}  // namespace swissknife
