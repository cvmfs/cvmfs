/**
 * This file is part of the CernVM File System.
 */

#include "swissknife_lease.h"

#include <algorithm>
#include <vector>

#include "gateway_util.h"
#include "logging.h"
#include "swissknife_lease_curl.h"
#include "swissknife_lease_json.h"
#include "util/posix.h"
#include "util/string.h"

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
  r.push_back(Parameter::Mandatory('k', "key file"));
  r.push_back(Parameter::Mandatory('p', "lease path"));
  return r;
}

int CommandLease::Main(const ArgumentList& args) {
  Parameters params;

  params.repo_service_url = *(args.find('u')->second);
  params.action = *(args.find('a')->second);
  params.key_file = *(args.find('k')->second);

  params.lease_path = *(args.find('p')->second);
  std::vector<std::string> tokens = SplitString(params.lease_path, '/');
  const std::string lease_fqdn = tokens.front();

  // Remove the fqdn from the list of tokens and join them with "_" to make the
  // token file suffix
  tokens.erase(tokens.begin());
  params.token_file_suffix = JoinStrings(tokens, "_");

  if (!CheckParams(params)) {
    return kLeaseParamError;
  }

  // Initialize curl
  if (curl_global_init(CURL_GLOBAL_ALL)) {
    return kLeaseCurlInitError;
  }

  std::string key_id;
  std::string secret;
  if (!gateway::ReadKeys(params.key_file, &key_id, &secret)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Error reading key file %s.",
             params.key_file.c_str());
    return 1;
  }

  LeaseError ret = kLeaseSuccess;
  if (params.action == "acquire") {
    CurlBuffer buffer;
    if (MakeAcquireRequest(key_id, secret, params.lease_path,
                           params.repo_service_url, &buffer)) {
      std::string session_token;
      if (buffer.data.size() > 0 && ParseAcquireReply(buffer, &session_token)) {
        // Save session token to
        // /var/spool/cvmfs/<REPO_NAME>/session_token_<SUBPATH>
        // TODO(radu): Is there a special way to access the scratch directory?
        const std::string token_file_name = "/var/spool/cvmfs/" + lease_fqdn +
                                            "/session_token_" +
                                            params.token_file_suffix;
        if (!SafeWriteToFile(session_token, token_file_name, 0600)) {
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
    std::string token_file_name = "/var/spool/cvmfs/" + lease_fqdn +
                                  "/session_token_" + params.token_file_suffix;
    FILE* token_file = std::fopen(token_file_name.c_str(), "r");
    if (token_file) {
      GetLineFile(token_file, &session_token);
      LogCvmfs(kLogCvmfs, kLogStderr, "Read session token from file: %s",
               session_token.c_str());

      CurlBuffer buffer;
      if (MakeEndRequest("DELETE", key_id, secret, session_token,
                         params.repo_service_url, "", &buffer)) {
        if (buffer.data.size() > 0 && ParseDropReply(buffer)) {
          std::fclose(token_file);
          if (unlink(token_file_name.c_str())) {
            LogCvmfs(kLogCvmfs, kLogStderr,
                     "Error deleting session token file");
            ret = kLeaseFileDeleteError;
          }
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
  }

  return ret;
}

}  // namespace swissknife
