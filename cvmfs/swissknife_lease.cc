/**
 * This file is part of the CernVM File System.
 */

#include "swissknife_lease.h"

#include <algorithm>
#include <vector>

#include "gateway_util.h"
#include "swissknife_lease_curl.h"
#include "swissknife_lease_json.h"
#include "util/logging.h"
#include "util/posix.h"
#include "util/string.h"

namespace {

bool CheckParams(const swissknife::CommandLease::Parameters &p) {
  if (p.action != "acquire" && p.action != "drop") {
    return false;
  }

  return true;
}

}  // namespace

namespace swissknife {

enum LeaseError {
  kLeaseSuccess,
  kLeaseBusy,
  kLeaseFailure,
  kLeaseParamError,
  kLeaseKeyParseError,
  kLeaseCurlInitError,
  kLeaseFileOpenError,
  kLeaseCurlReqError,
};

CommandLease::~CommandLease() { }

ParameterList CommandLease::GetParams() const {
  ParameterList r;
  r.push_back(Parameter::Mandatory('u', "repo service url"));
  r.push_back(Parameter::Mandatory('a', "action (acquire or drop)"));
  r.push_back(Parameter::Mandatory('k', "key file"));
  r.push_back(Parameter::Mandatory('p', "lease path"));
  return r;
}

int CommandLease::Main(const ArgumentList &args) {
  Parameters params;

  params.repo_service_url = *(args.find('u')->second);
  params.action = *(args.find('a')->second);
  params.key_file = *(args.find('k')->second);

  params.lease_path = *(args.find('p')->second);
  std::vector<std::string> tokens = SplitString(params.lease_path, '/');
  const std::string lease_fqdn = tokens.front();

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
    return kLeaseKeyParseError;
  }

  LeaseError ret = kLeaseSuccess;
  if (params.action == "acquire") {
    CurlBuffer buffer;
    if (MakeAcquireRequest(key_id, secret, params.lease_path,
                           params.repo_service_url, &buffer)) {
      std::string session_token;
      LeaseReply rep = ParseAcquireReply(buffer, &session_token);
      switch (rep) {
        case kLeaseReplySuccess: {
          const std::string token_file_name = "/var/spool/cvmfs/" + lease_fqdn
                                              + "/session_token";

          if (!SafeWriteToFile(session_token, token_file_name, 0600)) {
            LogCvmfs(kLogCvmfs, kLogStderr, "Error opening file: %s",
                     std::strerror(errno));
            ret = kLeaseFileOpenError;
          }
        } break;
        case kLeaseReplyBusy:
          return kLeaseBusy;
          break;
        case kLeaseReplyFailure:
        default:
          return kLeaseFailure;
      }
    } else {
      ret = kLeaseCurlReqError;
    }
  } else if (params.action == "drop") {
    // Try to read session token from repository scratch directory
    std::string session_token;
    std::string token_file_name = "/var/spool/cvmfs/" + lease_fqdn
                                  + "/session_token";
    FILE *token_file = std::fopen(token_file_name.c_str(), "r");
    if (token_file) {
      GetLineFile(token_file, &session_token);
      LogCvmfs(kLogCvmfs, kLogDebug, "Read session token from file: %s",
               session_token.c_str());

      CurlBuffer buffer;
      if (MakeEndRequest("DELETE", key_id, secret, session_token,
                         params.repo_service_url, "", &buffer)) {
        if (kLeaseReplySuccess == ParseDropReply(buffer)) {
          std::fclose(token_file);
          if (unlink(token_file_name.c_str())) {
            LogCvmfs(kLogCvmfs, kLogStderr,
                     "Warning - Could not delete session token file.");
          }
          return kLeaseSuccess;
        } else {
          LogCvmfs(kLogCvmfs, kLogStderr, "Could not drop active lease");
          ret = kLeaseFailure;
        }
      } else {
        LogCvmfs(kLogCvmfs, kLogStderr, "Error making DELETE request");
        ret = kLeaseCurlReqError;
      }

      std::fclose(token_file);
    } else {
      LogCvmfs(kLogCvmfs, kLogStderr, "Error reading session token from file");
      ret = kLeaseFileOpenError;
    }
  }

  return ret;
}

}  // namespace swissknife
