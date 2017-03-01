/**
 * This file is part of the CernVM File System.
 */

#include "session_context.h"

namespace upload {

SessionContext::~SessionContext() {}

bool SessionContext::Initialize(const std::string& api_url,
                                const std::string& session_token,
                                bool drop_lease) {
  // Add checks on api_url and session_token ?
  api_url_ = api_url;
  session_token_ = session_token;
  drop_lease_ = drop_lease;

  return true;
}

bool SessionContext::FinalizeSession() { return true; }

}  // namespace upload
