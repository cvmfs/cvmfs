#include "swissknife_lease.h"

#include "curl/curl.h"

static bool CheckParams(const swissknife::CommandLease::Parameters& p) {
  if (p.action != "acquire" && p.action != "drop") {
    return false;
  }

  return true;
}

namespace swissknife {

CommandLease::~CommandLease() {
}

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
  params.lease_path = *(args.find('p')->second);

  if (!CheckParams(params)) return 2;

  // Initialize curl
  if (curl_global_init(CURL_GLOBAL_ALL)) {
    return 1;
  }

  if (params.action == "acquire") {
    // Acquire lease from repo services
  } else if (params.action == "drop") {
    // Drop the current lease
  }

  return 0;
}

}
