#include "swissknife_lease.h"

#include "util/pointer.h"
#include "json_document.h"
#include "json.h"
#include "logging.h"

#include "curl/curl.h"

namespace {

bool CheckParams(const swissknife::CommandLease::Parameters& p) {
  if (p.action != "acquire" && p.action != "drop") {
    return false;
  }

  return true;
}

/*size_t SendCB(void *buffer, size_t size, size_t nmemb, void *userp) {
}*/

struct CurlBuffer {
  std::string data;
};

size_t RecvCB(void *buffer, size_t size, size_t nmemb, void *userp) {
  CurlBuffer* my_buffer = (CurlBuffer*)userp;

  if (size * nmemb < 1) {
    return 0;
  }

  my_buffer->data = static_cast<char*>(buffer);

  return my_buffer->data.size();
}

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

  CURL* h_curl = curl_easy_init();
  CURLcode ret = static_cast<CURLcode>(0);
  if (h_curl) {
    if (params.action == "acquire") {
      // Prepare payload
      std::string payload = "{\"user\" : \"" + params.user_name + "\", \"path\" : \"" + params.lease_path + "\"}";

      // Make request to acquire lease from repo services
      curl_easy_setopt(h_curl, CURLOPT_URL, (params.repo_service_url + "/api/leases").c_str());
      curl_easy_setopt(h_curl, CURLOPT_NOPROGRESS, 1L);
      curl_easy_setopt(h_curl, CURLOPT_POSTFIELDS, payload.c_str());
      curl_easy_setopt(h_curl,
                       CURLOPT_POSTFIELDSIZE_LARGE,
                       static_cast<curl_off_t>(payload.length()));
      curl_easy_setopt(h_curl, CURLOPT_USERAGENT, "curl/7.47.0");
      curl_easy_setopt(h_curl, CURLOPT_MAXREDIRS, 50L);
      curl_easy_setopt(h_curl, CURLOPT_CUSTOMREQUEST, "POST");
      curl_easy_setopt(h_curl, CURLOPT_TCP_KEEPALIVE, 1L);

      CurlBuffer buffer;

      curl_easy_setopt(h_curl, CURLOPT_WRITEFUNCTION, RecvCB);
      curl_easy_setopt(h_curl, CURLOPT_WRITEDATA, &buffer);

      ret = curl_easy_perform(h_curl);

      curl_easy_cleanup(h_curl);
      h_curl = NULL;

      //Parse reply
      UniquePtr<JsonDocument> reply(JsonDocument::Create(buffer.data));
      if (reply->IsValid()) {
        JSON* result = JsonDocument::SearchInObject(reply->root(), "status", JSON_STRING);
        if (result != NULL) {
          std::string status = result->string_value;
          if (status == "ok") {
            LogCvmfs(kLogCvmfs, kLogStderr, "Status: ok");
            JSON* token = JsonDocument::SearchInObject(reply->root(), "session_token", JSON_STRING);
            if (token != NULL) {
              LogCvmfs(kLogCvmfs, kLogStderr, "Session token: %s", token->string_value);
              std::string session_token = token->string_value;
              // Save session token to /var/spool/cvmfs/<REPO_NAME>
              // TODO: Is there a special way to access the scratch directory?
              std::string token_file_name = "/var/spool/cvmfs/" + params.lease_path + "/session_token";
              FILE* token_file = std::fopen(token_file_name.c_str(), "w");
              if (token_file != NULL) {
                std::fprintf(token_file, "%s", session_token.c_str());
                std::fclose(token_file);
              } else {
                LogCvmfs(kLogCvmfs, kLogStderr, "Error opening file: %s", std::strerror(errno));
              }
            }
          } else if (status == "path_busy") {
            JSON* time_remaining = JsonDocument::SearchInObject(reply->root(), "time_remaining", JSON_INT);
            if (time_remaining != NULL) {
              LogCvmfs(kLogCvmfs, kLogStderr, "Path busy. Time remaining = %d", time_remaining->int_value);
            }
          } else if (status == "error") {
            JSON* reason = JsonDocument::SearchInObject(reply->root(), "reason", JSON_STRING);
            if (reason != NULL) {
              LogCvmfs(kLogCvmfs, kLogStderr, "Error: %s", reason->string_value);
            }
          } else {
            LogCvmfs(kLogCvmfs, kLogStderr, "Unknown reply. Status: %s", status.c_str());
          }
        }
      }

    } else if (params.action == "drop") {
      // Drop the current lease
    }
  } else {
    return 1;
  }

  return ret;
}

}
