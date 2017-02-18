/**
 * This file is part of the CernVM File System
 */

#include "cvmfs_config.h"
#include "uri_map.h"

#include <cassert>

using namespace std;  // NOLINT

WebRequest::WebRequest(const struct mg_request_info *req) {
  string method = req->request_method;
  if (method == "GET")
    verb_ = WebRequest::kGet;
  else if (method == "PUT")
    verb_ = WebRequest::kPut;
  else if (method == "POST")
    verb_ = WebRequest::kPost;
  else if (method == "DELETE")
    verb_ = WebRequest::kDelete;
  else
    verb_ = WebRequest::kUnknown;
  uri_ = req->uri;
}


//------------------------------------------------------------------------------


void WebReply::Send(Code code, const string &msg, struct mg_connection *conn) {
  string header;
  switch (code) {
    case k200: header = "HTTP/1.1 200 OK\r\n"; break;
    case k404: header = "HTTP/1.1 404 Not Found\r\n"; break;
    case k405: header = "HTTP/1.1 405 Method Not Allowed\r\n"; break;
    default: assert(false);
  }
  mg_printf(conn,
            "%s"
            "Content-Type: text/plain\r\n"
            "Content-Length: %lu\r\n"
            "\r\n"
            "%s", header.c_str(), msg.length(), msg.c_str());
}


//------------------------------------------------------------------------------


void UriMap::Register(const WebRequest &request, UriHandler *handler) {
  Pathspec path_spec(request.uri());
  assert(path_spec.IsValid() && path_spec.IsAbsolute());
  rules_.push_back(Match(path_spec, request.verb(), handler));
}


UriHandler *UriMap::Route(const WebRequest &request) {
  for (unsigned i = 0; i < rules_.size(); ++i) {
    if ( (rules_[i].uri_spec.IsPrefixMatching(request.uri())) &&
         (rules_[i].verb == request.verb()) )
    {
      return rules_[i].handler;
    }
  }
  return NULL;
}


bool UriMap::IsKnownUri(const std::string &uri) {
  for (unsigned i = 0; i < rules_.size(); ++i) {
    if (rules_[i].uri_spec.IsPrefixMatching(uri))
      return true;
  }
  return false;
}
