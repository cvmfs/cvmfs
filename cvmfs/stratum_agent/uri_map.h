/**
 * This file is part of the CernVM File System
 */

#ifndef CVMFS_STRATUM_AGENT_URI_MAP_H_
#define CVMFS_STRATUM_AGENT_URI_MAP_H_

#include <stdint.h>

#include <string>
#include <vector>

#include "mongoose.h"
#include "pathspec/pathspec.h"


/**
 * Captures the request path and the HTTP request method of an HTTP request,
 * which is enough information to route the request to a handler.
 */
class WebRequest {
 public:
  enum Verb {
    kGet,
    kPut,
    kPost,
    kDelete,
    kUnknown,
  };

  explicit WebRequest(const struct mg_request_info *req);
  WebRequest(const std::string &uri, Verb verb) : verb_(verb), uri_(uri) { }

  Verb verb() const { return verb_; }
  std::string uri() const { return uri_; }

 private:
  WebRequest() : verb_(kUnknown) { }

  Verb verb_;
  std::string uri_;
};


class WebReply {
 public:
  enum Code {
    k200,
    k400,
    k404,
    k405
  };

  static void Send(Code code, const std::string &msg,
                   struct mg_connection *conn);
};


/**
 * Abstract base class for a request handler.  Requests are identified by a
 * 64bit id.  Every request translates into one OnRequest call.
 */
class UriHandler {
 public:
  virtual void OnRequest(const struct mg_request_info *req_info,
                         struct mg_connection *conn) = 0;
};


/**
 * Registeres handlers for URI path specifications and returns handler for
 * concrete requests.  Matching of requests is done in the order of
 * specification registration.
 */
class UriMap {
 public:
  void Register(const WebRequest &request, UriHandler *handler);
  UriHandler *Route(const WebRequest &request);
  bool IsKnownUri(const std::string &uri);

 private:
  struct Match {
    Match(const Pathspec &p, const WebRequest::Verb v, UriHandler *h)
      : uri_spec(p), verb(v), handler(h) { }
    Pathspec uri_spec;
    WebRequest::Verb verb;
    UriHandler *handler;
  };

  std::vector<Match> rules_;
};

#endif  // CVMFS_STRATUM_AGENT_URI_MAP_H_
