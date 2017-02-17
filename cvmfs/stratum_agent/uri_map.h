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

  static WebRequest *Create(struct mg_request_info *req);

  Verb verb() const { return verb_; }
  std::string uri() const { return uri_; }

 private:
  WebRequest();

  Verb verb_;
  std::string uri_;
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
  void Register(const std::string &uri_spec, UriHandler *handler);
  UriHandler *Route(const std::string &uri);

 private:
  struct Match {
    Match(const Pathspec &s, UriHandler *h) : uri_spec(s), handler(h) { }
    Pathspec uri_spec;
    UriHandler *handler;
  };

  std::vector<Match> rules_;
};

#endif  // CVMFS_STRATUM_AGENT_URI_MAP_H_
