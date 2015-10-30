/**
 * This file is part of the CernVM File System
 */

#ifndef CVMFS_WEBAPI_URI_MAP_H_
#define CVMFS_WEBAPI_URI_MAP_H_

#include <stdint.h>

#include <string>
#include <vector>

#include "../pathspec/pathspec.h"

class FastCgi;

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

  static WebRequest *CreateFromCgiHeaders(FastCgi *fcgi);

  Verb verb() const { return verb_; }
  std::string uri() const { return uri_; }

 private:
  WebRequest();

  Verb verb_;
  std::string uri_;
};


/**
 * Abstract base class for a request handler.  Requests are identified by a
 * 64bit id.  Every request translates into one OnRequest call and one or
 * multiple OnData calls.
 */
class UriHandler {
 public:
  explicit UriHandler(FastCgi *fcgi) : fcgi_(fcgi) { }
  virtual ~UriHandler() { }
  virtual void OnRequest(const uint64_t id, const WebRequest &request) = 0;
  virtual void OnData(const uint64_t id,
                      unsigned char **buf,
                      unsigned *length) = 0;

 protected:
   FastCgi *fcgi_;
};


/**
 * Registeres handlers for URI path specifications and returns handler for
 * concrete requests.  Matching of requests is done in the order of
 * specification registration.
 */
class UriMap {
 public:
  bool Register(const std::string &uri_spec, UriHandler *handler);
  UriHandler *Route(const std::string &uri);

 private:
  struct Match {
    Match(const Pathspec &s, UriHandler *h) : uri_spec(s), handler(h) { }
    Pathspec uri_spec;
    UriHandler *handler;
  };

  std::vector<Match> rules_;
};

#endif  // CVMFS_WEBAPI_URI_MAP_H_
