/**
 * This file is part of the CernVM File System.
 */

#include "url.h"

#include <algorithm>

#include "util/string.h"

const int Url::kDefaultPort = 80;
const char *Url::kDefaultProtocol = "http";

Url *Url::Parse(const std::string &url, const std::string &default_protocol,
                int default_port) {
  if (url.empty()) {
    return NULL;
  }

  size_t cursor = 0;

  // Is there a protocol prefix?
  std::string protocol = default_protocol;
  const size_t sep_pos = url.find("://");
  if (sep_pos != std::string::npos) {
    protocol = url.substr(0, sep_pos);
    cursor = sep_pos + 3;
  }

  std::string host;
  std::string path;
  uint64_t port = default_port;

  // Try to find another ":", preceding a port number
  const size_t col_pos = url.find(":", cursor);
  if (col_pos != std::string::npos) {
    // Port number was given
    host = url.substr(cursor, col_pos - cursor);
    cursor = col_pos + 1;

    const size_t slash_pos = url.find("/", cursor);
    if (slash_pos == 0) {
      return NULL;
    }

    // Check that part between ":" and "/" or the end of the string is an
    // unsigned number
    const size_t port_end = slash_pos == std::string::npos ? std::string::npos
                                                           : slash_pos - cursor;
    if (!String2Uint64Parse(url.substr(cursor, port_end), &port)) {
      return NULL;
    }

    if (slash_pos != std::string::npos) {
      path = url.substr(slash_pos);
    }
  } else {
    // No port number was given
    const size_t slash_pos = url.find("/", cursor);
    if (slash_pos != std::string::npos) {
      host = url.substr(cursor, slash_pos - cursor);
      path = url.substr(slash_pos);
    } else {
      host = url.substr(cursor);
    }
  }

  // At the moment, host name validation is very basic
  if (!ValidateHost(host)) {
    return NULL;
  }

  return new Url(protocol, host, path, port);
}

bool Url::ValidateHost(const std::string &host) {
  if (host.empty()) {
    return false;
  }

  // Host must not be just a number
  uint64_t test;
  if (String2Uint64Parse(host, &test)) {
    return false;
  }

  return true;
}

Url::Url() : protocol_(), host_(), path_(), port_(), address_() { }

Url::Url(const std::string &protocol, const std::string &host,
         const std::string &path, int port)
    : protocol_(protocol), host_(host), path_(path), port_(port), address_() {
  if (port_ != kDefaultPort) {
    address_ = protocol + "://" + host + ":" + StringifyInt(port_) + path;
  } else {
    address_ = protocol + "://" + host + path;
  }
}
Url::~Url() { }
