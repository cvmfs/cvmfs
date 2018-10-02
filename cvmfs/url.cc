/**
 * This file is part of the CernVM File System.
 */

#include "url.h"

#include <algorithm>

#include "util/string.h"

const int Url::kDefaultPort = 80;
const std::string Url::kDefaultProtocol = "http";

Url* Url::Parse(const std::string& url, const std::string& default_protocol,
                int default_port) {
  if (url.empty()) {
    return NULL;
  }

  size_t cursor = 0;

  // Is there a protocol prefix?
  std::string protocol = default_protocol;
  size_t sep_pos = url.find("://");
  if (sep_pos != std::string::npos) {
    protocol = url.substr(0, sep_pos);
    cursor = sep_pos + 3;
  }

  std::string host;
  std::string path;
  uint64_t port = default_port;

  // Try to find another ":", preceding a port number
  size_t col_pos = url.find(":", cursor);
  if (col_pos != std::string::npos) {
    // Port number was given
    host = url.substr(cursor, col_pos - cursor);
    cursor = col_pos + 1;

    size_t slash_pos = url.find("/", cursor);
    if (slash_pos == 0) {
      return NULL;
    }

    // Check that part between ":" and "/" or the end of the string is an
    // unsigned number
    size_t port_end =
        slash_pos == std::string::npos ? std::string::npos : slash_pos - cursor;
    if (!String2Uint64Parse(url.substr(cursor, port_end), &port)) {
      return NULL;
    }

    if (slash_pos != std::string::npos) {
      path = url.substr(slash_pos);
    }
  } else {
    // No port number was given
    size_t slash_pos = url.find("/", cursor);
    if (slash_pos != std::string::npos) {
      host = url.substr(cursor, slash_pos - cursor);
      path = url.substr(slash_pos);
    } else {
      host = url.substr(cursor);
    }
  }

  if (!ValidateHost(host)) {
    return NULL;
  }

  return new Url(protocol, host, path, port);
}

bool Url::ValidateHost(const std::string& host) {
  if (host.empty()) {
    return false;
  }

  // Host must not be just a number
  uint64_t test;
  if (String2Uint64Parse(host, &test)) {
    return false;
  }

  int num_dots = std::count(host.begin(), host.end(), '.');
  // If host does not contain exactly 3 dots, then parts must not be numbers;
  if (num_dots != 3) {
    size_t cursor = 0;
    while (cursor < host.size()) {
      size_t dot_pos = host.find('.', cursor);
      std::string part;
      if (dot_pos == std::string::npos) {
        part = host.substr(cursor);
      } else {
        part = host.substr(cursor, dot_pos - cursor);
      }
      if (String2Uint64Parse(part, &test)) {
        return false;
      }
      if (dot_pos == std::string::npos) {
        break;
      }
      cursor = dot_pos + 1;
    }
  }

  return true;
}

Url::Url() : protocol_(), host_(), path_(), address_(), port_() {}

Url::Url(const std::string& protocol, const std::string& host,
         const std::string& path, int port)
    : protocol_(protocol),
      host_(host),
      path_(path),
      address_(protocol + "://" + host + path),
      port_(port) {}
Url::~Url() {}
