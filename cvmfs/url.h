/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_URL_H_
#define CVMFS_URL_H_

#include <string>

class Url {
 public:
  /**
   * Parse a URL string and build a Url object
   *
   * A URL string is parsed and a Url object is constructed with the extracted
   * address and port components.
   *
   * Returns NULL if the URL string is not well formed
   *
   * Ex: The url: <fqdn>:<port>/<path> will parse as address <fqdn>/<path>
   *     and port <port>
   */
  static const int kDefaultPort;
  static const char *kDefaultProtocol;

  /**
   * Parse an URL string
   *
   * Parse an URL string and extract the protocol, host, port, and path
   * components. The method can be called with protocol and port values that are
   * used when the input string doesn't contain these parts. The default
   * protocol is "http" and the default port is 80
   */
  static Url *Parse(const std::string &url,
                    const std::string &default_protocol = kDefaultProtocol,
                    int default_port = kDefaultPort);
  static bool ValidateHost(const std::string &host);

  ~Url();

  std::string protocol() const { return protocol_; }
  std::string address() const { return address_; }
  std::string host() const { return host_; }
  std::string path() const { return path_; }
  int port() const { return port_; }

 private:
  Url();
  Url(const std::string &protocol, const std::string &host,
      const std::string &path, int port);

  std::string protocol_;
  std::string host_;
  std::string path_;
  int port_;
  std::string address_;
};

#endif  // CVMFS_URL_H_
