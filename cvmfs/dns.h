/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_DNS_H_
#define CVMFS_DNS_H_

#include <stdint.h>

#include <ctime>
#include <set>
#include <string>
#include <vector>

#include "atomic.h"
#include "util.h"

namespace dns {

/**
 * Possible errors when trying to resolve a host name.
 */
enum Failures {
  kFailOk = 0,
  kFailInvalidResolvers,
  kFailTimeout,
  kFailInvalidHost,
  kFailUnknownHost,
  kFailNoAddress,
  kFailNotYetResolved,
  kFailOther,
};


inline const char *Code2Ascii(const Failures error) {
  const int kNumElems = 8;
  if (error >= kNumElems)
    return "no text available (internal error)";

  const char *texts[kNumElems];
  texts[0] = "OK";
  texts[1] = "invalid resolver addresses";
  texts[2] = "DNS query timeout";
  texts[3] = "invalid host name to resolve";
  texts[4] = "unknown host name";
  texts[5] = "no IP address for host";
  texts[6] = "internal error, not yet resolved";
  texts[7] = "unknown name resolving error";

  return texts[error];
}

/**
 * Stores the resolved IPv4 and IPv6 addresses of a host name including their
 * time to live.  Data in these objects are immutable.  Once the TTL has
 * expired, they become invalid and a new Host object should be fetched from
 * a resolver.
 */
class Host {
  FRIEND_TEST(T_Dns, Host);
  FRIEND_TEST(T_Dns, HostEquivalent);
  FRIEND_TEST(T_Dns, HostValid);
 public:
  bool IsEquivalent(const Host &other) const;
  bool IsValid() const;

  Host(const Host &other);
  Host &operator= (const Host &other);

  int64_t id() const { return id_; };
  bool hasIpv6() const { return !ipv6_addresses_.empty(); }
  std::set<std::string> ipv4_addresses() const { return ipv4_addresses_; }
  std::set<std::string> ipv6_addresses() const { return ipv6_addresses_; }
  std::string name() const { return name_; };
 private:
  void CopyFrom(const Host &other);

  /**
   * Only the Resolver constructs Host objects.
   */
  Host();

   /**
    * Counter that is increased with every creation of a host object.  Allows to
    * distinguish two Host objects with the same host name.  E.g. when the proxy
    * list in Download.cc reads "http:/A:3128|http:/A:3128".
    */
  static atomic_int64 global_id_;

  /**
   * When the name resolution becomes outdated, in UTC seconds since UNIX epoch.
   * Once the deadline is passed, IsValid returns false.
   */
  time_t deadline_;

  /**
   * The unique id of this instance of Host.
   */
  int64_t id_;

  /**
   * ASCII representation of IPv4 addresses (a.b.c.d), so that they can be
   * readily used by curl.
   */
  std::set<std::string> ipv4_addresses_;

  /**
   * ASCII representation of IPv6 addresses in the form "[a:b:c:d:e:f:g:h]",
   * so that they can be readily used by curl.
   */
  std::set<std::string> ipv6_addresses_;

  /**
   * The fully qualified host name.
   */
  std::string name_;

  /**
   * Status of the name resolution
   */
  Failures status_;
};


/**
 * Abstract interface of a name resolver.  Returns a Host object upon successful
 * name resolution.  Can be configured with DNS servers, with a timeout, and
 * whether to use IPv4 only or not.
 */
class Resolver : SingleCopy {
 public:
  Resolver(const bool ipv4_only, const unsigned timeout);
  virtual ~Resolver() = 0;

  virtual void SetResolvers(const std::vector<std::string> &new_resolvers) = 0;
  virtual void SetSystemResolvers() = 0;
  virtual Host Resolve(const std::string &name) = 0;

  bool ipv4_only() const { return ipv4_only_; }
  unsigned timeout() const { return timeout_; }
 private:
  /**
   * Do not try to get AAAA records if true.
   */
  bool ipv4_only_;

  /**
   * Timeout in seconds for DNS queries.  Zero means no timeout.
   */
  unsigned timeout_;
};


/**
 * Implementation of the Resolver class using the c-ares library.
 */
class CaresResolver : public Resolver {
 public:
  CaresResolver(const bool ipv4_only, const unsigned timeout);
  ~CaresResolver();

  void SetResolvers(const std::vector<std::string> &new_resolvers);
  void SetSystemResolvers();
  Host Resolve(const std::string &name);
};

}  // namespace dns

#endif  // CVMFS_DNS_H_
