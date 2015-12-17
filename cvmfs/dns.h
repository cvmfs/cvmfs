/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_DNS_H_
#define CVMFS_DNS_H_

#include <stdint.h>

#include <cstdio>
#include <ctime>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "atomic.h"
#include "duplex_cares.h"
#include "gtest/gtest_prod.h"
#include "prng.h"
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
  kFailUnknownHost,   ///< Resolver returned a negative reply
  kFailMalformed,
  kFailNoAddress,     ///< Resolver returned a positive reply but without IPs
  kFailNotYetResolved,
  kFailOther,

  kFailNumEntries
};


/**
 * Steers IP protocol selection.
 */
enum IpPreference {
  // use system default, currently unused and mapped to IPv4
  kIpPreferSystem = 0,
  kIpPreferV4,
  kIpPreferV6,
};

inline const char *Code2Ascii(const Failures error) {
  const char *texts[kFailNumEntries + 1];
  texts[0] = "OK";
  texts[1] = "invalid resolver addresses";
  texts[2] = "DNS query timeout";
  texts[3] = "invalid host name to resolve";
  texts[4] = "unknown host name";
  texts[5] = "malformed DNS request";
  texts[6] = "no IP address for host";
  texts[7] = "internal error, not yet resolved";
  texts[8] = "unknown name resolving error";
  texts[9] = "no text";
  return texts[error];
}


std::string ExtractHost(const std::string &url);
std::string ExtractPort(const std::string &url);
std::string RewriteUrl(const std::string &url, const std::string &ip);
std::string StripIp(const std::string &decorated_ip);


/**
 * Stores the resolved IPv4 and IPv6 addresses of a host name including their
 * time to live.  Data in these objects are immutable.  They are created by a
 * a Resolver.  Once the TTL has expired, they become invalid and a new Host
 * object should be fetched from a resolver.
 *
 * A host object can be copied into a new object with an extended deadline.
 * This is useful if an attempt to resolve a name fails where it previously
 * succeeded.  In this case, the extended deadline can be used to delay another
 * name resolution attempt for some grace period.
 *
 * For successful name resolution, the name is the fully qualified domain name,
 * even if a short name was given to the resolver.
 */
class Host {
  FRIEND_TEST(T_Dns, HostEquivalent);
  FRIEND_TEST(T_Dns, HostExpired);
  FRIEND_TEST(T_Dns, HostValid);
  FRIEND_TEST(T_Dns, HostExtendDeadline);
  FRIEND_TEST(T_Dns, HostBestAddresses);
  friend class Resolver;

 public:
  static Host ExtendDeadline(const Host &original, unsigned seconds_from_now);
  Host();
  Host(const Host &other);
  Host &operator= (const Host &other);
  bool IsEquivalent(const Host &other) const;
  bool IsExpired() const;
  bool IsValid() const;

  time_t deadline() const { return deadline_; }
  int64_t id() const { return id_; }
  bool HasIpv4() const { return !ipv4_addresses_.empty(); }
  bool HasIpv6() const { return !ipv6_addresses_.empty(); }
  const std::set<std::string> &ipv4_addresses() const {
    return ipv4_addresses_;
  }
  const std::set<std::string> &ipv6_addresses() const {
    return ipv6_addresses_;
  }
  const std::set<std::string> &ViewBestAddresses(IpPreference preference) const;
  const std::string &name() const { return name_; }
  Failures status() const { return status_; }

 private:
  void CopyFrom(const Host &other);

   /**
    * Counter that is increased with every creation of a host object.  Allows to
    * distinguish two Host objects with the same host name.  E.g. when the proxy
    * list in Download.cc reads "http://A:3128|http://A:3128".
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
   * The host name either fully qualified or within the search domain.
   */
  std::string name_;

  /**
   * Error code of the name resolution that led to this object.
   */
  Failures status_;
};


/**
 * Abstract interface of a name resolver.  Returns a Host object upon successful
 * name resolution.  Also provides a vector interface to resolve multiple names
 * in parallel.  Can be configured with DNS servers, with a timeout, and whether
 * to use IPv4 only or not.
 */
class Resolver : SingleCopy {
 public:
  /**
   * Enlarge very small TTLs to 1 minute.
   */
  static const unsigned kMinTtl = 60;

  /**
   * Cut off very large TTLs to 1 day.
   */
  static const unsigned kMaxTtl = 84600;

  Resolver(const bool ipv4_only,
           const unsigned retries,
           const unsigned timeout_ms);
  virtual ~Resolver() { }

  /**
   * A list of new resolvers in the form <IP address>[:port].
   */
  virtual bool SetResolvers(const std::vector<std::string> &resolvers) = 0;
  virtual bool SetSearchDomains(const std::vector<std::string> &domains) = 0;
  virtual void SetSystemResolvers() = 0;
  virtual void SetSystemSearchDomains() = 0;
  Host Resolve(const std::string &name);
  void ResolveMany(const std::vector<std::string> &names,
                   std::vector<Host> *hosts);

  const std::vector<std::string> &domains() const { return domains_; }
  bool ipv4_only() const { return ipv4_only_; }
  const std::vector<std::string> &resolvers() const { return resolvers_; }
  unsigned retries() const { return retries_; }
  unsigned timeout_ms() const { return timeout_ms_; }
  void set_throttle(const unsigned throttle) { throttle_ = throttle; }
  unsigned throttle() const { return throttle_; }

 protected:
  /**
   * Takes host names and returns the resolved lists of A and AAAA records in
   * the same order.  To keep it simple, returns only a single TTL per host,
   * the lower value of both record types A/AAAA.  The output vectors have
   * the same size as the input vector names.  Names that are handled by the
   * base class are marked with skip[i] set to true. The input names are
   * completed to fully qualified domain names.
   */
  virtual void DoResolve(const std::vector<std::string> &names,
                         const std::vector<bool> &skip,
                         std::vector<std::vector<std::string> > *ipv4_addresses,
                         std::vector<std::vector<std::string> > *ipv6_addresses,
                         std::vector<Failures> *failures,
                         std::vector<unsigned> *ttls,
                         std::vector<std::string> *fqdns) = 0;
  bool IsIpv4Address(const std::string &address);
  bool IsIpv6Address(const std::string &address);

  /**
   * Currently active search domain list
   */
  std::vector<std::string> domains_;

  /**
   * Do not try to get AAAA records if true.
   */
  bool ipv4_only_;

  /**
   * Currently used resolver list in the form <ip address>:<port>
   */
  std::vector<std::string> resolvers_;

  /**
   * 1 + retries_ attempts to unresponsive servers, each attempt bounded by
   * timeout_ms_
   */
  unsigned retries_;

  /**
   * Timeout in milliseconds for DNS queries.  Zero means no timeout.
   */
  unsigned timeout_ms_;

  /**
   * Limit number of resolved IP addresses.  If throttle_ is 0 it has no effect.
   * Otherwise, if more than thottle_ IPs are registered for a host, only
   * throttle_ randomly picked IPs are returned.
   */
  unsigned throttle_;

  /**
   * Required for picking IP addresses in throttle_
   */
  Prng prng_;
};


/**
 * Implementation of the Resolver interface using the c-ares library.
 */
class CaresResolver : public Resolver {
  friend class NormalResolver;
 public:
  /**
   * More IP addresses for a single name will be ignored.
   */
  static const unsigned kMaxAddresses = 16;

  static CaresResolver *Create(const bool ipv4_only,
                               const unsigned retries,
                               const unsigned timeout_ms);
  virtual ~CaresResolver();

  virtual bool SetResolvers(const std::vector<std::string> &resolvers);
  virtual bool SetSearchDomains(const std::vector<std::string> &domains);
  virtual void SetSystemResolvers();
  virtual void SetSystemSearchDomains();

 protected:
  CaresResolver(const bool ipv4_only,
                const unsigned retries,
                const unsigned timeout_ms);
  virtual void DoResolve(const std::vector<std::string> &names,
                         const std::vector<bool> &skip,
                         std::vector<std::vector<std::string> > *ipv4_addresses,
                         std::vector<std::vector<std::string> > *ipv6_addresses,
                         std::vector<Failures> *failures,
                         std::vector<unsigned> *ttls,
                         std::vector<std::string> *fqdns);

 private:
  void WaitOnCares();
  ares_channel *channel_;
  std::vector<std::string> system_resolvers_;
  std::vector<std::string> system_domains_;
};


/**
 * Resolves against static name information like in /etc/hosts.  Setting
 * resolver addresses is a no-op for this resolver.  Search domains are not
 * automatically found but need to be set.  Not the most efficient
 * implementation but in the context of cvmfs should be called at most every 5
 * minutes.
 */
class HostfileResolver : public Resolver {
  friend class NormalResolver;
 public:
  static HostfileResolver *Create(const std::string &path, bool ipv4_only);
  virtual ~HostfileResolver();

  virtual bool SetResolvers(const std::vector<std::string> &resolvers) {
    return true;
  }
  virtual bool SetSearchDomains(const std::vector<std::string> &domains);
  virtual void SetSystemResolvers() { }
  virtual void SetSystemSearchDomains();

 protected:
  explicit HostfileResolver(const bool ipv4_only);
  virtual void DoResolve(const std::vector<std::string> &names,
                         const std::vector<bool> &skip,
                         std::vector<std::vector<std::string> > *ipv4_addresses,
                         std::vector<std::vector<std::string> > *ipv6_addresses,
                         std::vector<Failures> *failures,
                         std::vector<unsigned> *ttls,
                         std::vector<std::string> *fqdns);

 private:
  struct HostEntry {
    std::vector<std::string> ipv4_addresses;
    std::vector<std::string> ipv6_addresses;
  };
  void ParseHostFile();

  /**
   * Host names to lists of IPv4 and IPv6 addresses.  Reverse lookup in the
   * hosts file.
   */
  std::map<std::string, HostEntry> host_map_;

  /**
   * Open the file descriptor when the resolver is constructed and only release
   * on destruction.  Thereby we can be relatively sure to not see I/O errors
   * once constructed.
   */
  FILE *fhosts_;
};


/**
 * The normal resolver combines Hostfile and C-ares resolver.  First looks up
 * host names in the host file.  All non-matches are looked up by c-ares.
 */
class NormalResolver : public Resolver {
  FRIEND_TEST(T_Dns, NormalResolverConstruct);

 public:
  static NormalResolver *Create(const bool ipv4_only,
                                const unsigned retries,
                                const unsigned timeout_ms);
  virtual bool SetResolvers(const std::vector<std::string> &resolvers);
  virtual bool SetSearchDomains(const std::vector<std::string> &domains);
  virtual void SetSystemResolvers();
  virtual void SetSystemSearchDomains();
  virtual ~NormalResolver();

 protected:
  virtual void DoResolve(const std::vector<std::string> &names,
                         const std::vector<bool> &skip,
                         std::vector<std::vector<std::string> > *ipv4_addresses,
                         std::vector<std::vector<std::string> > *ipv6_addresses,
                         std::vector<Failures> *failures,
                         std::vector<unsigned> *ttls,
                         std::vector<std::string> *fqdns);
  NormalResolver();

 private:
  CaresResolver *cares_resolver_;
  HostfileResolver *hostfile_resolver_;
};

}  // namespace dns

#endif  // CVMFS_DNS_H_
