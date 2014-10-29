/**
 * This file is part of the CernVM File System.
 */

#include "dns.h"

#include <cassert>

#include "duplex_cares.h"
#include "logging.h"
#include "sanitizer.h"
#include "util.h"

using namespace std;  // NOLINT

namespace dns {

atomic_int64 Host::global_id_ = 0;

void Host::CopyFrom(const Host &other) {
  deadline_ = other.deadline_;
  id_ = other.id_;
  ipv4_addresses_ = other.ipv4_addresses_;
  ipv6_addresses_ = other.ipv6_addresses_;
  name_ = other.name_;
  status_ = other.status_;
}


/**
 * All fields except the unique id_ are set by the resolver.  Host objects
 * can be copied around but only the resolve can create them.
 */
Host::Host()
  : deadline_(0)
  , id_(atomic_xadd64(&global_id_, 1))
  , status_(kFailNotYetResolved)
{
}


Host::Host(const Host &other) {
  CopyFrom(other);
}


Host &Host::operator= (const Host &other) {
  if (&other == this)
    return *this;
  CopyFrom(other);
  return *this;
}


/**
 * Compares the name and the resolved addresses independent of deadlines.  Used
 * to decide if the current proxy list needs to be changed after re-resolving
 * a host name.
 */
bool Host::IsEquivalent(const Host &other) const {
  return (status_ == kFailOk) && (other.status_ == kFailOk) &&
      (name_ == other.name_) &&
      (ipv4_addresses_ == other.ipv4_addresses_) &&
      (ipv6_addresses_ == other.ipv6_addresses_);
}


/**
 * A host object is valid after it has been successfully resolved and until the
 * DNS ttl expires.  Successful name resolution means that there is at least
 * one IP address.
 */
bool Host::IsValid() const {
  if (status_ != kFailOk)
    return false;

  assert(!ipv4_addresses_.empty() || !ipv6_addresses_.empty());

  time_t now = time(NULL);
  assert(now != static_cast<time_t>(-1));
  return deadline_ >= now;
}


//------------------------------------------------------------------------------


/**
 * Basic input validation to ensure that this could syntactically represent a
 * valid IPv4 address.
 */
bool Resolver::IsIpv4Address(const string &address) {
  // Are there any unexpected characters?
  sanitizer::InputSanitizer sanitizer("09 .");
  if (!sanitizer.IsValid(address))
    return false;

  // 4 octets in the range 0-255?
  vector<string> octets = SplitString(address, '.');
  if (octets.size() != 4)
    return false;
  for (unsigned i = 0; i < 4; ++i) {
    uint64_t this_octet = String2Uint64(octets[i]);
    if (this_octet > 255)
      return false;
  }

  return true;
}


/**
 * Basic input validation to ensure that this could syntactically represent a
 * valid IPv6 address.
 */
bool Resolver::IsIpv6Address(const string &address) {
  // Are there any unexpected characters?
  sanitizer::InputSanitizer sanitizer("09 af AF :");
  if (!sanitizer.IsValid(address))
    return false;

  // 8 groups in the range 0-2^16?
  vector<string> groups = SplitString(address, ':');
  if (groups.size() != 8)
    return false;
  for (unsigned i = 0; i < 8; ++i) {
    uint64_t this_group = HexString2Uint64(groups[i]);
    if (this_group > 0xffff)
      return false;
  }

  return true;
}


Resolver::Resolver(const bool ipv4_only, const unsigned timeout)
  : ipv4_only_(ipv4_only)
  , timeout_(timeout)
{
}


/**
 * Calls the overwritten concrete resolver, verifies the sanity of the returned
 * addresses and constructs the Host object.
 */
Host Resolver::Resolve(const string &name) {
  Host host;
  host.name_ = name;
  vector<string> ipv4_addresses;
  vector<string> ipv6_addresses;
  unsigned ttl;
  host.status_ = DoResolve(name, &ipv4_addresses, &ipv6_addresses, &ttl);
  if (host.status_ != kFailOk)
    return host;

  host.deadline_ = time(NULL) + ttl;

  // Verify addresses and make them readily available for curl
  for (unsigned i = 0; i < ipv4_addresses.size(); ++i) {
    if (!IsIpv4Address(ipv4_addresses[i])) {
      LogCvmfs(kLogDns, kLogDebug | kLogSyslogWarn,
               "host name %s resolves to invalid IPv4 address %s",
               name.c_str(), ipv4_addresses[i].c_str());
      continue;
    }
    LogCvmfs(kLogDns, kLogDebug, "add address %s -> %s",
             name.c_str(), ipv4_addresses[i].c_str());
    host.ipv4_addresses_.insert(ipv4_addresses[i]);
  }

  for (unsigned i = 0; i < ipv6_addresses.size(); ++i) {
    if (!IsIpv6Address(ipv6_addresses[i])) {
      LogCvmfs(kLogDns, kLogDebug | kLogSyslogWarn,
               "host name %s resolves to invalid IPv6 address %s",
               name.c_str(), ipv6_addresses[i].c_str());
      continue;
    }
    // For URLs we need brackets around IPv6 addresses
    LogCvmfs(kLogDns, kLogDebug, "add address %s -> %s",
             name.c_str(), ipv6_addresses[i].c_str());
    host.ipv6_addresses_.insert("[" + ipv6_addresses[i] + "]");
  }

  if (host.ipv4_addresses_.empty() && host.ipv6_addresses_.empty())
    host.status_ = kFailNoAddress;

  return host;
}


//------------------------------------------------------------------------------



}  // namespace dns
