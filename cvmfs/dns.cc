/**
 * This file is part of the CernVM File System.
 */

#include "dns.h"

#include <cassert>

#include "duplex_cares.h"

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
Host::Host() {
  id_ = atomic_xadd64(&global_id_, 1);
  status_ = kFailNotYetResolved;
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
 * DNS ttl expires.
 */
bool Host::IsValid() const {
  if (status_ != kFailOk)
    return false;

  time_t now = time(NULL);
  assert(now != static_cast<time_t>(-1));
  return deadline_ >= now;
}


//------------------------------------------------------------------------------


Resolver::Resolver(const bool ipv4_only, const unsigned timeout)
  : ipv4_only_(ipv4_only)
  , timeout_(timeout)
{

}

}  // namespace dns
