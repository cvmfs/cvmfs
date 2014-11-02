/**
 * This file is part of the CernVM File System.
 */

#include "dns.h"

#include <arpa/inet.h>
#include <arpa/nameser.h>
#include <errno.h>
#include <poll.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cstring>

#include "logging.h"
#include "sanitizer.h"
#include "smalloc.h"
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
  return sanitizer.IsValid(address);
}


Resolver::Resolver(
  const bool ipv4_only,
  const unsigned retries,
  const unsigned timeout_ms)
  : ipv4_only_(ipv4_only)
  , retries_(retries)
  , timeout_ms_(timeout_ms)
{
}


/**
 * Wrapper around the vector interface.
 */
Host Resolver::Resolve(const string &name) {
  vector<string> names;
  names.push_back(name);
  vector<Host> hosts;
  ResolveMany(names, &hosts);
  return hosts[0];
}


/**
 * Calls the overwritten concrete resolver, verifies the sanity of the returned
 * addresses and constructs the Host objects in the same order as the names.
 */
void Resolver::ResolveMany(const vector<string> &names, vector<Host> *hosts) {
  unsigned num = names.size();
  if (num == 0)
    return;

  vector<vector<string> > ipv4_addresses(num);
  vector<vector<string> > ipv6_addresses(num);
  vector<Failures> failures(num);
  vector<unsigned> ttls(num);
  DoResolve(names, &ipv4_addresses, &ipv6_addresses, &failures, &ttls);

  for (unsigned i = 0; i < num; ++i) {
    Host host;
    host.name_ = names[i];
    host.status_ = failures[i];
    if (host.status_ != kFailOk) {
      hosts->push_back(host);
      continue;
    }

    unsigned effective_ttl = ttls[i];
    if (effective_ttl < kMinTtl) {
      effective_ttl = kMinTtl;
    } else if (effective_ttl > kMaxTtl) {
      effective_ttl = kMaxTtl;
    }
    host.deadline_ = time(NULL) + effective_ttl;

    // Verify addresses and make them readily available for curl
    for (unsigned j = 0; j < ipv4_addresses[i].size(); ++j) {
      if (!IsIpv4Address(ipv4_addresses[i][j])) {
        LogCvmfs(kLogDns, kLogDebug | kLogSyslogWarn,
                 "host name %s resolves to invalid IPv4 address %s",
                 names[i].c_str(), ipv4_addresses[i][j].c_str());
        continue;
      }
      LogCvmfs(kLogDns, kLogDebug, "add address %s -> %s",
               names[i].c_str(), ipv4_addresses[i][j].c_str());
      host.ipv4_addresses_.insert(ipv4_addresses[i][j]);
    }

    for (unsigned j = 0; j < ipv6_addresses[i].size(); ++j) {
      if (!IsIpv6Address(ipv6_addresses[i][j])) {
        LogCvmfs(kLogDns, kLogDebug | kLogSyslogWarn,
                 "host name %s resolves to invalid IPv6 address %s",
                 names[i].c_str(), ipv6_addresses[i][j].c_str());
        continue;
      }
      // For URLs we need brackets around IPv6 addresses
      LogCvmfs(kLogDns, kLogDebug, "add address %s -> %s",
               names[i].c_str(), ipv6_addresses[i][j].c_str());
      host.ipv6_addresses_.insert("[" + ipv6_addresses[i][j] + "]");
    }

    if (host.ipv4_addresses_.empty() && host.ipv6_addresses_.empty())
      host.status_ = kFailNoAddress;

    hosts->push_back(host);
  }
}


//------------------------------------------------------------------------------


namespace {

enum ResourceRecord {
  kRrA = 0,
  kRrAaaa,
};

struct QueryInfo {
  QueryInfo(vector<string> *a, const string &n, const ResourceRecord r)
    : addresses(a)
    , complete(false)
    , name(n)
    , record(r)
    , status(kFailOther)
    , ttl(0)
  { }

  vector<string> *addresses;
  bool complete;
  string name;
  ResourceRecord record;
  Failures status;
  unsigned ttl;
};

}  // namespace


static Failures CaresExtractIpv4(const unsigned char *abuf, int alen,
                                 vector<string> *addresses,
                                 unsigned *ttl);
static Failures CaresExtractIpv6(const unsigned char *abuf, int alen,
                                 vector<string> *addresses,
                                 unsigned *ttl);

/**
 * Called when a DNS query returns or times out.  Sets the return status and the
 * IP addresses (if successful) in the QueryInfo object.
 */
static void CallbackCares(
  void *arg,
  int status,
  int timeouts_ms,
  unsigned char *abuf,
  int alen)
{
  QueryInfo *info = reinterpret_cast<QueryInfo *>(arg);

  info->complete = true;
  switch (status) {
    case ARES_SUCCESS:
      Failures retval;
      switch (info->record) {
        case kRrA:
          retval = CaresExtractIpv4(abuf, alen, info->addresses, &info->ttl);
          break;
        case kRrAaaa:
          retval = CaresExtractIpv6(abuf, alen, info->addresses, &info->ttl);
          break;
        default:
          // Never here.
          abort();
      }
      info->status = retval;
      break;
    case ARES_ENODATA:
      info->status = kFailUnknownHost;
      break;
    case ARES_EFORMERR:
      info->status = kFailMalformed;
      break;
    case ARES_ENOTFOUND:
      info->status = kFailUnknownHost;
      break;
    case ARES_ETIMEOUT:
      info->status = kFailTimeout;
      break;
    case ARES_ECONNREFUSED:
      info->status = kFailInvalidResolvers;
      break;
    default:
      info->status = kFailOther;
  }
}


/**
 * Extracts IPv4 addresses from an A record return in c-ares.  TTLs are
 * merged to a single one, representing the minimum.
 */
static Failures CaresExtractIpv4(
  const unsigned char *abuf,
  int alen,
  vector<string> *addresses,
  unsigned *ttl)
{
  struct ares_addrttl records[CaresResolver::kMaxAddresses];
  int naddrttls = CaresResolver::kMaxAddresses;
  int retval = ares_parse_a_reply(abuf, alen, NULL, records, &naddrttls);

  switch (retval) {
    case ARES_SUCCESS:
      if (naddrttls <= 0)
        return kFailMalformed;
      *ttl = unsigned(-1);
      for (unsigned i = 0; i < static_cast<unsigned>(naddrttls); ++i) {
        if (records[i].ttl < 0)
          continue;
        *ttl = std::min(unsigned(records[i].ttl), *ttl);

        char addrstr[INET_ADDRSTRLEN];
        const void *retval_p =
          inet_ntop(AF_INET, &(records[i].ipaddr), addrstr, INET_ADDRSTRLEN);
        if (!retval_p)
          continue;
        addresses->push_back(addrstr);
      }
      if (addresses->empty())
        return kFailMalformed;
      return kFailOk;
    case ARES_EBADRESP:
      // Fall through
    case ARES_ENODATA:
      return kFailMalformed;
    default:
      return kFailOther;
  }
}


static Failures CaresExtractIpv6(
  const unsigned char *abuf,
  int alen,
  vector<string> *addresses,
  unsigned *ttl)
{
  struct ares_addr6ttl records[CaresResolver::kMaxAddresses];
  int naddrttls = CaresResolver::kMaxAddresses;
  int retval = ares_parse_aaaa_reply(abuf, alen, NULL, records, &naddrttls);

  switch (retval) {
    case ARES_SUCCESS:
      if (naddrttls <= 0)
        return kFailMalformed;
      *ttl = unsigned(-1);
      for (unsigned i = 0; i < static_cast<unsigned>(naddrttls); ++i) {
        if (records[i].ttl < 0)
          continue;
        *ttl = std::min(unsigned(records[i].ttl), *ttl);

        char addrstr[INET6_ADDRSTRLEN];
        const void *retval_p =
          inet_ntop(AF_INET6, &(records[i].ip6addr), addrstr, INET6_ADDRSTRLEN);
        if (!retval_p)
          continue;
        addresses->push_back(addrstr);
      }
      if (addresses->empty())
        return kFailMalformed;
      return kFailOk;
    case ARES_EBADRESP:
      // Fall through
    case ARES_ENODATA:
      return kFailMalformed;
    default:
      return kFailOther;
  }
}


CaresResolver::CaresResolver(
  const bool ipv4_only,
  const unsigned retries,
  const unsigned timeout_ms)
  : Resolver(ipv4_only, retries, timeout_ms)
  , channel(NULL)
{
}


CaresResolver::~CaresResolver() {
  if (channel) {
    ares_destroy(*channel);
    free(channel);
  }
}


/**
 * Returns a CaresResolver readily initialized, or NULL if an error occurs.
 */
CaresResolver *CaresResolver::Create(
  const bool ipv4_only,
  const unsigned retries,
  const unsigned timeout_ms)
{
  CaresResolver *resolver = new CaresResolver(ipv4_only, retries, timeout_ms);
  resolver->channel = reinterpret_cast<ares_channel *>(
    smalloc(sizeof(ares_channel)));
  memset(resolver->channel, 0, sizeof(ares_channel));

  struct ares_options options;
  memset(&options, 0, sizeof(options));
  options.timeout = timeout_ms;
  options.tries = 1 + retries;
  int retval = ares_init_options(resolver->channel, &options,
                                 ARES_OPT_TIMEOUTMS | ARES_OPT_TRIES);
  if (retval != ARES_SUCCESS) {
    LogCvmfs(kLogDns, kLogDebug | kLogSyslogErr,
             "failed to initialize c-ares resolver (%d - %s)",
             retval, ares_strerror(retval));
    free(resolver->channel);
    resolver->channel = NULL;
    delete resolver;
    return NULL;
  }

  resolver->SetSystemResolvers();
  return resolver;
}


/**
 * Pushes all the DNS queries into the c-ares channel and waits for the results
 * on the file descriptors.
 */
void CaresResolver::DoResolve(
  const vector<string> &names,
  vector<vector<string> > *ipv4_addresses,
  vector<vector<string> > *ipv6_addresses,
  vector<Failures> *failures,
  vector<unsigned> *ttls)
{
  unsigned num = names.size();
  if (num == 0)
    return;

  vector<QueryInfo *> infos_ipv4(num, NULL);
  vector<QueryInfo *> infos_ipv6(num, NULL);

  for (unsigned i = 0; i < num; ++i) {
    if (!ipv4_only()) {
      infos_ipv6[i] = new QueryInfo(&(*ipv6_addresses)[i], names[i], kRrAaaa);
      ares_search(*channel, names[i].c_str(), ns_c_in, ns_t_aaaa,
                  CallbackCares, infos_ipv6[i]);
    }
    infos_ipv4[i] = new QueryInfo(&(*ipv4_addresses)[i], names[i], kRrA);
    ares_search(*channel, names[i].c_str(), ns_c_in, ns_t_a,
                CallbackCares, infos_ipv4[i]);
  }

  bool all_complete;
  do {
    all_complete = true;
    WaitOnCares();
    for (unsigned i = 0; i < num; ++i) {
      if ((infos_ipv4[i] && !infos_ipv4[i]->complete) ||
          (infos_ipv6[i] && !infos_ipv6[i]->complete))
      {
        all_complete = false;
        break;
      }
    }
  } while (!all_complete);

  // Silently ignore errors with IPv4/6 if there are at least some usable IP
  // addresses.
  for (unsigned i = 0; i < num; ++i) {
    Failures status = kFailOther;
    (*ttls)[i] = unsigned(-1);
    if (infos_ipv6[i]) {
      (*ttls)[i] = std::min(infos_ipv6[i]->ttl, (*ttls)[i]);
      status = infos_ipv6[i]->status;
    }
    if (infos_ipv4[i]) {
      (*ttls)[i] = std::min(infos_ipv4[i]->ttl, (*ttls)[i]);
      if (status != kFailOk)
        status = infos_ipv4[i]->status;
    }
    (*failures)[i] = status;
  }

  for (unsigned i = 0; i < num; ++i) {
    delete infos_ipv4[i];
    delete infos_ipv6[i];
  }
}


void CaresResolver::SetResolvers(const vector<string> &new_resolvers) {
}


void CaresResolver::SetSystemResolvers() {
}


/**
 * Polls on c-ares sockets and triggers call-backs execution.  Might be
 * necessary to call this repeatadly.
 */
void CaresResolver::WaitOnCares() {
  // Adapted from libcurl
  ares_socket_t socks[ARES_GETSOCK_MAXNUM];
  struct pollfd pfd[ARES_GETSOCK_MAXNUM];
  int bitmask = ares_getsock(*channel, socks, ARES_GETSOCK_MAXNUM);
  unsigned num = 0;
  for (unsigned i = 0; i < ARES_GETSOCK_MAXNUM; ++i) {
    pfd[i].events = 0;
    pfd[i].revents = 0;
    if (ARES_GETSOCK_READABLE(bitmask, i)) {
      pfd[i].fd = socks[i];
      pfd[i].events |= POLLRDNORM|POLLIN;
    }
    if (ARES_GETSOCK_WRITABLE(bitmask, i)) {
      pfd[i].fd = socks[i];
      pfd[i].events |= POLLWRNORM|POLLOUT;
    }
    if (pfd[i].events != 0)
      num++;
    else
      break;
  }

  int nfds = 0;
  if (num > 0) {
    do {
      nfds = poll(pfd, num, timeout_ms());
      if (nfds == -1) {
        // poll must not fail for other reasons
        if ((errno != EAGAIN) && (errno != EINTR))
          abort();
      }
    } while (nfds == -1);
  }

  if (nfds == 0) {
    // Call ares_process() unconditonally here, even if we simply timed out
    // above, as otherwise the ares name resolve won't timeout.
    ares_process_fd(*channel, ARES_SOCKET_BAD, ARES_SOCKET_BAD);
  } else {
    // Go through the descriptors and ask for executing the callbacks.
    for (unsigned i = 0; i < num; ++i) {
      ares_process_fd(*channel,
                      pfd[i].revents & (POLLRDNORM|POLLIN) ?
                        pfd[i].fd : ARES_SOCKET_BAD,
                      pfd[i].revents & (POLLWRNORM|POLLOUT) ?
                        pfd[i].fd : ARES_SOCKET_BAD);
    }
  }
}

}  // namespace dns
