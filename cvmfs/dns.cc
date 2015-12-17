/**
 * This file is part of the CernVM File System.
 *
 * The CernVM-FS name resolving uses objects that inherit from the Resolver
 * interface.  Resolvers implement a vector interface that resolves mutliple
 * names in parallel.  Common cases such as IP addresses as names are handled
 * by the base class -- Resolver implementations only have to resolve real host
 * names to IPv4/6 addresses, using given search domains if necessary.
 *
 * Name resolving information is stored in Host objects.  Host objects are
 * immutable.  They associate a hostname with sets of IPv4 and IPv6 addresses.
 * They also carry the result of the name resolution attempt (success, failure)
 * and the TTL in the form of a deadline.  They are only created by a resolver
 * and upon creation carry a unique id that corresponds to a particular name
 * resolving attempt.
 *
 * The NormalResolver uses both the CaresResolver for DNS queries and the
 * HostfileResolve for queries in /etc/hosts.  If an entry is found in
 * /etc/hosts, the CaresResolver is unused.
 */

#include "dns.h"

#include <arpa/inet.h>
#include <arpa/nameser.h>
#include <errno.h>
#include <netdb.h>
#include <poll.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <cstring>

#include "logging.h"
#include "sanitizer.h"
#include "smalloc.h"
#include "util.h"

using namespace std;  // NOLINT

namespace dns {

/**
 * Sets pos_begin and pos_end to the first/last character of the host name in
 * a string like http://<hostname>:<port>.  Works also if the host name is an
 * IPv4/6 address.  Sets pos_begin and pos_end to 0 if url doesn't match the
 * format.
 */
static void PinpointHostSubstr(
  const std::string &url,
  unsigned *pos_begin,
  unsigned *pos_end)
{
  *pos_begin = *pos_end = 0;
  const unsigned len = url.size();
  unsigned i = 0;

  // Search '//' in the url string and jump behind
  for (; i < len; ++i) {
    if ((url[i] == '/') && (i < len-2) && (url[i+1] == '/')) {
      i += 2;
      *pos_begin = i;
      break;
    }
  }

  // Find the end of the hostname part
  if (*pos_begin > 0) {
    bool in_ipv6 = (url[i] == '[');
    for (; i < len; ++i) {
      if (in_ipv6) {
        if (url[i] != ']')
          continue;
        in_ipv6 = false;
      }

      if ((url[i] == ':') || (url[i] == '/'))
        break;
    }
    if (!in_ipv6)
      *pos_end = i - 1;

    if (*pos_end < *pos_begin)
      *pos_end = *pos_begin = 0;
  }
}


/**
 * Returns the host name from a string in the format
 * http://<hostname>:<port>[/path]
 * or an empty string if url doesn't match the format.
 */
std::string ExtractHost(const std::string &url) {
  unsigned pos_begin;
  unsigned pos_end;
  PinpointHostSubstr(url, &pos_begin, &pos_end);
  if (pos_begin == 0)
    return "";
  return url.substr(pos_begin, (pos_end - pos_begin) + 1);
}


/**
 * Returns the port from a string in the format
 * http://<hostname>:<port>/path
 * or an empty string if url doesn't match the format.
 */
std::string ExtractPort(const std::string &url) {
  unsigned pos_begin;
  unsigned pos_end;
  PinpointHostSubstr(url, &pos_begin, &pos_end);
  if (pos_begin == 0 ||
      pos_end + 2 >= url.size() ||
      url.at(pos_end + 1) != ':')
      return "";

  // Do not include path
  std::size_t pos_port = url.find("/", pos_end);
  std::string retme;
  if (pos_port == std::string::npos)
    retme = url.substr(pos_end + 2);
  else
    retme = url.substr(pos_end + 2, pos_port - pos_end - 2);

  // Port is an integer
  for (std::string::iterator it = retme.begin(); it != retme.end(); ++it)
    if (isdigit(*it) == 0)
      return "";

  return retme;
}


/**
 * Replaces the host name in the url with the given IP address.  If it is an
 * IPv6 address, it has to be in brackets.  If the input is not a valid URL,
 * it is returned unmodified.
 */
string RewriteUrl(const string &url, const string &ip) {
  unsigned pos_begin;
  unsigned pos_end;
  PinpointHostSubstr(url, &pos_begin, &pos_end);
  if (pos_begin == 0)
    return url;

  string result = url;
  result.replace(pos_begin, (pos_end - pos_begin) + 1, ip);
  return result;
}


/**
 * Removes the brackets from IPv6 addresses.  Leaves IPv4 addresses unchanged.
 */
string StripIp(const string &decorated_ip) {
  if (!decorated_ip.empty()) {
    if ((decorated_ip[0] == '[') &&
        (decorated_ip[decorated_ip.length()-1] == ']'))
    {
      return decorated_ip.substr(1, decorated_ip.length()-2);
    }
  }
  return decorated_ip;
}


//------------------------------------------------------------------------------


atomic_int64 Host::global_id_ = 0;

const set<string> &Host::ViewBestAddresses(IpPreference preference) const {
  if (((preference == kIpPreferSystem) || (preference == kIpPreferV4)) &&
      HasIpv4())
  {
    return ipv4_addresses_;
  }
  if ((preference == kIpPreferV6) && !HasIpv6())
    return ipv4_addresses_;
  return ipv6_addresses_;
}


void Host::CopyFrom(const Host &other) {
  deadline_ = other.deadline_;
  id_ = other.id_;
  ipv4_addresses_ = other.ipv4_addresses_;
  ipv6_addresses_ = other.ipv6_addresses_;
  name_ = other.name_;
  status_ = other.status_;
}


/**
 * Creates a copy of the original host with a new ID and sets a new dealine
 * given in seconds from the current time.
 */
Host Host::ExtendDeadline(const Host &original, unsigned seconds_from_now) {
  Host new_host(original);
  new_host.id_ = atomic_xadd64(&global_id_, 1);
  new_host.deadline_ = time(NULL) + seconds_from_now;
  return new_host;
}


/**
 * All fields except the unique id_ are set by the resolver.  Host objects
 * can be copied around but only the resolver can create valid, new objects.
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
 * Compares the TTL from a provious call to time() with the current time.
 */
bool Host::IsExpired() const {
  time_t now = time(NULL);
  assert(now != static_cast<time_t>(-1));
  return deadline_ < now;
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
  return !IsExpired();
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
  , throttle_(0)
{
  prng_.InitLocaltime();
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
  vector<string> fqdns(num);
  vector<bool> skip(num);

  // Deal with special names: empty, IPv4, IPv6
  for (unsigned i = 0; i < num; ++i) {
    if (names[i].empty()) {
      LogCvmfs(kLogDns, kLogDebug, "empty hostname");
      Host invalid_host;
      invalid_host.name_ = "";
      invalid_host.status_ = kFailInvalidHost;
      hosts->push_back(invalid_host);
      skip[i] = true;
    } else if (IsIpv4Address(names[i])) {
      LogCvmfs(kLogDns, kLogDebug, "IPv4 address %s", names[i].c_str());
      Host ipv4_host;
      ipv4_host.name_ = names[i];
      ipv4_host.status_ = kFailOk;
      ipv4_host.ipv4_addresses_.insert(names[i]);
      ipv4_host.deadline_ = time(NULL) + kMaxTtl;
      hosts->push_back(ipv4_host);
      skip[i] = true;
    } else if ((names[i].length() >= 3) &&
               (names[i][0] == '[') &&
               (names[i][names[i].length()-1] == ']'))
    {
      LogCvmfs(kLogDns, kLogDebug, "IPv6 address %s", names[i].c_str());
      Host ipv6_host;
      ipv6_host.name_ = names[i];
      ipv6_host.status_ = kFailOk;
      ipv6_host.ipv6_addresses_.insert(names[i]);
      ipv6_host.deadline_ = time(NULL) + kMaxTtl;
      hosts->push_back(ipv6_host);
      skip[i] = true;
    } else {
      hosts->push_back(Host());
      skip[i] = false;
    }
  }

  DoResolve(
    names, skip, &ipv4_addresses, &ipv6_addresses, &failures, &ttls, &fqdns);

  // Construct host objects
  for (unsigned i = 0; i < num; ++i) {
    if (skip[i])
      continue;

    Host host;
    host.name_ = fqdns[i];
    host.status_ = failures[i];

    unsigned effective_ttl = ttls[i];
    if (effective_ttl < kMinTtl) {
      effective_ttl = kMinTtl;
    } else if (effective_ttl > kMaxTtl) {
      effective_ttl = kMaxTtl;
    }
    host.deadline_ = time(NULL) + effective_ttl;

    if (host.status_ != kFailOk) {
      LogCvmfs(kLogDns, kLogDebug, "failed to resolve %s - %d (%s), ttl %u",
               names[i].c_str(), host.status_, Code2Ascii(host.status_),
               effective_ttl);
      (*hosts)[i] = host;
      continue;
    }

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

    if (host.ipv4_addresses_.empty() && host.ipv6_addresses_.empty()) {
      LogCvmfs(kLogDns, kLogDebug, "no addresses returned for %s",
               names[i].c_str());
      host.status_ = kFailNoAddress;
    }

    // Remove surplus IP addresses
    if (throttle_ > 0) {
      while (host.ipv4_addresses_.size() > throttle_) {
        unsigned random = prng_.Next(host.ipv4_addresses_.size());
        set<string>::iterator rnd_itr = host.ipv4_addresses_.begin();
        std::advance(rnd_itr, random);
        host.ipv4_addresses_.erase(rnd_itr);
      }
      while (host.ipv6_addresses_.size() > throttle_) {
        unsigned random = prng_.Next(host.ipv6_addresses_.size());
        set<string>::iterator rnd_itr = host.ipv6_addresses_.begin();
        std::advance(rnd_itr, random);
        host.ipv6_addresses_.erase(rnd_itr);
      }
    }

    (*hosts)[i] = host;
  }
}


//------------------------------------------------------------------------------


namespace {

enum ResourceRecord {
  kRrA = 0,
  kRrAaaa,
};

/**
 * Used to transport a name resolving request across the asynchronous c-ares
 * interface.  The QueryInfo objects are used for both IPv4 and IPv6 requests.
 * The addresses are entered directly via pointers, ttls and fqdns are later
 * merged into a single response (for IPv4/IPv6).
 */
struct QueryInfo {
  QueryInfo(
    vector<string> *a,
    const string &n,
    const ResourceRecord r)
    : addresses(a)
    , complete(false)
    , fqdn(n)
    , name(n)
    , record(r)
    , status(kFailOther)
    , ttl(0)
  { }

  vector<string> *addresses;
  bool complete;
  string fqdn;
  string name;
  ResourceRecord record;
  Failures status;
  unsigned ttl;
};

}  // namespace


static Failures CaresExtractIpv4(const unsigned char *abuf, int alen,
                                 vector<string> *addresses,
                                 unsigned *ttl,
                                 string *fqdn);
static Failures CaresExtractIpv6(const unsigned char *abuf, int alen,
                                 vector<string> *addresses,
                                 unsigned *ttl,
                                 string *fqdn);

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
          retval = CaresExtractIpv4(
            abuf, alen, info->addresses, &info->ttl, &info->fqdn);
          break;
        case kRrAaaa:
          retval = CaresExtractIpv6(
            abuf, alen, info->addresses, &info->ttl, &info->fqdn);
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
  unsigned *ttl,
  string *fqdn)
{
  struct hostent *host_entry = NULL;
  struct ares_addrttl records[CaresResolver::kMaxAddresses];
  int naddrttls = CaresResolver::kMaxAddresses;
  int retval = ares_parse_a_reply(abuf, alen, &host_entry, records, &naddrttls);

  switch (retval) {
    case ARES_SUCCESS:
      if (host_entry == NULL)
        return kFailMalformed;
      if (host_entry->h_name == NULL) {
        ares_free_hostent(host_entry);
        return kFailMalformed;
      }
      *fqdn = string(host_entry->h_name);
      ares_free_hostent(host_entry);

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
  unsigned *ttl,
  string *fqdn)
{
  struct hostent *host_entry = NULL;
  struct ares_addr6ttl records[CaresResolver::kMaxAddresses];
  int naddrttls = CaresResolver::kMaxAddresses;
  int retval =
    ares_parse_aaaa_reply(abuf, alen, &host_entry, records, &naddrttls);

  switch (retval) {
    case ARES_SUCCESS:
      if (host_entry == NULL)
        return kFailMalformed;
      if (host_entry->h_name == NULL) {
        ares_free_hostent(host_entry);
        return kFailMalformed;
      }
      *fqdn = string(host_entry->h_name);
      ares_free_hostent(host_entry);

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
  , channel_(NULL)
{
}


CaresResolver::~CaresResolver() {
  if (channel_) {
    ares_destroy(*channel_);
    free(channel_);
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
  int retval;
  if (getenv("HOSTALIASES") == NULL) {
    retval = setenv("HOSTALIASES", "/etc/hosts", 1);
    assert(retval == 0);
  }

  CaresResolver *resolver = new CaresResolver(ipv4_only, retries, timeout_ms);
  resolver->channel_ = reinterpret_cast<ares_channel *>(
    smalloc(sizeof(ares_channel)));
  memset(resolver->channel_, 0, sizeof(ares_channel));

  struct ares_addr_node *addresses;
  struct ares_addr_node *iter;
  struct ares_options options;
  int optmask;
  memset(&options, 0, sizeof(options));
  options.timeout = timeout_ms;
  options.tries = 1 + retries;
  options.lookups = strdup("b");
  optmask = ARES_OPT_TIMEOUTMS | ARES_OPT_TRIES | ARES_OPT_LOOKUPS;
  retval = ares_init_options(resolver->channel_, &options, optmask);
  if (retval != ARES_SUCCESS)
    goto create_fail;

  // Save search domains
  retval = ares_save_options(*resolver->channel_, &options, &optmask);
  if (retval != ARES_SUCCESS)
    goto create_fail;
  for (int i = 0; i < options.ndomains; ++i) {
    resolver->domains_.push_back(options.domains[i]);
  }
  ares_destroy_options(&options);
  resolver->system_domains_ = resolver->domains_;

  // Save the system default resolvers
  addresses = NULL;
  retval = ares_get_servers(*resolver->channel_, &addresses);
  if (retval != ARES_SUCCESS)
    goto create_fail;
  iter = addresses;
  while (iter) {
    switch (iter->family) {
      case AF_INET: {
        char addrstr[INET_ADDRSTRLEN];
        const void *retval_p =
          inet_ntop(AF_INET, &(iter->addr), addrstr, INET_ADDRSTRLEN);
        if (!retval_p) {
          LogCvmfs(kLogDns, kLogDebug | kLogSyslogErr,
                   "invalid system name resolver");
        } else {
          resolver->resolvers_.push_back(string(addrstr) + ":53");
        }
        break;
      }
      case AF_INET6: {
        char addrstr[INET6_ADDRSTRLEN];
        const void *retval_p =
          inet_ntop(AF_INET6, &(iter->addr), addrstr, INET6_ADDRSTRLEN);
        if (!retval_p) {
          LogCvmfs(kLogDns, kLogDebug | kLogSyslogErr,
                   "invalid system name resolver");
        } else {
          resolver->resolvers_.push_back("[" + string(addrstr) + "]:53");
        }
        break;
      }
      default:
        // Never here.
        abort();
    }
    iter = iter->next;
  }
  ares_free_data(addresses);
  resolver->system_resolvers_ = resolver->resolvers_;

  return resolver;

 create_fail:
  LogCvmfs(kLogDns, kLogDebug | kLogSyslogErr,
           "failed to initialize c-ares resolver (%d - %s)",
           retval, ares_strerror(retval));
  free(resolver->channel_);
  resolver->channel_ = NULL;
  delete resolver;
  return NULL;
}


/**
 * Pushes all the DNS queries into the c-ares channel and waits for the results
 * on the file descriptors.
 */
void CaresResolver::DoResolve(
  const vector<string> &names,
  const vector<bool> &skip,
  vector<vector<string> > *ipv4_addresses,
  vector<vector<string> > *ipv6_addresses,
  vector<Failures> *failures,
  vector<unsigned> *ttls,
  vector<string> *fqdns)
{
  unsigned num = names.size();
  if (num == 0)
    return;

  vector<QueryInfo *> infos_ipv4(num, NULL);
  vector<QueryInfo *> infos_ipv6(num, NULL);

  for (unsigned i = 0; i < num; ++i) {
    if (skip[i])
      continue;

    if (!ipv4_only()) {
      infos_ipv6[i] = new QueryInfo(&(*ipv6_addresses)[i], names[i], kRrAaaa);
      ares_search(*channel_, names[i].c_str(), ns_c_in, ns_t_aaaa,
                  CallbackCares, infos_ipv6[i]);
    }
    infos_ipv4[i] = new QueryInfo(&(*ipv4_addresses)[i], names[i], kRrA);
    ares_search(*channel_, names[i].c_str(), ns_c_in, ns_t_a,
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
    if (skip[i])
      continue;

    Failures status = kFailOther;
    (*ttls)[i] = unsigned(-1);
    (*fqdns)[i] = "";
    if (infos_ipv6[i]) {
      status = infos_ipv6[i]->status;
      if (status == kFailOk) {
        (*ttls)[i] = std::min(infos_ipv6[i]->ttl, (*ttls)[i]);
        (*fqdns)[i] = infos_ipv6[i]->fqdn;
      }
    }
    if (infos_ipv4[i]) {
      (*ttls)[i] = std::min(infos_ipv4[i]->ttl, (*ttls)[i]);
      if ((*fqdns)[i] == "")
        (*fqdns)[i] = infos_ipv4[i]->fqdn;
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


bool CaresResolver::SetResolvers(const vector<string> &resolvers) {
  string address_list = JoinStrings(resolvers, ",");
  int retval = ares_set_servers_csv(*channel_, address_list.c_str());
  if (retval != ARES_SUCCESS)
    return false;

  resolvers_ = resolvers;
  return true;
}


/**
 * Changes the options of the active channel.  This is hacky and deals with
 * c-ares internal data structures because there is no way to do it via public
 * APIs.
 */
bool CaresResolver::SetSearchDomains(const vector<string> &domains) {
  // From ares_private.h
  struct {
    int flags;
    int timeout;
    int tries;
    int ndots;
    int rotate;
    int udp_port;
    int tcp_port;
    int socket_send_buffer_size;
    int socket_receive_buffer_size;
    char **domains;
    int ndomains;
    // More fields come in the original data structure
  } ares_channelhead;

  memcpy(&ares_channelhead, *channel_, sizeof(ares_channelhead));
  if (ares_channelhead.domains) {
    for (int i = 0; i < ares_channelhead.ndomains; ++i) {
      free(ares_channelhead.domains[i]);
    }
    free(ares_channelhead.domains);
    ares_channelhead.domains = NULL;
  }

  ares_channelhead.ndomains = static_cast<int>(domains.size());
  if (ares_channelhead.ndomains > 0) {
    ares_channelhead.domains = reinterpret_cast<char **>(
      smalloc(ares_channelhead.ndomains * sizeof(char *)));
    for (int i = 0; i < ares_channelhead.ndomains; ++i) {
      ares_channelhead.domains[i] = strdup(domains[i].c_str());
    }
  }

  memcpy(*channel_, &ares_channelhead, sizeof(ares_channelhead));

  domains_ = domains;
  return true;
}


void CaresResolver::SetSystemResolvers() {
  int retval = SetResolvers(system_resolvers_);
  assert(retval == true);
}


void CaresResolver::SetSystemSearchDomains() {
  int retval = SetSearchDomains(system_domains_);
  assert(retval == true);
}


/**
 * Polls on c-ares sockets and triggers call-backs execution.  Might be
 * necessary to call this repeatadly.
 */
void CaresResolver::WaitOnCares() {
  // Adapted from libcurl
  ares_socket_t socks[ARES_GETSOCK_MAXNUM];
  struct pollfd pfd[ARES_GETSOCK_MAXNUM];
  int bitmask = ares_getsock(*channel_, socks, ARES_GETSOCK_MAXNUM);
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
    ares_process_fd(*channel_, ARES_SOCKET_BAD, ARES_SOCKET_BAD);
  } else {
    // Go through the descriptors and ask for executing the callbacks.
    for (unsigned i = 0; i < num; ++i) {
      ares_process_fd(*channel_,
                      pfd[i].revents & (POLLRDNORM|POLLIN) ?
                        pfd[i].fd : ARES_SOCKET_BAD,
                      pfd[i].revents & (POLLWRNORM|POLLOUT) ?
                        pfd[i].fd : ARES_SOCKET_BAD);
    }
  }
}


//------------------------------------------------------------------------------


/**
 * Opens a file descriptor to the host file that stays open until destruction.
 * If no path is given, the HOST_ALIASES environment variable is evaluated
 * followed by /etc/hosts.
 */
HostfileResolver *HostfileResolver::Create(
  const string &path,
  bool ipv4_only)
{
  HostfileResolver *resolver = new HostfileResolver(ipv4_only);

  string hosts_file = path;
  if (hosts_file == "") {
    char *hosts_env = getenv("HOST_ALIASES");
    if (hosts_env != NULL) {
      hosts_file = string(hosts_env);
    } else {
      hosts_file = "/etc/hosts";
    }
  }
  resolver->fhosts_ = fopen(hosts_file.c_str(), "r");
  if (!resolver->fhosts_) {
    LogCvmfs(kLogDns, kLogDebug | kLogSyslogWarn, "failed to read host file %s",
             hosts_file.c_str());
    delete resolver;
    return NULL;
  }
  return resolver;
}


/**
 * Used to process longer domain names before shorter ones, in order to get
 * the correct fully qualified domain name.  Reversed return value in order to
 * sort in descending order.
 */
static bool SortNameLength(const string &a, const string &b) {
  unsigned len_a = a.length();
  unsigned len_b = b.length();
  if (len_a != len_b)
    return len_a > len_b;
  return a > b;
}


/**
 * Creates a fresh reverse lookup map
 */
void HostfileResolver::DoResolve(
  const vector<string> &names,
  const vector<bool> &skip,
  vector< vector<std::string> > *ipv4_addresses,
  vector< vector<std::string> > *ipv6_addresses,
  vector<Failures> *failures,
  vector<unsigned> *ttls,
  vector<string> *fqdns)
{
  unsigned num = names.size();
  if (num == 0)
    return;

  ParseHostFile();
  for (unsigned i = 0; i < num; ++i) {
    if (skip[i])
      continue;

    vector<string> effective_names;
    if (!names[i].empty() && (names[i][names[i].length()-1] == '.')) {
      effective_names.push_back(names[i].substr(0, names[i].length()-1));
    } else {
      effective_names.push_back(names[i]);
      for (unsigned j = 0; j < domains().size(); ++j) {
        effective_names.push_back(names[i] + "." + domains()[j]);
      }
    }

    // Use the longest matching name as fqdn
    std::sort(effective_names.begin(), effective_names.end(), SortNameLength);

    (*failures)[i] = kFailUnknownHost;
    (*fqdns)[i] = names[i];
    for (unsigned j = 0; j < effective_names.size(); ++j) {
      map<string, HostEntry>::iterator iter =
        host_map_.find(effective_names[j]);
      if (iter != host_map_.end()) {
        (*ipv4_addresses)[i].insert((*ipv4_addresses)[i].end(),
                                    iter->second.ipv4_addresses.begin(),
                                    iter->second.ipv4_addresses.end());
        (*ipv6_addresses)[i].insert((*ipv6_addresses)[i].end(),
                                    iter->second.ipv6_addresses.begin(),
                                    iter->second.ipv6_addresses.end());
        (*ttls)[i] = kMinTtl;
        (*fqdns)[i] = effective_names[j];
        (*failures)[i] = kFailOk;
        break;
      }  // Host name found
    }  // All possible names (search domains added)
  }
}


HostfileResolver::HostfileResolver(const bool ipv4_only)
  : Resolver(ipv4_only, 0, 0)
  , fhosts_(NULL)
{ }


HostfileResolver::~HostfileResolver() {
  if (fhosts_)
    fclose(fhosts_);
}


/**
 * TODO: this should be only necessary when the modification timestamp changed.
 */
void HostfileResolver::ParseHostFile() {
  assert(fhosts_);
  rewind(fhosts_);
  host_map_.clear();

  string line;
  while (GetLineFile(fhosts_, &line)) {
    const unsigned len = line.length();
    unsigned i = 0;
    string address;
    while (i < len) {
      if (line[i] == '#')
        break;

      while (((line[i] == ' ') || (line[i] == '\t')) && (i < len))
        ++i;

      string token;
      while ((line[i] != ' ') && (line[i] != '\t') && (line[i] != '#') &&
             (i < len))
      {
        token += line[i];
        ++i;
      }

      if (address == "") {
        address = token;
      } else {
        if (token[token.length()-1] == '.')
          token = token.substr(0, token.length()-1);

        map<string, HostEntry>::iterator iter = host_map_.find(token);
        if (iter == host_map_.end()) {
          HostEntry entry;
          if (IsIpv4Address(address))
            entry.ipv4_addresses.push_back(address);
          else
            if (!ipv4_only()) entry.ipv6_addresses.push_back(address);
          // printf("ADD %s -> %s\n", token.c_str(), address.c_str());
          host_map_[token] = entry;
        } else {
          if (IsIpv4Address(address))
            iter->second.ipv4_addresses.push_back(address);
          else
            if (!ipv4_only()) iter->second.ipv6_addresses.push_back(address);
          // printf("PUSHING %s -> %s\n", token.c_str(), address.c_str());
        }
      }
    }  // Current line
  }  // Hosts file
}


bool HostfileResolver::SetSearchDomains(const vector<string> &domains) {
  domains_ = domains;
  return true;
}


void HostfileResolver::SetSystemSearchDomains() {
  // TODO(jblomer)
  assert(false);
}


//------------------------------------------------------------------------------


/**
 * Creates hostfile and c-ares resolvers and uses c-ares resolvers search
 * domains for the hostfile resolver.
 */
NormalResolver *NormalResolver::Create(
  const bool ipv4_only,
  const unsigned retries,
  const unsigned timeout_ms)
{
  CaresResolver *cares_resolver =
    CaresResolver::Create(ipv4_only, retries, timeout_ms);
  if (!cares_resolver)
    return NULL;
  HostfileResolver *hostfile_resolver = HostfileResolver::Create("", ipv4_only);
  if (!hostfile_resolver) {
    delete cares_resolver;
    return NULL;
  }
  bool retval = hostfile_resolver->SetSearchDomains(cares_resolver->domains());
  assert(retval);

  NormalResolver *normal_resolver = new NormalResolver();
  normal_resolver->cares_resolver_ = cares_resolver;
  normal_resolver->hostfile_resolver_ = hostfile_resolver;
  normal_resolver->domains_ = cares_resolver->domains();
  normal_resolver->resolvers_ = cares_resolver->resolvers();
  normal_resolver->retries_ = cares_resolver->retries();
  normal_resolver->timeout_ms_ = cares_resolver->timeout_ms();
  return normal_resolver;
}


/**
 * Makes only sense for the c-ares resolver.
 */
bool NormalResolver::SetResolvers(const vector<string> &resolvers) {
  return cares_resolver_->SetResolvers(resolvers);
}


/**
 * Set new search domains for both resolvers or for none.
 */
bool NormalResolver::SetSearchDomains(const vector<string> &domains) {
  vector<string> old_domains = hostfile_resolver_->domains();
  bool retval = hostfile_resolver_->SetSearchDomains(domains);
  if (!retval)
    return false;
  retval = cares_resolver_->SetSearchDomains(domains);
  if (!retval) {
    retval = hostfile_resolver_->SetSearchDomains(old_domains);
    assert(retval);
    return false;
  }
  return true;
}


void NormalResolver::SetSystemResolvers() {
  cares_resolver_->SetSystemResolvers();
}


void NormalResolver::SetSystemSearchDomains() {
  cares_resolver_->SetSystemSearchDomains();
  bool retval =
    hostfile_resolver_->SetSearchDomains(cares_resolver_->domains());
  assert(retval);
}


/**
 * First pass done by the hostfile resolver, all successfully resolved names
 * are skipped by the c-ares resolver.
 */
void NormalResolver::DoResolve(
  const vector<string> &names,
  const vector<bool> &skip,
  vector< vector<string> > *ipv4_addresses,
  vector< vector<string> > *ipv6_addresses,
  vector<Failures> *failures,
  vector<unsigned> *ttls,
  vector<string> *fqdns)
{
  unsigned num = names.size();
  hostfile_resolver_->DoResolve(names, skip, ipv4_addresses, ipv6_addresses,
                                failures, ttls, fqdns);
  vector<bool> skip_cares = skip;
  for (unsigned i = 0; i < num; ++i) {
    if ((*failures)[i] == kFailOk)
      skip_cares[i] = true;
  }
  cares_resolver_->DoResolve(names, skip_cares, ipv4_addresses, ipv6_addresses,
                             failures, ttls, fqdns);
}


NormalResolver::NormalResolver()
  : Resolver(false, 0, 0)
  , cares_resolver_(NULL)
  , hostfile_resolver_(NULL)
{
}


NormalResolver::~NormalResolver() {
  delete cares_resolver_;
  delete hostfile_resolver_;
}

}  // namespace dns
