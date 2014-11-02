#include "gtest/gtest.h"

#include "../../cvmfs/dns.h"
#include "../../cvmfs/util.h"

#include <algorithm>
#include <ctime>
#include <cstdio>

using namespace std;  // NOLINT

namespace dns {

class T_Dns : public ::testing::Test {
 protected:
  virtual void SetUp() {
    default_resolver =
      CaresResolver::Create(false /* ipv4_only */, 1 /* retries */, 2000);
    ipv4_resolver =
      CaresResolver::Create(true /* ipv4_only */, 1 /* retries */, 2000);
  }

  virtual ~T_Dns() {
    delete default_resolver;
    delete ipv4_resolver;
  }

  CaresResolver *default_resolver;
  CaresResolver *ipv4_resolver;
};


class DummyResolver : public Resolver {
 public:
  DummyResolver() : Resolver(false /* ipv4 only */, 0 /* retries */, 2000) { };
  ~DummyResolver() { };

  virtual bool SetResolvers(const std::vector<std::string> &resolvers) {
    return false;
  }
  virtual bool SetSearchDomains(const std::vector<std::string> &domains) {
    return false;
  }
  virtual void SetSystemResolvers() {
  }
  virtual void SetSystemSearchDomains() {
  }

 protected:
  virtual void DoResolve(const vector<string> &names,
                         const std::vector<bool> &skip,
                         vector<vector<string> > *ipv4_addresses,
                         vector<vector<string> > *ipv6_addresses,
                         vector<Failures> *failures,
                         vector<unsigned> *ttls)
  {
    for (unsigned i = 0; i < names.size(); ++i) {
      if (skip[i])
        continue;

      (*ttls)[i] = 600;
      if (names[i] == "normal") {
        (*ipv4_addresses)[i].push_back("127.0.0.1");
        (*ipv4_addresses)[i].push_back("127.0.0.2");
        (*ipv6_addresses)[i].push_back(
          "0000:0000:0000:0000:0000:0000:0000:0001");
        (*ipv6_addresses)[i].push_back(
          "0000:0000:0000:0000:0000:0000:0000:a00F");
      } else if (names[i] == "ipv4") {
        (*ipv4_addresses)[i].push_back("127.0.0.1");
        (*ipv4_addresses)[i].push_back("127.0.0.2");
      } else if (names[i] == "ipv6") {
        (*ipv6_addresses)[i].push_back(
          "0000:0000:0000:0000:0000:0000:0000:0001");
        (*ipv6_addresses)[i].push_back(
          "0000:0000:0000:0000:0000:0000:0000:a00F");
      } else if (names[i] == "bad-ipv4") {
        (*ipv4_addresses)[i].push_back("127.0.0.a");
        (*ipv4_addresses)[i].push_back("127.0.0.12345");
        (*ipv4_addresses)[i].push_back("127.0.0");
        (*ipv4_addresses)[i].push_back("abc127.0.0.1");
        (*ipv4_addresses)[i].push_back("127.0.0.1");
      } else if (names[i] == "bad-ipv6") {
        (*ipv6_addresses)[i].push_back(
          "0000:0000:0000:0000:0000:0000:0000:000G");
        (*ipv6_addresses)[i].push_back(
          "0000:0000:0000:0000:0000:0000:0000:0001");
      } else if (names[i] == "large-ttl") {
        (*ipv4_addresses)[i].push_back("127.0.0.1");
        (*ttls)[i] = unsigned(-1);
      } else if (names[i] == "small-ttl") {
        (*ipv4_addresses)[i].push_back("127.0.0.1");
        (*ttls)[i] = 1;
      } else if (names[i] == "timeout") {
        (*failures)[i] = kFailTimeout;
        continue;
      } else if (names[i] == "empty") {
        // No IP addresses returned
      }
      (*failures)[i] = kFailOk;
    }
  }
};

static void ExpectResolvedName(
  const Host &host,
  const string &ipv4,
  const string &ipv6)
{
  set<string> ipv4_addresses = host.ipv4_addresses();
  if (!ipv4.empty()) {
    ASSERT_EQ(ipv4_addresses.size(), 1U);
    EXPECT_EQ(*ipv4_addresses.begin(), ipv4);
  } else {
    EXPECT_EQ(ipv4_addresses.size(), 0U);
  }
  if (!ipv6.empty()) {
    EXPECT_TRUE(host.HasIpv6());
    set<string> ipv6_addresses = host.ipv6_addresses();
    ASSERT_EQ(ipv6_addresses.size(), 1U);
    EXPECT_EQ(*ipv6_addresses.begin(), ipv6);
  } else {
    EXPECT_FALSE(host.HasIpv6());
    EXPECT_EQ(host.ipv6_addresses().size(), 0U);
  }
}


TEST_F(T_Dns, Host) {
  Host host;
  Host host2;
  Host host3 = host;

  EXPECT_EQ(host.id(), host3.id());
  EXPECT_NE(host.id(), host2.id());
  EXPECT_EQ(host.status(), kFailNotYetResolved);
  EXPECT_FALSE(host.IsValid());
  EXPECT_FALSE(host.IsEquivalent(host2));
  EXPECT_FALSE(host.IsEquivalent(host3));
}


TEST_F(T_Dns, HostEquivalent) {
  Host host;
  Host host2;

  host.name_ = host2.name_ = "name";
  host.status_ = host2.status_ = kFailOk;
  host.deadline_ = 1;
  host2.deadline_ = 2;

  EXPECT_TRUE(host.IsEquivalent(host2));
  EXPECT_TRUE(host2.IsEquivalent(host));

  host2.status_ = kFailOther;
  EXPECT_FALSE(host.IsEquivalent(host2));
  EXPECT_FALSE(host2.IsEquivalent(host));
  host2.status_ = kFailOk;

  host.ipv4_addresses_.insert("10.0.0.1");
  host.ipv4_addresses_.insert("10.0.0.2");
  // Different order shouldn't matter.
  host2.ipv4_addresses_.insert("10.0.0.2");
  host2.ipv4_addresses_.insert("10.0.0.1");
  EXPECT_TRUE(host.IsEquivalent(host2));
  EXPECT_TRUE(host2.IsEquivalent(host));

  host.ipv4_addresses_.insert("10.0.0.3");
  EXPECT_FALSE(host.IsEquivalent(host2));
  EXPECT_FALSE(host2.IsEquivalent(host));

  host2.ipv4_addresses_.insert("10.0.0.3");
  EXPECT_TRUE(host.IsEquivalent(host2));
  EXPECT_TRUE(host2.IsEquivalent(host));

  EXPECT_FALSE(host.HasIpv6());
  EXPECT_FALSE(host2.HasIpv6());

  host.ipv6_addresses_.insert("[::1]");
  EXPECT_FALSE(host.IsEquivalent(host2));
  EXPECT_FALSE(host2.IsEquivalent(host));

  host2.ipv6_addresses_.insert("[::1]");
  EXPECT_TRUE(host.IsEquivalent(host2));
  EXPECT_TRUE(host2.IsEquivalent(host));

  host.ipv6_addresses_.insert("[::2]");
  host2.ipv6_addresses_.insert("[::3]");
  EXPECT_FALSE(host.IsEquivalent(host2));
  EXPECT_FALSE(host2.IsEquivalent(host));
}


TEST_F(T_Dns, HostValid) {
  Host host;
  EXPECT_FALSE(host.IsValid());

  host.name_ = "name";
  host.status_ = kFailOther;
  EXPECT_FALSE(host.IsValid());

  host.ipv4_addresses_.insert("10.0.0.1");
  host.status_ = kFailOk;
  host.deadline_ = 0;
  EXPECT_FALSE(host.IsValid());

  host.deadline_ = time(NULL) + 10;
  EXPECT_TRUE(host.IsValid());
}


TEST_F(T_Dns, HostExtendDeadline) {
  Host host;
  host.name_ = "name";
  host.deadline_ = 1;
  host.ipv4_addresses_.insert("10.0.0.1");
  host.ipv6_addresses_.insert("[::2]");
  host.status_ = kFailOk;

  Host host2 = Host::ExtendDeadline(host, 1);
  EXPECT_TRUE(host.IsEquivalent(host2));
  EXPECT_TRUE(host2.IsEquivalent(host));
  EXPECT_EQ(host2.deadline(), 2U);
}


TEST_F(T_Dns, Resolver) {
  DummyResolver resolver;

  Host host = resolver.Resolve("normal");
  EXPECT_EQ(host.name(), "normal");
  EXPECT_EQ(host.status(), kFailOk);
  EXPECT_TRUE(host.IsValid());
  EXPECT_TRUE(host.HasIpv6());
  EXPECT_EQ(host.ipv4_addresses().size(), 2U);
  EXPECT_EQ(host.ipv6_addresses().size(), 2U);

  host = resolver.Resolve("ipv4");
  EXPECT_EQ(host.name(), "ipv4");
  EXPECT_EQ(host.status(), kFailOk);
  EXPECT_TRUE(host.IsValid());
  EXPECT_FALSE(host.HasIpv6());
  EXPECT_EQ(host.ipv4_addresses().size(), 2U);
  EXPECT_EQ(host.ipv6_addresses().size(), 0U);

  host = resolver.Resolve("ipv6");
  EXPECT_EQ(host.name(), "ipv6");
  EXPECT_EQ(host.status(), kFailOk);
  EXPECT_TRUE(host.IsValid());
  EXPECT_TRUE(host.HasIpv6());
  EXPECT_EQ(host.ipv4_addresses().size(), 0U);
  EXPECT_EQ(host.ipv6_addresses().size(), 2U);

  host = resolver.Resolve("bad-ipv4");
  EXPECT_EQ(host.name(), "bad-ipv4");
  EXPECT_EQ(host.status(), kFailOk);
  EXPECT_TRUE(host.IsValid());
  EXPECT_FALSE(host.HasIpv6());
  EXPECT_EQ(host.ipv4_addresses().size(), 1U);
  EXPECT_EQ(host.ipv6_addresses().size(), 0U);

  host = resolver.Resolve("bad-ipv6");
  EXPECT_EQ(host.name(), "bad-ipv6");
  EXPECT_EQ(host.status(), kFailOk);
  EXPECT_TRUE(host.IsValid());
  EXPECT_TRUE(host.HasIpv6());
  EXPECT_EQ(host.ipv4_addresses().size(), 0U);
  EXPECT_EQ(host.ipv6_addresses().size(), 1U);

  host = resolver.Resolve("timeout");
  EXPECT_EQ(host.name(), "timeout");
  EXPECT_EQ(host.status(), kFailTimeout);
  EXPECT_FALSE(host.IsValid());

  host = resolver.Resolve("empty");
  EXPECT_EQ(host.name(), "empty");
  EXPECT_EQ(host.status(), kFailNoAddress);
  EXPECT_FALSE(host.IsValid());
}


TEST_F(T_Dns, ResolverTtlRange) {
  DummyResolver resolver;

  time_t now = time(NULL);
  Host host = resolver.Resolve("small-ttl");
  EXPECT_GE(host.deadline(), now + Resolver::kMinTtl);

  host = resolver.Resolve("large-ttl");
  now = time(NULL);
  EXPECT_LE(host.deadline(), now + Resolver::kMaxTtl);
}


TEST_F(T_Dns, ResolverIpAddresses) {
  DummyResolver resolver;

  Host host = resolver.Resolve("127.0.0.1");
  ExpectResolvedName(host, "127.0.0.1", "");

  host = resolver.Resolve("[::1]");
  ExpectResolvedName(host, "", "[::1]");
}


TEST_F(T_Dns, ResolverEmpty) {
  DummyResolver resolver;
  Host host = resolver.Resolve("");
  EXPECT_EQ(host.status(), kFailInvalidHost);
}


TEST_F(T_Dns, CaresResolverConstruct) {
  CaresResolver *resolver = CaresResolver::Create(false, 2, 2000);
  EXPECT_EQ(resolver->retries(), 2U);
  delete resolver;
}


TEST_F(T_Dns, CaresResolverSimple) {
  Host host = default_resolver->Resolve("a.root-servers.net");
  ExpectResolvedName(host, "198.41.0.4", "[2001:503:ba3e::2:30]");
}


TEST_F(T_Dns, CaresResolverMany) {
  vector<string> names;
  names.push_back("a.root-servers.net");
  names.push_back("b.root-servers.net");
  names.push_back("c.root-servers.net");
  names.push_back("d.root-servers.net");
  names.push_back("e.root-servers.net");
  names.push_back("f.root-servers.net");
  names.push_back("g.root-servers.net");
  names.push_back("h.root-servers.net");
  names.push_back("i.root-servers.net");
  names.push_back("j.root-servers.net");
  names.push_back("k.root-servers.net");
  names.push_back("l.root-servers.net");
  names.push_back("m.root-servers.net");
  names.push_back("127.0.0.1");
  names.push_back("nemo.root-servers.net");
  vector<Host> hosts;
  default_resolver->ResolveMany(names, &hosts);
  ASSERT_EQ(hosts.size(), names.size());
  ExpectResolvedName(hosts[0], "198.41.0.4", "[2001:503:ba3e::2:30]");
  ExpectResolvedName(hosts[1], "192.228.79.201", "[2001:500:84::b]");
  ExpectResolvedName(hosts[2], "192.33.4.12", "[2001:500:2::c]");
  ExpectResolvedName(hosts[3], "199.7.91.13", "[2001:500:2d::d]");
  ExpectResolvedName(hosts[4], "192.203.230.10", "");
  ExpectResolvedName(hosts[5], "192.5.5.241", "[2001:500:2f::f]");
  ExpectResolvedName(hosts[6], "192.112.36.4", "");
  ExpectResolvedName(hosts[7], "128.63.2.53", "[2001:500:1::803f:235]");
  ExpectResolvedName(hosts[8], "192.36.148.17", "[2001:7fe::53]");
  ExpectResolvedName(hosts[9], "192.58.128.30", "[2001:503:c27::2:30]");
  ExpectResolvedName(hosts[10], "193.0.14.129", "[2001:7fd::1]");
  ExpectResolvedName(hosts[11], "199.7.83.42", "[2001:500:3::42]");
  ExpectResolvedName(hosts[12], "202.12.27.33", "[2001:dc3::35]");
  ExpectResolvedName(hosts[13], "127.0.0.1", "");
  EXPECT_EQ(hosts[14].status(), kFailUnknownHost);
}


TEST_F(T_Dns, CaresResolverIpv4) {
  Host host = ipv4_resolver->Resolve("a.root-servers.net");
  ExpectResolvedName(host, "198.41.0.4", "");
}


TEST_F(T_Dns, CaresResolverSameResult) {
  Host host = default_resolver->Resolve("a.root-servers.net");
  Host host2 = default_resolver->Resolve("a.root-servers.net");
  EXPECT_TRUE(host.IsEquivalent(host2));
  EXPECT_TRUE(host2.IsEquivalent(host));
}


TEST_F(T_Dns, CaresResolverFinalDot) {
  Host host = default_resolver->Resolve("a.root-servers.net");
  Host host2 = default_resolver->Resolve("a.root-servers.net.");
  EXPECT_EQ(host.ipv4_addresses(), host2.ipv4_addresses());
  EXPECT_EQ(host.ipv6_addresses(), host2.ipv6_addresses());
}


TEST_F(T_Dns, CaresResolverLocalhost) {
  Host host = default_resolver->Resolve("localhost");
  if (host.HasIpv6()) {
    ExpectResolvedName(host, "127.0.0.1", "[::1]");
  } else {
    ExpectResolvedName(host, "127.0.0.1", "");
  }
}


TEST_F(T_Dns, CaresResolverSearchDomain) {
  /*Host host = default_resolver->Resolve("a");
  EXPECT_EQ(host.status(), kFailUnknownHost);

  vector<string> new_domains;
  new_domains.push_back("root-servers.net");
  bool retval = default_resolver->SetSearchDomains(new_domains);
  EXPECT_EQ(retval, true);
  host = default_resolver->Resolve("a");
  ExpectResolvedName(host, "198.41.0.4", "[2001:503:ba3e::2:30]");*/
}


TEST_F(T_Dns, CaresResolverReadConfig) {
  FILE *f = fopen("/etc/resolv.conf", "r");
  ASSERT_FALSE(f == NULL);
  vector<string> nameservers;
  vector<string> domains;
  string line;
  while (GetLineFile(f, &line)) {
    vector<string> tokens = SplitString(line, ' ');
    if (tokens[0] == "nameserver")
      nameservers.push_back(tokens[1] + ":53");
    else if (tokens[0] == "search")
      domains.push_back(tokens[1]);
  }
  fclose(f);

  vector<string> system_resolvers = default_resolver->resolvers();
  vector<string> system_domains = default_resolver->domains();
  sort(system_resolvers.begin(), system_resolvers.end());
  sort(system_domains.begin(), system_domains.end());
  sort(nameservers.begin(), nameservers.end());
  sort(domains.begin(), domains.end());
  EXPECT_EQ(nameservers, system_resolvers);
  EXPECT_EQ(domains, system_domains);
}


TEST_F(T_Dns, CaresResolverBadResolver) {
  CaresResolver *quick_resolver = CaresResolver::Create(false, 0, 100);
  ASSERT_FALSE(quick_resolver == NULL);

  vector<string> bad_resolvers;
  bad_resolvers.push_back("127.0.0.1");
  bool retval = quick_resolver->SetResolvers(bad_resolvers);
  ASSERT_EQ(retval, true);
  time_t before = time(NULL);
  Host host = quick_resolver->Resolve("a.root-servers.net");
  time_t after = time(NULL);
  EXPECT_EQ(host.status(), kFailInvalidResolvers);
  EXPECT_LE(after-before, 1);
}

}  // namespace dns
