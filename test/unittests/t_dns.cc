/**
 * This file is part of the CernVM File System.
 */

#include "gtest/gtest.h"

#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <ctime>
#include <string>
#include <vector>

#include "dns.h"
#include "logging.h"
#include "platform.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/string.h"

using namespace std;  // NOLINT

namespace dns {

class T_Dns : public ::testing::Test {
 protected:
  virtual void SetUp() {
    default_resolver =
      CaresResolver::Create(false /* ipv4_only */, 1 /* retries */, 2000);
    ASSERT_TRUE(default_resolver);
    ipv4_resolver =
      CaresResolver::Create(true /* ipv4_only */, 1 /* retries */, 2000);
    ASSERT_TRUE(ipv4_resolver);

    fhostfile = CreateTempFile("./cvmfs_ut_dns", 0600, "w", &hostfile);
    ASSERT_TRUE(fhostfile);
    hostfile_resolver = HostfileResolver::Create(hostfile, false);
    ASSERT_TRUE(hostfile_resolver);
  }

  virtual void TearDown() {
    int retval = unsetenv("HOST_ALIASES");
    ASSERT_EQ(0, retval);
  }

  virtual ~T_Dns() {
    delete default_resolver;
    delete ipv4_resolver;
    delete hostfile_resolver;
    fclose(fhostfile);
    unlink(hostfile.c_str());
  }

  void CreateHostfile(const string &content) {
    int retval = ftruncate(fileno(fhostfile), 0);
    rewind(fhostfile);
    ASSERT_EQ(0, retval);
    for (unsigned i = 0; i < content.length(); ++i) {
      int nbytes = fprintf(fhostfile, "%c", content[i]);
      ASSERT_EQ(1, nbytes);
    }
    fflush(fhostfile);
  }

  struct LogMessage {
    LogSource source;
    int mask;
    string message;

    explicit LogMessage(LogSource source, int mask, string message) {
      this->source = source;
      this->mask = mask;
      this->message = message;
    }
    bool operator==(const LogMessage &other) const {
      return (source == other.source) &&
            (mask == other.mask) &&
            (message == other.message);
    }
  };

  static void TDnsAltLogFunc(LogSource source, int mask, const char *msg) {
    g_log_messages.push_back(LogMessage(source, mask, msg));
  }
  static vector<LogMessage> g_log_messages;

  CaresResolver *default_resolver;
  CaresResolver *ipv4_resolver;
  HostfileResolver *hostfile_resolver;
  FILE *fhostfile;
  string hostfile;
};

vector<T_Dns::LogMessage> T_Dns::g_log_messages;

class DummyResolver : public Resolver {
 public:
  DummyResolver() : Resolver(false /* ipv4 only */, 0 /* retries */, 2000) { }
  ~DummyResolver() { }

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
                         vector<unsigned> *ttls,
                         vector<string> *fqdns)
  {
    for (unsigned i = 0; i < names.size(); ++i) {
      if (skip[i])
        continue;

      (*ttls)[i] = 600;
      (*fqdns)[i] = names[i];
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
      } else  if (names[i] == "many") {
        (*ipv4_addresses)[i].push_back("127.0.0.1");
        (*ipv4_addresses)[i].push_back("127.0.0.2");
        (*ipv4_addresses)[i].push_back("127.0.0.3");
        (*ipv4_addresses)[i].push_back("127.0.0.4");
        (*ipv6_addresses)[i].push_back(
          "0000:0000:0000:0000:0000:0000:0000:0001");
        (*ipv6_addresses)[i].push_back(
          "0000:0000:0000:0000:0000:0000:0000:0002");
        (*ipv6_addresses)[i].push_back(
          "0000:0000:0000:0000:0000:0000:0000:0003");
        (*ipv6_addresses)[i].push_back(
          "0000:0000:0000:0000:0000:0000:0000:0004");
      }
      (*failures)[i] = kFailOk;
    }
  }
};

static void ExpectResolvedName(
  const Host &host,
  const string &fqdn,
  const string &ipv4,
  const string &ipv6)
{
  set<string> ipv4_addresses = host.ipv4_addresses();
  if (!ipv4.empty()) {
    EXPECT_TRUE(host.HasIpv4());
    ASSERT_EQ(1U, ipv4_addresses.size());
    EXPECT_EQ(ipv4, *ipv4_addresses.begin());
  } else {
    EXPECT_FALSE(host.HasIpv4());
    EXPECT_EQ(0U, ipv4_addresses.size());
  }
  if (!ipv6.empty()) {
    EXPECT_TRUE(host.HasIpv6());
    set<string> ipv6_addresses = host.ipv6_addresses();
    ASSERT_EQ(1U, ipv6_addresses.size());
    EXPECT_EQ(ipv6, *ipv6_addresses.begin());
  } else {
    EXPECT_FALSE(host.HasIpv6());
    EXPECT_EQ(0U, host.ipv6_addresses().size());
  }
  EXPECT_EQ(fqdn, host.name());
}


//------------------------------------------------------------------------------


TEST_F(T_Dns, ExtractHost) {
  EXPECT_EQ(ExtractHost("http://localhost:3128"), "localhost");
  EXPECT_EQ(ExtractHost("http://localhost.com:3128"), "localhost.com");
  EXPECT_EQ(ExtractHost("http://localhost/foo"), "localhost");
  EXPECT_EQ(ExtractHost("http://localhost.com/foo"), "localhost.com");
  EXPECT_EQ(ExtractHost("http://localhost.com/"), "localhost.com");
  EXPECT_EQ(ExtractHost("http://localhost.com:3128/"), "localhost.com");
  EXPECT_EQ(ExtractHost("http://localhost.com:3128/foo/bar"), "localhost.com");
  EXPECT_EQ(ExtractHost("http://localhost"), "localhost");
  EXPECT_EQ(ExtractHost("http://127.0.0.1"), "127.0.0.1");
  EXPECT_EQ(ExtractHost("http://[::1]"), "[::1]");
  EXPECT_EQ(ExtractHost("http://[::1]:3128"), "[::1]");
  EXPECT_EQ(ExtractHost("http://[::1]:3128/"), "[::1]");
  EXPECT_EQ(ExtractHost("http://[::1]/foo"), "[::1]");
  EXPECT_EQ(ExtractHost("http://[::1]/"), "[::1]");
  EXPECT_EQ(ExtractHost(""), "");
  EXPECT_EQ(ExtractHost("localhost"), "");
  EXPECT_EQ(ExtractHost("http:/"), "");
  EXPECT_EQ(ExtractHost("http://"), "");
  EXPECT_EQ(ExtractHost("http://:"), "");
  EXPECT_EQ(ExtractHost("http://["), "");
  EXPECT_EQ(ExtractHost("http://[]"), "[]");
  EXPECT_EQ(ExtractHost("http://user:pw@localhost:3128"), "localhost");
  EXPECT_EQ(ExtractHost("http://user:pw@[::1]:3128"), "[::1]");
  EXPECT_EQ(ExtractHost("http://user:pw@localhost/path"), "localhost");
  EXPECT_EQ(ExtractHost("http://user:pw@localhost/p@th"), "localhost");
  EXPECT_EQ(ExtractHost("http://user:pw@"), "");
}


TEST_F(T_Dns, ExtractPort) {
  EXPECT_EQ(ExtractPort("http://localhost:8"), "8");
  EXPECT_EQ(ExtractPort("http://localhost:3128"), "3128");
  EXPECT_EQ(ExtractPort("http://localhost:80/foo"), "80");
  EXPECT_EQ(ExtractPort("http://localhost/foo:80"), "");
  EXPECT_EQ(ExtractPort("http://localhost"), "");
  EXPECT_EQ(ExtractPort("http://localhost:"), "");
  EXPECT_EQ(ExtractPort("http://localhost:/"), "");
  EXPECT_EQ(ExtractPort("http://localhost:port"), "");

  EXPECT_EQ(ExtractPort("http://127.0.0.1:8"), "8");
  EXPECT_EQ(ExtractPort("http://127.0.0.1:3128"), "3128");
  EXPECT_EQ(ExtractPort("http://127.0.0.1:80/foo"), "80");
  EXPECT_EQ(ExtractPort("http://127.0.0.1/foo:80"), "");
  EXPECT_EQ(ExtractPort("http://127.0.0.1"), "");
  EXPECT_EQ(ExtractPort("http://127.0.0.1:"), "");
  EXPECT_EQ(ExtractPort("http://127.0.0.1:/"), "");
  EXPECT_EQ(ExtractPort("http://127.0.0.1:port"), "");

  EXPECT_EQ(ExtractPort("http://[::1]:8"), "8");
  EXPECT_EQ(ExtractPort("http://[::1]:3128"), "3128");
  EXPECT_EQ(ExtractPort("http://[::1]:80/foo"), "80");
  EXPECT_EQ(ExtractPort("http://[::1]/foo:80"), "");
  EXPECT_EQ(ExtractPort("http://[::1]"), "");
  EXPECT_EQ(ExtractPort("http://[::1]:"), "");
  EXPECT_EQ(ExtractPort("http://[::1]:/"), "");
  EXPECT_EQ(ExtractPort("http://[::1]:port"), "");

  EXPECT_EQ(ExtractPort("localhost:8"), "");
  EXPECT_EQ(ExtractPort("localhost:3128"), "");
  EXPECT_EQ(ExtractPort("localhost:80/foo"), "");
  EXPECT_EQ(ExtractPort("localhost/foo:80"), "");
  EXPECT_EQ(ExtractPort("localhost"), "");
  EXPECT_EQ(ExtractPort("localhost:"), "");
  EXPECT_EQ(ExtractPort("localhost:/"), "");
  EXPECT_EQ(ExtractPort("localhost:port"), "");

  EXPECT_EQ(ExtractPort("http://:8"), "");
  EXPECT_EQ(ExtractPort("http://:3128"), "");
  EXPECT_EQ(ExtractPort("http://:80/foo"), "");
  EXPECT_EQ(ExtractPort("http:///foo:80"), "");
  EXPECT_EQ(ExtractPort("http://"), "");
  EXPECT_EQ(ExtractPort("http://:"), "");
  EXPECT_EQ(ExtractPort("http://:/"), "");
  EXPECT_EQ(ExtractPort("http://:port"), "");

  EXPECT_EQ(ExtractPort(""), "");
  EXPECT_EQ(ExtractPort("http:/"), "");
  EXPECT_EQ(ExtractPort("http:/:80"), "");
  EXPECT_EQ(ExtractPort("http://["), "");
  EXPECT_EQ(ExtractPort("http://[:80"), "");
  EXPECT_EQ(ExtractPort("http://[]"), "");
  EXPECT_EQ(ExtractPort("http://[]:80"), "80");
  EXPECT_EQ(ExtractPort("http://[]:port"), "");
}


TEST_F(T_Dns, RewriteUrl) {
  EXPECT_EQ(RewriteUrl("http://localhost:3128", "127.0.0.1"),
            "http://127.0.0.1:3128");
  EXPECT_EQ(RewriteUrl("http://localhost:3128", "[::1]"),
            "http://[::1]:3128");
  EXPECT_EQ(RewriteUrl("http://localhost/foo", "127.0.0.1"),
            "http://127.0.0.1/foo");
  EXPECT_EQ(RewriteUrl("http://localhost", "127.0.0.1"), "http://127.0.0.1");
  EXPECT_EQ(RewriteUrl("http://127.0.0.1", "127.0.0.1"), "http://127.0.0.1");
  EXPECT_EQ(RewriteUrl("http://[::1]", "127.0.0.1"), "http://127.0.0.1");
  EXPECT_EQ(RewriteUrl("http://[::1]:3128", "127.0.0.1"),
            "http://127.0.0.1:3128");
  EXPECT_EQ(RewriteUrl("http://[::1:3128", "127.0.0.1"), "http://[::1:3128");
  EXPECT_EQ(RewriteUrl("http://[::1", "127.0.0.1"), "http://[::1");
  EXPECT_EQ(RewriteUrl("", "127.0.0.1"), "");
  EXPECT_EQ(RewriteUrl("", ""), "");
  EXPECT_EQ(RewriteUrl("http://localhost:3128", ""), "http://:3128");
  EXPECT_EQ(RewriteUrl("http", "127.0.0.1"), "http");
  EXPECT_EQ(RewriteUrl("http:/", "127.0.0.1"), "http:/");
  EXPECT_EQ(RewriteUrl("http://", "127.0.0.1"), "http://");
  EXPECT_EQ(RewriteUrl("http://:", "127.0.0.1"), "http://:");
  EXPECT_EQ(RewriteUrl("http:///", "127.0.0.1"), "http:///");
  EXPECT_EQ(RewriteUrl("http://[", "127.0.0.1"), "http://[");
  EXPECT_EQ(RewriteUrl("http://[]", "127.0.0.1"), "http://127.0.0.1");
  EXPECT_EQ(RewriteUrl("file:///foo/bar", "127.0.0.1"), "file:///foo/bar");
}


TEST_F(T_Dns, StripIp) {
  EXPECT_EQ(StripIp("[::1]"), "::1");
  EXPECT_EQ(StripIp("127.0.0.1"), "127.0.0.1");
  EXPECT_EQ(StripIp("[]"), "");
  EXPECT_EQ(StripIp(""), "");
  EXPECT_EQ(StripIp("["), "[");
  EXPECT_EQ(StripIp("]"), "]");
  EXPECT_EQ(StripIp("[::1"), "[::1");
  EXPECT_EQ(StripIp("::1"), "::1");
}


TEST_F(T_Dns, AddDefaultScheme) {
  EXPECT_EQ("http://[::1]", AddDefaultScheme("[::1]"));
  EXPECT_EQ("http://http:/", AddDefaultScheme("http:/"));
  EXPECT_EQ("http://localhost", AddDefaultScheme("http://localhost"));
  EXPECT_EQ("https://host.example.com:3128",
            AddDefaultScheme("https://host.example.com:3128"));
  EXPECT_EQ("", AddDefaultScheme(""));
  EXPECT_EQ("DIRECT", AddDefaultScheme("DIRECT"));
  EXPECT_EQ("http://direct", AddDefaultScheme("direct"));
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


TEST_F(T_Dns, HostExpired) {
  Host host;
  host.name_ = "name";
  host.status_ = kFailOther;
  host.deadline_ = 0;
  EXPECT_TRUE(host.IsExpired());

  host.deadline_ = time(NULL) + 10;
  EXPECT_FALSE(host.IsExpired());

  host.ipv4_addresses_.insert("10.0.0.1");
  host.status_ = kFailOk;
  EXPECT_FALSE(host.IsExpired());
  host.deadline_ = 0;
  EXPECT_TRUE(host.IsExpired());
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


TEST_F(T_Dns, HostBestAddresses) {
  Host host;
  host.name_ = "name";
  host.status_ = kFailOk;
  host.deadline_ = time(NULL) + 10;
  host.ipv4_addresses_.insert("10.0.0.1");
  EXPECT_TRUE(host.IsValid());

  EXPECT_EQ("10.0.0.1", *host.ViewBestAddresses(kIpPreferSystem).begin());
  EXPECT_EQ("10.0.0.1", *host.ViewBestAddresses(kIpPreferV4).begin());
  EXPECT_EQ("10.0.0.1", *host.ViewBestAddresses(kIpPreferV6).begin());

  host.ipv6_addresses_.insert("[::1]");
  EXPECT_EQ("10.0.0.1", *host.ViewBestAddresses(kIpPreferSystem).begin());
  EXPECT_EQ("10.0.0.1", *host.ViewBestAddresses(kIpPreferV4).begin());
  EXPECT_EQ("[::1]", *host.ViewBestAddresses(kIpPreferV6).begin());

  host.ipv4_addresses_.clear();
  EXPECT_EQ("[::1]", *host.ViewBestAddresses(kIpPreferSystem).begin());
  EXPECT_EQ("[::1]", *host.ViewBestAddresses(kIpPreferV4).begin());
  EXPECT_EQ("[::1]", *host.ViewBestAddresses(kIpPreferV6).begin());
}


TEST_F(T_Dns, HostExtendDeadline) {
  Host host;
  host.name_ = "name";
  host.deadline_ = 10000;
  host.ipv4_addresses_.insert("10.0.0.1");
  host.ipv6_addresses_.insert("[::2]");
  host.status_ = kFailOk;

  Host host2 = Host::ExtendDeadline(host, 10);
  EXPECT_TRUE(host.IsEquivalent(host2));
  EXPECT_TRUE(host2.IsEquivalent(host));
  EXPECT_GE(host2.deadline(), time(NULL) + 9);
  EXPECT_LE(host2.deadline(), time(NULL) + 11);
}


TEST_F(T_Dns, Resolver) {
  DummyResolver resolver;

  Host host = resolver.Resolve("normal");
  EXPECT_EQ(host.name(), "normal");
  EXPECT_EQ(host.status(), kFailOk);
  EXPECT_TRUE(host.IsValid());
  EXPECT_TRUE(host.HasIpv4());
  EXPECT_TRUE(host.HasIpv6());
  EXPECT_EQ(host.ipv4_addresses().size(), 2U);
  EXPECT_EQ(host.ipv6_addresses().size(), 2U);

  host = resolver.Resolve("ipv4");
  EXPECT_EQ(host.name(), "ipv4");
  EXPECT_EQ(host.status(), kFailOk);
  EXPECT_TRUE(host.IsValid());
  EXPECT_TRUE(host.HasIpv4());
  EXPECT_FALSE(host.HasIpv6());
  EXPECT_EQ(host.ipv4_addresses().size(), 2U);
  EXPECT_EQ(host.ipv6_addresses().size(), 0U);

  host = resolver.Resolve("ipv6");
  EXPECT_EQ(host.name(), "ipv6");
  EXPECT_EQ(host.status(), kFailOk);
  EXPECT_TRUE(host.IsValid());
  EXPECT_FALSE(host.HasIpv4());
  EXPECT_TRUE(host.HasIpv6());
  EXPECT_EQ(host.ipv4_addresses().size(), 0U);
  EXPECT_EQ(host.ipv6_addresses().size(), 2U);

  host = resolver.Resolve("bad-ipv4");
  EXPECT_EQ(host.name(), "bad-ipv4");
  EXPECT_EQ(host.status(), kFailOk);
  EXPECT_TRUE(host.IsValid());
  EXPECT_TRUE(host.HasIpv4());
  EXPECT_FALSE(host.HasIpv6());
  EXPECT_EQ(host.ipv4_addresses().size(), 1U);
  EXPECT_EQ(host.ipv6_addresses().size(), 0U);

  host = resolver.Resolve("bad-ipv6");
  EXPECT_EQ(host.name(), "bad-ipv6");
  EXPECT_EQ(host.status(), kFailOk);
  EXPECT_TRUE(host.IsValid());
  EXPECT_FALSE(host.HasIpv4());
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
  EXPECT_GE(host.deadline(),
            now + static_cast<time_t>(Resolver::kDefaultMinTtl));

  host = resolver.Resolve("large-ttl");
  now = time(NULL);
  EXPECT_LE(host.deadline(),
            now + static_cast<time_t>(Resolver::kDefaultMaxTtl));

  resolver.set_min_ttl(3);
  resolver.set_max_ttl(17);
  now = time(NULL);
  host = resolver.Resolve("small-ttl");
  EXPECT_GE(host.deadline(), now + static_cast<time_t>(3));
  host = resolver.Resolve("large-ttl");
  now = time(NULL);
  EXPECT_LE(host.deadline(), now + static_cast<time_t>(17));
}


TEST_F(T_Dns, ResolverIpAddresses) {
  DummyResolver resolver;

  Host host = resolver.Resolve("127.0.0.1");
  ExpectResolvedName(host, "127.0.0.1", "127.0.0.1", "");

  host = resolver.Resolve("[::1]");
  ExpectResolvedName(host, "[::1]", "", "[::1]");

  host = resolver.Resolve("[]");
  EXPECT_FALSE(host.IsValid());
}


TEST_F(T_Dns, ResolverEmpty) {
  DummyResolver resolver;
  Host host = resolver.Resolve("");
  EXPECT_EQ(host.status(), kFailInvalidHost);
}


TEST_F(T_Dns, ResolverManySlow) {
  DummyResolver resolver;

  Host host = resolver.Resolve("many");
  EXPECT_TRUE(host.IsValid());
  EXPECT_EQ(4U, host.ipv4_addresses().size());
  EXPECT_EQ(4U, host.ipv6_addresses().size());

  resolver.set_throttle(2);
  Host host2 = resolver.Resolve("many");
  EXPECT_TRUE(host2.IsValid());
  EXPECT_EQ(2U, host2.ipv4_addresses().size());
  EXPECT_EQ(2U, host2.ipv6_addresses().size());
}


TEST_F(T_Dns, CaresResolverConstruct) {
  CaresResolver *resolver = CaresResolver::Create(false, 2, 2000);
  EXPECT_EQ(resolver->retries(), 2U);
  delete resolver;
}


TEST_F(T_Dns, CaresResolverSimple) {
  Host host = default_resolver->Resolve("a.root-servers.net");
  ExpectResolvedName(host, "a.root-servers.net",
                     "198.41.0.4", "[2001:503:ba3e::2:30]");
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
  names.push_back("nemo.example.com");
  vector<Host> hosts;
  default_resolver->ResolveMany(names, &hosts);
  ASSERT_EQ(hosts.size(), names.size());
  ExpectResolvedName(hosts[0], "a.root-servers.net",
                     "198.41.0.4", "[2001:503:ba3e::2:30]");
  ExpectResolvedName(hosts[1], "b.root-servers.net",
                     "199.9.14.201", "[2001:500:200::b]");
  ExpectResolvedName(hosts[2], "c.root-servers.net",
                     "192.33.4.12", "[2001:500:2::c]");
  ExpectResolvedName(hosts[3], "d.root-servers.net",
                     "199.7.91.13", "[2001:500:2d::d]");
  ExpectResolvedName(hosts[4], "e.root-servers.net",
                     "192.203.230.10", "[2001:500:a8::e]");
  ExpectResolvedName(hosts[5], "f.root-servers.net",
                     "192.5.5.241", "[2001:500:2f::f]");
  ExpectResolvedName(hosts[6], "g.root-servers.net",
                     "192.112.36.4", "[2001:500:12::d0d]");
  ExpectResolvedName(hosts[7], "h.root-servers.net",
                     "198.97.190.53", "[2001:500:1::53]");
  ExpectResolvedName(hosts[8], "i.root-servers.net",
                     "192.36.148.17", "[2001:7fe::53]");
  ExpectResolvedName(hosts[9], "j.root-servers.net",
                     "192.58.128.30", "[2001:503:c27::2:30]");
  ExpectResolvedName(hosts[10], "k.root-servers.net",
                     "193.0.14.129", "[2001:7fd::1]");
  ExpectResolvedName(hosts[11], "l.root-servers.net",
                     "199.7.83.42", "[2001:500:9f::42]");
  ExpectResolvedName(hosts[12], "m.root-servers.net",
                     "202.12.27.33", "[2001:dc3::35]");
  ExpectResolvedName(hosts[13], "127.0.0.1", "127.0.0.1", "");
  EXPECT_TRUE((hosts[14].status() == kFailUnknownHost) ||
              (hosts[14].status() == kFailTimeout));
}


TEST_F(T_Dns, CaresResolverIpv4) {
  Host host = ipv4_resolver->Resolve("a.root-servers.net");
  ExpectResolvedName(host, "a.root-servers.net", "198.41.0.4", "");
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


// TODO(jblomer): figure out why this fails on Travis
TEST_F(T_Dns, CaresResolverLocalhost) {
  Host host = default_resolver->Resolve("localhost");
  // Not using ExpectResolvedName because the canonical name for localhost
  // differs from system to system.
  ASSERT_EQ(1U, host.ipv4_addresses().size());
  EXPECT_EQ("127.0.0.1", *host.ipv4_addresses().begin());
  if (host.HasIpv6()) {
    ASSERT_EQ(1U, host.ipv6_addresses().size());
    EXPECT_EQ("[::1]", *host.ipv6_addresses().begin());
  } else {
    EXPECT_EQ(0U, host.ipv6_addresses().size());
  }
}


TEST_F(T_Dns, CaresResolverSearchDomainSlow) {
  Host host = default_resolver->Resolve("a");
  EXPECT_TRUE((host.status() == kFailUnknownHost) ||
              (host.status() == kFailTimeout) ||
              (host.status() == kFailInvalidResolvers));

  vector<string> new_domains;
  new_domains.push_back("example.com");
  new_domains.push_back("root-servers.net");
  bool retval = default_resolver->SetSearchDomains(new_domains);
  EXPECT_EQ(retval, true);
  host = default_resolver->Resolve("a");
  ExpectResolvedName(host, "a.root-servers.net",
                     "198.41.0.4", "[2001:503:ba3e::2:30]");
  host = default_resolver->Resolve("g");
  ExpectResolvedName(host, "g.root-servers.net",
                     "192.112.36.4", "[2001:500:12::d0d]");

  new_domains.clear();
  retval = default_resolver->SetSearchDomains(new_domains);
  EXPECT_EQ(retval, true);
  host = default_resolver->Resolve("a");
  EXPECT_TRUE((host.status() == kFailUnknownHost) ||
              (host.status() == kFailTimeout) ||
              (host.status() == kFailInvalidResolvers));
}


TEST_F(T_Dns, CaresResolverReadConfig) {
  FILE *f = fopen("/etc/resolv.conf", "r");
  ASSERT_FALSE(f == NULL);
  vector<string> nameservers;
  vector<string> domains;
  string line;
  while (GetLineFile(f, &line)) {
    vector<string> tokens = SplitString(line, ' ');
    if (tokens[0] == "nameserver") {
      if (tokens[1].find(":") != string::npos)
        nameservers.push_back("[" + tokens[1] + "]:53");
      else
        nameservers.push_back(tokens[1] + ":53");
    } else if ((tokens[0] == "search") || (tokens[0] == "domain")) {
      for (unsigned i = 1; i < tokens.size(); ++i)
        domains.push_back(tokens[i]);
    }
  }
  fclose(f);

  vector<string> system_resolvers = default_resolver->resolvers();
  vector<string> system_domains = default_resolver->domains();
  sort(system_domains.begin(), system_domains.end());
  sort(domains.begin(), domains.end());

  // On macOS, libresolv might filter out name servers listed in
  // /etc/resolv.conf
  EXPECT_FALSE(nameservers.empty());
  bool found = false;
  for (unsigned i = 0; i < nameservers.size(); ++i) {
    if (std::find(system_resolvers.begin(), system_resolvers.end(),
                  nameservers[i]) != system_resolvers.end())
    {
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found);
}


TEST_F(T_Dns, CaresResolverBadResolver) {
  UniquePtr<CaresResolver> quick_resolver(CaresResolver::Create(false, 0, 100));
  ASSERT_TRUE(quick_resolver.IsValid());

  vector<string> bad_resolvers;
  bad_resolvers.push_back("127.0.0.2");
  bool retval = quick_resolver->SetResolvers(bad_resolvers);
  ASSERT_EQ(retval, true);
  time_t before = time(NULL);
  Host host = quick_resolver->Resolve("a.root-servers.net");
  time_t after = time(NULL);
  EXPECT_TRUE((host.status() == kFailInvalidResolvers) ||
              (host.status() == kFailTimeout));
  EXPECT_LE(after-before, 1);
}


TEST_F(T_Dns, CaresResolverTimeout) {
  // As of c-ares 1.15, the timeout algorithm changed to exponential backoff
  // without cutoff.  This should result in a total of 4 queries with timeouts
  // of 256, 256, 512, 1024, which sums up to a little over 2 seconds
  UniquePtr<CaresResolver> quick_resolver(CaresResolver::Create(false, 3, 256));
  ASSERT_TRUE(quick_resolver.IsValid());

  vector<string> bad_address;
  bad_address.push_back("127.0.0.2");
  bool retval = quick_resolver->SetResolvers(bad_address);
  ASSERT_EQ(retval, true);
  uint64_t before = platform_monotonic_time();
  Host host = quick_resolver->Resolve("a.root-servers.net");
  uint64_t after = platform_monotonic_time();
  // C-ares oddity: why is it kFailInvalidResolvers in CaresResolverBadResolver?
  EXPECT_EQ(kFailTimeout, host.status());
  // TODO(jblomer): on macOS, the real timeout is sometimes 4s, sometimes 3.1s
  // This is not yet understood.
  EXPECT_LE(after-before, 4U);
}


TEST_F(T_Dns, HostfileResolverConstruct) {
  HostfileResolver *resolver = HostfileResolver::Create("", false);
  ASSERT_TRUE(resolver != NULL);
  delete resolver;

  resolver = HostfileResolver::Create("/no/readable/file", false);
  EXPECT_TRUE(resolver == NULL);
  delete resolver;
}


TEST_F(T_Dns, HostfileResolverSimple) {
  CreateHostfile("127.0.0.1 localhost localhost.localdomain\n::1 localhost");
  Host host = hostfile_resolver->Resolve("localhost");
  EXPECT_EQ(host.status(), kFailOk);
  ExpectResolvedName(host, "localhost", "127.0.0.1", "[::1]");

  host = hostfile_resolver->Resolve("unknown");
  EXPECT_EQ(kFailUnknownHost, host.status());
}


TEST_F(T_Dns, HostfileResolverIpv4only) {
  CreateHostfile("127.0.0.1 localhost\n::1 localhost\n"
                 "::2 localhost2\n127.0.0.2 localhost2\n");
  HostfileResolver *resolver = HostfileResolver::Create(hostfile, true);
  Host host = resolver->Resolve("localhost");
  EXPECT_EQ(host.status(), kFailOk);
  ExpectResolvedName(host, "localhost", "127.0.0.1", "");

  host = resolver->Resolve("localhost2");
  EXPECT_EQ(host.status(), kFailOk);
  ExpectResolvedName(host, "localhost2", "127.0.0.2", "");
  delete resolver;
}


TEST_F(T_Dns, HostfileResolverHostaliasEnv) {
  CreateHostfile("127.0.0.1 weirdhost\n");
  HostfileResolver *resolver = HostfileResolver::Create("", false);
  Host host = resolver->Resolve("weirdhost");
  EXPECT_EQ(host.status(), kFailUnknownHost);
  host = resolver->Resolve("localhost");  // Should be in /etc/hosts
  EXPECT_EQ(host.status(), kFailOk);
  delete resolver;

  int retval = setenv("HOST_ALIASES", hostfile.c_str(), 1);
  ASSERT_EQ(retval, 0);
  resolver = HostfileResolver::Create("", false);
  host = resolver->Resolve("weirdhost");
  EXPECT_EQ(host.status(), kFailOk);
  ExpectResolvedName(host, "weirdhost", "127.0.0.1", "");
  delete resolver;
}


TEST_F(T_Dns, HostfileResolverRefreshedFile) {
  CreateHostfile("127.0.0.1 localhost\n");
  Host host = hostfile_resolver->Resolve("localhost");
  EXPECT_EQ(host.status(), kFailOk);
  ExpectResolvedName(host, "localhost", "127.0.0.1", "");

  CreateHostfile("127.0.0.2 localhost\n127.0.0.3 more\n");
  host = hostfile_resolver->Resolve("localhost");
  EXPECT_EQ(host.status(), kFailOk);
  ExpectResolvedName(host, "localhost", "127.0.0.2", "");
  host = hostfile_resolver->Resolve("more");
  EXPECT_EQ(host.status(), kFailOk);
  ExpectResolvedName(host, "more", "127.0.0.3", "");
}


TEST_F(T_Dns, HostfileResolverSkip) {
  CreateHostfile("127.0.0.1 localhost\n");
  vector<Host> hosts;
  vector<string> names;
  names.push_back("[::1]");
  names.push_back("localhost");
  names.push_back("127.0.0.1");
  names.push_back("127.0.0.1");
  names.push_back("localhost");
  names.push_back("unknown");
  names.push_back("[::1]");
  hostfile_resolver->ResolveMany(names, &hosts);
  // IP addresses are "resolved" by the base class
  EXPECT_EQ(kFailOk, hosts[0].status());
  EXPECT_EQ(kFailOk, hosts[1].status());
  EXPECT_EQ(kFailOk, hosts[2].status());
  EXPECT_EQ(kFailOk, hosts[3].status());
  EXPECT_EQ(kFailOk, hosts[4].status());
  EXPECT_EQ(kFailUnknownHost, hosts[5].status());
  EXPECT_EQ(kFailOk, hosts[6].status());
}


TEST_F(T_Dns, HostfileResolverSearchDomains) {
  CreateHostfile("127.0.0.1 localhost\n127.0.0.2 myhost.mydomain"
                 "127.0.0.3 myhost.remote myhost.remotedomain");
  Host host = hostfile_resolver->Resolve("localhost");
  ExpectResolvedName(host, "localhost", "127.0.0.1", "");
  host = hostfile_resolver->Resolve("localhost.");
  ExpectResolvedName(host, "localhost", "127.0.0.1", "");

  vector<string> search_domains;
  search_domains.push_back("unused");
  search_domains.push_back("mydomain");
  search_domains.push_back("remotedomain");
  bool retval = hostfile_resolver->SetSearchDomains(search_domains);
  EXPECT_EQ(true, retval);
  host = hostfile_resolver->Resolve("myhost.");
  EXPECT_EQ(kFailUnknownHost, host.status());
  host = hostfile_resolver->Resolve("myhost");
  ExpectResolvedName(host, "myhost.remotedomain", "127.0.0.2", "");
}


TEST_F(T_Dns, HostfileResolverEmptyFile) {
  Host host = hostfile_resolver->Resolve("localhost");
  EXPECT_EQ(kFailUnknownHost, host.status());
}


TEST_F(T_Dns, HostfileResolverComment) {
  CreateHostfile("#127.0.0.1 localhost\n127.0.0.2 localhost\n"
                 "127.0.0.3 localh#ost\n127.0.0.4 localhost2#\n");
  Host host = hostfile_resolver->Resolve("localhost");
  EXPECT_EQ(kFailOk, host.status());
  ExpectResolvedName(host, "localhost", "127.0.0.2", "");
  host = hostfile_resolver->Resolve("localh");
  EXPECT_EQ(kFailOk, host.status());
  ExpectResolvedName(host, "localh", "127.0.0.3", "");
  host = hostfile_resolver->Resolve("localhost2");
  EXPECT_EQ(kFailOk, host.status());
  ExpectResolvedName(host, "localhost2", "127.0.0.4", "");
}


TEST_F(T_Dns, HostfileResolverWhitespace) {
  CreateHostfile("127.0.0.1 localhost\n\n\n  127.0.0.2\tlocalhost2\n"
                 "127.0.0.3   \tlocalhost3\t   ");
  Host host = hostfile_resolver->Resolve("localhost");
  EXPECT_EQ(kFailOk, host.status());
  ExpectResolvedName(host, "localhost", "127.0.0.1", "");
  host = hostfile_resolver->Resolve("localhost2");
  EXPECT_EQ(kFailOk, host.status());
  ExpectResolvedName(host, "localhost2", "127.0.0.2", "");
  host = hostfile_resolver->Resolve("localhost3");
  EXPECT_EQ(kFailOk, host.status());
  ExpectResolvedName(host, "localhost3", "127.0.0.3", "");
}


TEST_F(T_Dns, HostfileResolverMultipleAddresses) {
  CreateHostfile("127.0.0.1 localhost\n127.0.0.2 localhost\n"
                 "::1 localhost\n::2 localhost\n");
  Host host = hostfile_resolver->Resolve("localhost");
  EXPECT_EQ(kFailOk, host.status());
  set<string> expected_ipv4;
  set<string> expected_ipv6;
  expected_ipv4.insert("127.0.0.1");
  expected_ipv4.insert("127.0.0.2");
  expected_ipv6.insert("[::1]");
  expected_ipv6.insert("[::2]");
  EXPECT_EQ(kFailOk, host.status());
  EXPECT_EQ(expected_ipv4, host.ipv4_addresses());
  EXPECT_EQ(expected_ipv6, host.ipv6_addresses());
}

TEST_F(T_Dns, HostfileResolverBlankLines) {
  g_log_messages.clear();
  SetAltLogFunc(TDnsAltLogFunc);
  CreateHostfile("   \n  #comment\n\n\n127.0.0.1 localhost\n\n");
  Host host = hostfile_resolver->Resolve("localhost");
  EXPECT_EQ(kFailOk, host.status());
  set<string> expected_ipv4;
  expected_ipv4.insert("127.0.0.1");
  EXPECT_EQ(expected_ipv4, host.ipv4_addresses());
  EXPECT_EQ(0U, g_log_messages.size());
  SetAltLogFunc(NULL);
}

TEST_F(T_Dns, HostfileResolverTooLong) {
  SetAltLogFunc(TDnsAltLogFunc);
  string long_host = "localhost.localhost.localhost.localhost.localhost"
                 ".localhost.localhost.localhost.localhost.localhost.localhost"
                 ".localhost.localhost.localhost.localhost.localhost.localhost"
                 ".localhost.localhost.localhost.localhost.localhost.localhost"
                 ".localhost.localhost.localhost.localhost.localhost.localhost";
  CreateHostfile(string("ABCD:ABCD:ABCD:ABCD:ABCD:ABCD:192.168.158.1901 "
                 "localhost\n127.0.0.2 ") + long_host + "\n");
  Host host = hostfile_resolver->Resolve("localhost");
  EXPECT_EQ(kFailUnknownHost, host.status());
  EXPECT_EQ(0U, host.ipv4_addresses().size());
  EXPECT_EQ(0U, host.ipv6_addresses().size());

  host = hostfile_resolver->Resolve(long_host);
  EXPECT_EQ(kFailUnknownHost, host.status());
  EXPECT_EQ(0U, host.ipv4_addresses().size());
  EXPECT_EQ(0U, host.ipv6_addresses().size());

  vector<LogMessage>::iterator it;
  LogMessage message_long_ip = LogMessage(kLogDns, kLogSyslogWarn,
    "Skipping line in hosts file due to invalid IP address format: "
    "ABCD:ABCD:ABCD:ABCD:ABCD:ABCD:192.168.158.1901 localhost");
  it = find(g_log_messages.begin(), g_log_messages.end(), message_long_ip);
  EXPECT_NE(it, g_log_messages.end());

  LogMessage message_long_host = LogMessage(kLogDns, kLogSyslogWarn,
    "Skipping invalid (too long) hostname in hosts file on line: 127.0.0.2 "
    + long_host);
  it = find(g_log_messages.begin(), g_log_messages.end(), message_long_host);
  EXPECT_NE(it, g_log_messages.end());
  SetAltLogFunc(NULL);
}

TEST_F(T_Dns, HostfileResolverBadFormat) {
  SetAltLogFunc(TDnsAltLogFunc);
  std::string silly_hostname = "foo";
  // Parsing stops at null character
  silly_hostname.push_back('\0');
  silly_hostname += "bar";
  std::string host_content =
    "127.0.0.1\nlocalhost localhost2\n127.0.0.2 " + silly_hostname;
  CreateHostfile(host_content);
  Host host = hostfile_resolver->Resolve("localhost");
  EXPECT_EQ(kFailUnknownHost, host.status());
  host = hostfile_resolver->Resolve("localhost2");
  EXPECT_EQ(kFailNoAddress, host.status());
  host = hostfile_resolver->Resolve(silly_hostname);
  EXPECT_EQ(kFailUnknownHost, host.status());
  host = hostfile_resolver->Resolve("foo");
  EXPECT_EQ(kFailOk, host.status());

  vector<LogMessage>::iterator it;
  LogMessage message_bad_ip = LogMessage(kLogDns, kLogDebug | kLogSyslogWarn,
    "host name localhost2 resolves to invalid IPv6 address localhost");
  it = find(g_log_messages.begin(), g_log_messages.end(), message_bad_ip);
  EXPECT_NE(it, g_log_messages.end());
  SetAltLogFunc(NULL);
}


TEST_F(T_Dns, NormalResolverConstruct) {
  UniquePtr<NormalResolver> resolver(NormalResolver::Create(false, 2, 2000));
  ASSERT_TRUE(resolver.IsValid());
  ASSERT_EQ(resolver->domains(), resolver->cares_resolver_->domains());
  ASSERT_EQ(resolver->resolvers(), resolver->cares_resolver_->resolvers());
  ASSERT_EQ(resolver->timeout_ms(), resolver->cares_resolver_->timeout_ms());
  ASSERT_EQ(resolver->retries(), resolver->cares_resolver_->retries());

  int retval = setenv("HOST_ALIASES", "/no/such/file", 1);
  ASSERT_EQ(0, retval);
  UniquePtr<NormalResolver> resolver2(NormalResolver::Create(false, 2, 2000));
  ASSERT_FALSE(resolver2.IsValid());
}


TEST_F(T_Dns, NormalResolverSimple) {
  UniquePtr<NormalResolver> resolver(NormalResolver::Create(false, 2, 2000));
  ASSERT_TRUE(resolver.IsValid());

  Host host = resolver->Resolve("localhost");
  EXPECT_EQ(kFailOk, host.status());
  host = resolver->Resolve("a.root-servers.net");
  ExpectResolvedName(host, "a.root-servers.net",
                     "198.41.0.4", "[2001:503:ba3e::2:30]");
}


TEST_F(T_Dns, NormalResolverLocalonly) {
  UniquePtr<NormalResolver> resolver(NormalResolver::Create(false, 2, 2000));
  ASSERT_TRUE(resolver.IsValid());

  vector<string> no_resolvers;
  no_resolvers.push_back("127.0.0.2");
  resolver->SetResolvers(no_resolvers);
  Host host = resolver->Resolve("localhost");
  EXPECT_EQ(kFailOk, host.status());
}


// TODO(jblomer): figure out why this fails on Travis
TEST_F(T_Dns, NormalResolverCombinedSlow) {
  UniquePtr<NormalResolver> resolver(NormalResolver::Create(false, 2, 2000));
  ASSERT_TRUE(resolver.IsValid());

  vector<string> names;
  names.push_back("a.root-servers.net");
  names.push_back("b.root-servers.net");
  names.push_back("localhost");
  names.push_back("127.0.0.1");
  names.push_back("[::1]");
  names.push_back("nemo.root-servers.net");
  vector<Host> hosts;
  default_resolver->ResolveMany(names, &hosts);
  ASSERT_EQ(names.size(), hosts.size());
  ExpectResolvedName(hosts[0], "a.root-servers.net",
                     "198.41.0.4", "[2001:503:ba3e::2:30]");
  ExpectResolvedName(hosts[1], "b.root-servers.net",
                     "199.9.14.201", "[2001:500:200::b]");
  EXPECT_EQ(kFailOk, hosts[2].status());
  ExpectResolvedName(hosts[3], "127.0.0.1", "127.0.0.1", "");
  ExpectResolvedName(hosts[4], "[::1]", "", "[::1]");
  EXPECT_TRUE((hosts[5].status() == kFailUnknownHost) ||
              (hosts[5].status() == kFailTimeout));
}

}  // namespace dns
