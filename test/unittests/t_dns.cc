#include "gtest/gtest.h"

#include "../../cvmfs/dns.h"

#include <ctime>

using namespace std;  // NOLINT

namespace dns {

class T_Dns : public ::testing::Test {
 protected:
  virtual void SetUp() {
  }
};


class DummyResolver : public Resolver {
 public:
  DummyResolver() : Resolver(false, 2) { };
  ~DummyResolver() { };

  void SetResolvers(const std::vector<std::string> &new_resolvers) {
  }
  void SetSystemResolvers() {
  }

 protected:
  virtual Failures DoResolve(const string &name,
                             vector<string> *ipv4_addresses,
                             vector<string> *ipv6_addresses,
                             unsigned *ttl)
  {
    if (name == "normal") {
      ipv4_addresses->push_back("127.0.0.1");
      ipv4_addresses->push_back("127.0.0.2");
      ipv6_addresses->push_back("0000:0000:0000:0000:0000:0000:0000:0001");
      ipv6_addresses->push_back("0000:0000:0000:0000:0000:0000:0000:a00F");
    } else if (name == "ipv4") {
      ipv4_addresses->push_back("127.0.0.1");
      ipv4_addresses->push_back("127.0.0.2");
    } else if (name == "ipv6") {
      ipv6_addresses->push_back("0000:0000:0000:0000:0000:0000:0000:0001");
      ipv6_addresses->push_back("0000:0000:0000:0000:0000:0000:0000:a00F");
    } else if (name == "bad-ipv4") {
      ipv4_addresses->push_back("127.0.0.a");
      ipv4_addresses->push_back("127.0.0.12345");
      ipv4_addresses->push_back("127.0.0");
      ipv4_addresses->push_back("abc127.0.0.1");
      ipv4_addresses->push_back("127.0.0.1");
    } else if (name == "bad-ipv6") {
      ipv6_addresses->push_back("0000:0000:0000:0000:0000:0000:0000:000G");
      ipv6_addresses->push_back("0000:0000:0000:0000:0000:0000:0000:fffff");
      ipv6_addresses->push_back("0000:0000:0000:0000:0000:0000:0000");
      ipv6_addresses->push_back("abc0000:0000:0000:0000:0000:0000:0000:0001");
      ipv6_addresses->push_back("0000:0000:0000:0000:0000:0000:0000:0001");
    } else if (name == "timeout") {
      return kFailTimeout;
    } else if (name == "empty") {
      // No IP addresses returned
    }
    *ttl = 600;
    return kFailOk;
  }
};


TEST_F(T_Dns, Host) {
  Host host;
  Host host2;
  Host host3 = host;

  EXPECT_EQ(host.id(), host3.id());
  EXPECT_NE(host.id(), host2.id());
  EXPECT_EQ(host.status_, kFailNotYetResolved);
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

  EXPECT_FALSE(host.hasIpv6());
  EXPECT_FALSE(host2.hasIpv6());

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


TEST_F(T_Dns, Resolver) {
  DummyResolver resolver;

  Host host = resolver.Resolve("normal");
  EXPECT_EQ(host.name(), "normal");
  EXPECT_EQ(host.status(), kFailOk);
  EXPECT_TRUE(host.IsValid());
  EXPECT_TRUE(host.hasIpv6());
  EXPECT_EQ(host.ipv4_addresses().size(), 2U);
  EXPECT_EQ(host.ipv6_addresses().size(), 2U);

  host = resolver.Resolve("ipv4");
  EXPECT_EQ(host.name(), "ipv4");
  EXPECT_EQ(host.status(), kFailOk);
  EXPECT_TRUE(host.IsValid());
  EXPECT_FALSE(host.hasIpv6());
  EXPECT_EQ(host.ipv4_addresses().size(), 2U);
  EXPECT_EQ(host.ipv6_addresses().size(), 0U);

  host = resolver.Resolve("ipv6");
  EXPECT_EQ(host.name(), "ipv6");
  EXPECT_EQ(host.status(), kFailOk);
  EXPECT_TRUE(host.IsValid());
  EXPECT_TRUE(host.hasIpv6());
  EXPECT_EQ(host.ipv4_addresses().size(), 0U);
  EXPECT_EQ(host.ipv6_addresses().size(), 2U);

  host = resolver.Resolve("bad-ipv4");
  EXPECT_EQ(host.name(), "bad-ipv4");
  EXPECT_EQ(host.status(), kFailOk);
  EXPECT_TRUE(host.IsValid());
  EXPECT_FALSE(host.hasIpv6());
  EXPECT_EQ(host.ipv4_addresses().size(), 1U);
  EXPECT_EQ(host.ipv6_addresses().size(), 0U);

  host = resolver.Resolve("bad-ipv6");
  EXPECT_EQ(host.name(), "bad-ipv6");
  EXPECT_EQ(host.status(), kFailOk);
  EXPECT_TRUE(host.IsValid());
  EXPECT_TRUE(host.hasIpv6());
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

}  // namespace dns
