/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "resolv_conf_event_handler.h"
#include "util/posix.h"

namespace {

const char *ipv4_only = "nameserver 127.0.0.1\n"
                        "memeserver 10.0.0.7\n"
                        "nameserver 192.168.01\n";
const char *ipv6_only = "nameserver 2001:0db8:0000:0042:0000:8a2e:0370:7334\n"
                        "memeserver 2001:0db8:0000:0042:0000:8a2e:0370:7334\n"
                        "nameserver 2001:0db8:0000:0042:0000:8a2e:03707334\n";
const char
    *ipv4_and_ipv6 = "nameserver 127.0.0.1\n"
                     "nameserver 2001:0db8:0000:0042:0000:8a2e:0370:7334\n"
                     "nameserver 192.168.0.1\n"
                     "memeserver 10.0.0.7\n";
const char *rubbish = "This is not supposed"
                      "to be a proper resolv.conf"
                      "instead I wrote a Haiku";
}  // namespace

class T_ResolvConfEventHandler : public ::testing::Test {
 public:
  virtual void SetUp() { addresses.clear(); }

  void TestWithContent(const std::string &content) {
    const std::string prefix = GetCurrentWorkingDirectory() + "/ip_input";
    const std::string input_file = CreateTempPath(prefix, 0600);
    SafeWriteToFile(content, input_file, 0600);

    ResolvConfEventHandler::GetDnsAddresses(input_file, &addresses);
  }

  ResolvConfEventHandler::AddressList addresses;
};

TEST_F(T_ResolvConfEventHandler, IPv4_Only) {
  TestWithContent(ipv4_only);

  EXPECT_EQ(1u, addresses.size());
}

TEST_F(T_ResolvConfEventHandler, IPv6_Only) {
  TestWithContent(ipv6_only);

  EXPECT_EQ(1u, addresses.size());
}

TEST_F(T_ResolvConfEventHandler, IPv4_and_IPv6) {
  TestWithContent(ipv4_and_ipv6);

  EXPECT_EQ(3u, addresses.size());
}

TEST_F(T_ResolvConfEventHandler, Rubbish) {
  TestWithContent(rubbish);

  EXPECT_TRUE(addresses.empty());
}
