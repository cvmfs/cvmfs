/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "url.h"
#include "util/pointer.h"

TEST(T_Url, Empty) {
  UniquePtr<Url> url(Url::Parse(""));
  ASSERT_FALSE(url.IsValid());
}

TEST(T_Url, DigitsOnly) {
  UniquePtr<Url> url(Url::Parse("1234"));
  ASSERT_FALSE(url.IsValid());
}

TEST(T_Url, InvalidPort) {
  UniquePtr<Url> url(Url::Parse("ws://some.host.name:invalid/the/path"));
  ASSERT_FALSE(url.IsValid());
}

TEST(T_Url, HostnameOnly) {
  UniquePtr<Url> url(Url::Parse("localhost"));
  ASSERT_TRUE(url.IsValid());

  EXPECT_EQ(Url::kDefaultProtocol, url->protocol());
  EXPECT_STREQ("localhost", url->host().c_str());
  EXPECT_STREQ("", url->path().c_str());
  EXPECT_STREQ("http://localhost", url->address().c_str());
  EXPECT_EQ(Url::kDefaultPort, url->port());
}

TEST(T_Url, IpOnly) {
  UniquePtr<Url> url(Url::Parse("192.168.0.1"));
  ASSERT_TRUE(url.IsValid());

  EXPECT_EQ(Url::kDefaultProtocol, url->protocol());
  EXPECT_STREQ("192.168.0.1", url->host().c_str());
  EXPECT_STREQ("", url->path().c_str());
  EXPECT_EQ(std::string(Url::kDefaultProtocol) + "://192.168.0.1",
            url->address());
  EXPECT_EQ(Url::kDefaultPort, url->port());
}

TEST(T_Url, HostnameWithProtocol) {
  UniquePtr<Url> url(Url::Parse("https://localhost"));
  ASSERT_TRUE(url.IsValid());

  EXPECT_STREQ("https", url->protocol().c_str());
  EXPECT_STREQ("localhost", url->host().c_str());
  EXPECT_STREQ("", url->path().c_str());
  EXPECT_STREQ("https://localhost", url->address().c_str());
  EXPECT_EQ(Url::kDefaultPort, url->port());
}

TEST(T_Url, HostnameNonDefaultPortAndProtocol) {
  UniquePtr<Url> url(Url::Parse("host", "portocol", 2345));
  ASSERT_TRUE(url.IsValid());

  EXPECT_STREQ("portocol", url->protocol().c_str());
  EXPECT_STREQ("host", url->host().c_str());
  EXPECT_STREQ("", url->path().c_str());
  EXPECT_STREQ("portocol://host:2345", url->address().c_str());
  EXPECT_EQ(2345, url->port());
}

TEST(T_Url, HostnameWithPath) {
  UniquePtr<Url> url(Url::Parse("host.name.com/some/path"));
  ASSERT_TRUE(url.IsValid());

  EXPECT_EQ(Url::kDefaultProtocol, url->protocol());
  EXPECT_STREQ("host.name.com", url->host().c_str());
  EXPECT_STREQ("/some/path", url->path().c_str());
  EXPECT_EQ(std::string(Url::kDefaultProtocol) + "://host.name.com/some/path",
            url->address());
  EXPECT_EQ(Url::kDefaultPort, url->port());
}

TEST(T_Url, HostnameWithProtocolAndPort) {
  UniquePtr<Url> url(Url::Parse("ws://some.host.name:1234"));
  ASSERT_TRUE(url.IsValid());

  EXPECT_STREQ("ws", url->protocol().c_str());
  EXPECT_STREQ("some.host.name", url->host().c_str());
  EXPECT_STREQ("", url->path().c_str());
  EXPECT_STREQ("ws://some.host.name:1234", url->address().c_str());
  EXPECT_EQ(1234, url->port());
}

TEST(T_Url, HostnameWithProtocolAndPath) {
  UniquePtr<Url> url(Url::Parse("file://some.host.name/the/path"));
  ASSERT_TRUE(url.IsValid());

  EXPECT_STREQ("file", url->protocol().c_str());
  EXPECT_STREQ("some.host.name", url->host().c_str());
  EXPECT_STREQ("/the/path", url->path().c_str());
  EXPECT_STREQ("file://some.host.name/the/path", url->address().c_str());
  EXPECT_EQ(Url::kDefaultPort, url->port());
}

TEST(T_Url, HostnameWithProtocolPortAndPath) {
  UniquePtr<Url> url(Url::Parse("ws://some.host.name:1234/the/path"));
  ASSERT_TRUE(url.IsValid());

  EXPECT_STREQ("ws", url->protocol().c_str());
  EXPECT_STREQ("some.host.name", url->host().c_str());
  EXPECT_STREQ("/the/path", url->path().c_str());
  EXPECT_STREQ("ws://some.host.name:1234/the/path", url->address().c_str());
  EXPECT_EQ(1234, url->port());
}

TEST(T_Url, RejectInvalid) {
  {
    UniquePtr<Url> url(Url::Parse("://"));
    EXPECT_FALSE(url.IsValid());
  }
  {
    UniquePtr<Url> url(Url::Parse("dsaas:/dsffsd/asdas"));
    EXPECT_FALSE(url.IsValid());
  }
  {
    UniquePtr<Url> url(Url::Parse(":/:/:/:/:/"));
    EXPECT_FALSE(url.IsValid());
  }
  {
    UniquePtr<Url> url(Url::Parse("/:/"));
    EXPECT_FALSE(url.IsValid());
  }
  {
    UniquePtr<Url> url(Url::Parse("////::::::////"));
    EXPECT_FALSE(url.IsValid());
  }
  {
    UniquePtr<Url> url(Url::Parse("/"));
    EXPECT_FALSE(url.IsValid());
  }
  {
    UniquePtr<Url> url(Url::Parse(":"));
    EXPECT_FALSE(url.IsValid());
  }
}
