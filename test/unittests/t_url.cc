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

  ASSERT_EQ(Url::kDefaultProtocol, url->protocol());
  ASSERT_STREQ("localhost", url->host().c_str());
  ASSERT_STREQ("", url->path().c_str());
  ASSERT_STREQ("http://localhost", url->address().c_str());
  ASSERT_EQ(Url::kDefaultPort, url->port());
}

TEST(T_Url, IpOnly) {
  UniquePtr<Url> url(Url::Parse("192.168.0.1"));
  ASSERT_TRUE(url.IsValid());

  ASSERT_EQ(Url::kDefaultProtocol, url->protocol());
  ASSERT_STREQ("192.168.0.1", url->host().c_str());
  ASSERT_STREQ("", url->path().c_str());
  ASSERT_EQ(Url::kDefaultProtocol + "://192.168.0.1", url->address());
  ASSERT_EQ(Url::kDefaultPort, url->port());
}

TEST(T_Url, HostnameWithProtocol) {
  UniquePtr<Url> url(Url::Parse("https://localhost"));
  ASSERT_TRUE(url.IsValid());

  ASSERT_STREQ("https", url->protocol().c_str());
  ASSERT_STREQ("localhost", url->host().c_str());
  ASSERT_STREQ("", url->path().c_str());
  ASSERT_STREQ("https://localhost", url->address().c_str());
  ASSERT_EQ(Url::kDefaultPort, url->port());
}

TEST(T_Url, HostnameNonDefaultPortAndProtocol) {
  UniquePtr<Url> url(Url::Parse("host", "portocol", 2345));
  ASSERT_TRUE(url.IsValid());

  ASSERT_STREQ("portocol", url->protocol().c_str());
  ASSERT_STREQ("host", url->host().c_str());
  ASSERT_STREQ("", url->path().c_str());
  ASSERT_STREQ("portocol://host", url->address().c_str());
  ASSERT_EQ(2345, url->port());
}

TEST(T_Url, HostnameWithPath) {
  UniquePtr<Url> url(Url::Parse("host.name.com/some/path"));
  ASSERT_TRUE(url.IsValid());

  ASSERT_EQ(Url::kDefaultProtocol, url->protocol());
  ASSERT_STREQ("host.name.com", url->host().c_str());
  ASSERT_STREQ("/some/path", url->path().c_str());
  ASSERT_EQ(Url::kDefaultProtocol + "://host.name.com/some/path",
            url->address());
  ASSERT_EQ(Url::kDefaultPort, url->port());
}

TEST(T_Url, HostnameWithProtocolAndPort) {
  UniquePtr<Url> url(Url::Parse("ws://some.host.name:1234"));
  ASSERT_TRUE(url.IsValid());

  ASSERT_STREQ("ws", url->protocol().c_str());
  ASSERT_STREQ("some.host.name", url->host().c_str());
  ASSERT_STREQ("", url->path().c_str());
  ASSERT_STREQ("ws://some.host.name", url->address().c_str());
  ASSERT_EQ(1234, url->port());
}

TEST(T_Url, HostnameWithProtocolAndPath) {
  UniquePtr<Url> url(Url::Parse("file://some.host.name/the/path"));
  ASSERT_TRUE(url.IsValid());

  ASSERT_STREQ("file", url->protocol().c_str());
  ASSERT_STREQ("some.host.name", url->host().c_str());
  ASSERT_STREQ("/the/path", url->path().c_str());
  ASSERT_STREQ("file://some.host.name/the/path", url->address().c_str());
  ASSERT_EQ(Url::kDefaultPort, url->port());
}

TEST(T_Url, HostnameWithProtocolPortAndPath) {
  UniquePtr<Url> url(Url::Parse("ws://some.host.name:1234/the/path"));
  ASSERT_TRUE(url.IsValid());

  ASSERT_STREQ("ws", url->protocol().c_str());
  ASSERT_STREQ("some.host.name", url->host().c_str());
  ASSERT_STREQ("/the/path", url->path().c_str());
  ASSERT_STREQ("ws://some.host.name/the/path", url->address().c_str());
  ASSERT_EQ(1234, url->port());
}
