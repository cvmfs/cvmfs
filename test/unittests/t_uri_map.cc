/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <string>

#include "../../cvmfs/webapi/uri_map.h"

using namespace std;  // NOLINT

class TestUriHandler : public UriHandler {
 public:
  explicit TestUriHandler(FastCgi *fcgi) : UriHandler(fcgi) { }
  virtual ~TestUriHandler() { }
  virtual void OnRequest(const uint64_t id, const WebRequest &request) { }
  virtual void OnData(const uint64_t id, unsigned char **buf, unsigned *length)
    { }
};

class T_UriMap : public ::testing::Test {
 protected:
  virtual void SetUp() {
    h1 = new TestUriHandler(NULL);
    h2 = new TestUriHandler(NULL);
    map_.Register("/v1/tickets", h1);
    map_.Register("/v1/ticket/?*", h1);
    map_.Register("/v1/tickets/foo", h2);
    map_.Register("/v1/status", h2);
  }

  virtual void TearDown() {
    delete h1;
    delete h2;
  }

  UriMap map_;
  TestUriHandler *h1;
  TestUriHandler *h2;
};


TEST_F(T_UriMap, Empty) {
  UriMap empty;
  EXPECT_EQ(NULL, empty.Route(""));
  EXPECT_EQ(NULL, empty.Route("/v1/resource"));
}


TEST_F(T_UriMap, Simple) {
  EXPECT_EQ(h1, map_.Route("/v1/tickets"));
  EXPECT_EQ(h1, map_.Route("/v1/tickets/"));
  EXPECT_EQ(h1, map_.Route("/v1/tickets/foo"));
  EXPECT_EQ(NULL, map_.Route("/v1/ticket"));
  EXPECT_EQ(NULL, map_.Route("/v1/ticket/"));
  EXPECT_EQ(NULL, map_.Route("/v2/tickets"));
  EXPECT_EQ(NULL, map_.Route("v1/tickets"));

  EXPECT_EQ(h1, map_.Route("/v1/ticket/foo"));
  EXPECT_EQ(h1, map_.Route("/v1/ticket/foo/bar"));

  EXPECT_EQ(h2, map_.Route("/v1/status"));
  EXPECT_EQ(h2, map_.Route("/v1/status/foo/bar"));
}
