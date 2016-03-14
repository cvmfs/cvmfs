/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <cstdio>

#include "../../cvmfs/encrypt.h"
#include "../../cvmfs/json_document.h"
#include "../../cvmfs/util/pointer.h"
#include "../../cvmfs/webapi/macaroon.h"

using namespace std;  // NOLINT

class T_Macaroon : public ::testing::Test {
 protected:
  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};


TEST_F(T_Macaroon, Basics) {
  UniquePtr<cipher::Key> k_s(cipher::Key::CreateRandomly(32));
  EXPECT_TRUE(k_s.IsValid());
  UniquePtr<cipher::Key> k_p(cipher::Key::CreateRandomly(32));
  EXPECT_TRUE(k_p.IsValid());

  cipher::MemoryKeyDatabase key_db;
  string id_s;
  string id_p;
  EXPECT_TRUE(key_db.StoreNew(k_s, &id_s));
  EXPECT_TRUE(key_db.StoreNew(k_p, &id_p));

  Macaroon m1(id_s, k_s.weak_ref(), id_p, k_p.weak_ref());
  Macaroon m2(id_s, k_s.weak_ref(), id_p, k_p.weak_ref());
  EXPECT_NE(m1.ExportPrimary(), m2.ExportPrimary());

  m1.set_ttl(300);
  Macaroon::VerifyFailures failure_code;
  UniquePtr<Macaroon> m_publisher(Macaroon::ParseOnPublisher(
    m1.ExportPrimary(), &key_db, &failure_code));
  EXPECT_TRUE(m_publisher.IsValid());
  EXPECT_EQ(Macaroon::kFailOk, failure_code);

  m_publisher->set_ttl_operation(60);
  m_publisher->set_publish_operation(Macaroon::kPublishTransaction);
  UniquePtr<Macaroon> m_storage(Macaroon::ParseOnStorage(
    m_publisher->ExportAttenuated(), &key_db, &failure_code));
  EXPECT_TRUE(m_storage.IsValid());
  EXPECT_EQ(Macaroon::kFailOk, failure_code);

  /*UniquePtr<JsonDocument> json_document(JsonDocument::Create(m1.ExportPrimary()));
  assert(json_document.IsValid());
  printf("%s\n", json_document->PrintPretty().c_str());

  UniquePtr<JsonDocument> json_pub(JsonDocument::Create(m_publisher->ExportAttenuated()));
  assert(json_pub.IsValid());
  printf("%s\n", json_pub->PrintPretty().c_str());*/
}
