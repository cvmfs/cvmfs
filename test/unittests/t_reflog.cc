/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "reflog.h"
#include "testutil.h"

template<class ReflogT>
class T_Reflog : public ::testing::Test {
 protected:
  static const char sandbox[];
  static const std::string fqrn;

 protected:
  virtual void SetUp() {
    if (NeedsSandbox()) {
      ASSERT_TRUE(MkdirDeep(string(sandbox), 0700))
          << "failed to create sandbox";
    }

    prng_.InitSeed(42);
  }

  virtual void TearDown() {
    if (NeedsSandbox()) {
      const bool retval = RemoveTree(string(sandbox));
      ASSERT_TRUE(retval) << "failed to remove sandbox";
    }

    MockReflog::Reset();
  }

  std::string GetReflogFilename() const {
    const std::string path = (NeedsSandbox())
                                 ? CreateTempPath(string(sandbox) + "/reflog",
                                                  0600)
                                 : ".cvmfsreflog";
    CheckEmpty(path);
    return path;
  }

  bool NeedsSandbox() const { return NeedsSandbox(type<ReflogT>()); }

  ReflogT *CreateReflog(const std::string &path) {
    return CreateReflog(type<ReflogT>(), path);
  }

  ReflogT *OpenReflog(const std::string &path) {
    return OpenReflog(type<ReflogT>(), path);
  }

  void CloseReflog(ReflogT *reflog) {
    return CloseReflog(type<ReflogT>(), reflog);
  }

 private:
  // type-based overloaded instantiation of Reflog object wrapper
  // Inspired from here:
  //   http://stackoverflow.com/questions/5512910/
  //          explicit-specialization-of-template-class-member-function
  template<typename T>
  struct type { };

  bool NeedsSandbox(const type<manifest::Reflog> type_specifier) const {
    return true;
  }

  bool NeedsSandbox(const type<MockReflog> type_specifier) const {
    return false;
  }

  ReflogT *CreateReflog(const type<manifest::Reflog> type_specifier,
                        const std::string &path) {
    return manifest::Reflog::Create(path, fqrn);
  }

  ReflogT *CreateReflog(const type<MockReflog> type_specifier,
                        const std::string &path) {
    MockReflog *reflog = MockReflog::Create(path, fqrn);
    return reflog;
  }

  ReflogT *OpenReflog(const type<manifest::Reflog> type_specifier,
                      const std::string &path) {
    return manifest::Reflog::Open(path);
  }

  ReflogT *OpenReflog(const type<MockReflog> type_specifier,
                      const std::string &path) {
    return MockReflog::Open(path);
  }

  void CloseReflog(const type<manifest::Reflog> type_specifier,
                   ReflogT *reflog) {
    delete reflog;
  }

  void CloseReflog(const type<MockReflog> type_specifier, ReflogT *reflog) {
    // NOOP
  }

  void CheckEmpty(const std::string &str) const { ASSERT_FALSE(str.empty()); }

 protected:
  mutable Prng prng_;
};

template<class ReflogT>
const char T_Reflog<ReflogT>::sandbox[] = "./cvmfs_ut_reflog";

template<class ReflogT>
const std::string T_Reflog<ReflogT>::fqrn = "test.cern.ch";

typedef ::testing::Types<manifest::Reflog, MockReflog> ReflogTypes;
TYPED_TEST_CASE(T_Reflog, ReflogTypes);


TEST(T_Reflog, Checksum) {
  shash::Any content_hash(shash::kSha1);
  content_hash.Randomize();
  manifest::Reflog::WriteChecksum("./reflog.chksum", content_hash);
  shash::Any read_hash;
  EXPECT_TRUE(manifest::Reflog::ReadChecksum("./reflog.chksum", &read_hash));
  EXPECT_EQ(content_hash, read_hash);
}


TYPED_TEST(T_Reflog, Initialize) { }


TYPED_TEST(T_Reflog, CreateReflog) {
  const std::string rp = TestFixture::GetReflogFilename();
  typedef TypeParam Reflog;

  Reflog *reflog = TestFixture::CreateReflog(rp);
  ASSERT_NE(static_cast<Reflog *>(NULL), reflog);
  EXPECT_EQ(TestFixture::fqrn, reflog->fqrn());
  TestFixture::CloseReflog(reflog);
}


TYPED_TEST(T_Reflog, OpenReflog) {
  const std::string rp = TestFixture::GetReflogFilename();
  typedef TypeParam Reflog;

  Reflog *rl1 = TestFixture::CreateReflog(rp);
  ASSERT_NE(static_cast<Reflog *>(NULL), rl1);
  EXPECT_EQ(TestFixture::fqrn, rl1->fqrn());
  TestFixture::CloseReflog(rl1);

  Reflog *rl2 = TestFixture::OpenReflog(rp);
  ASSERT_NE(static_cast<Reflog *>(NULL), rl2);
  EXPECT_EQ(TestFixture::fqrn, rl2->fqrn());
  TestFixture::CloseReflog(rl2);
}


TYPED_TEST(T_Reflog, InsertAndCountReferences) {
  const std::string rp = TestFixture::GetReflogFilename();
  typedef TypeParam Reflog;

  Reflog *rl1 = TestFixture::CreateReflog(rp);
  ASSERT_NE(static_cast<Reflog *>(NULL), rl1);
  EXPECT_EQ(TestFixture::fqrn, rl1->fqrn());

  rl1->AddCatalog(
      h("b99a789dcdffff8f95b977cc8e2037fcd3960b5b", shash::kSuffixCatalog));
  rl1->AddCertificate(
      h("b778b910390254b37ec66366aeef04f034c51941", shash::kSuffixCertificate));
  rl1->AddMetainfo(
      h("8de3e8cbd611ce225d62341698b9408a47edf76b", shash::kSuffixMetainfo));
  rl1->AddHistory(
      h("cab790100c3b10afd7e755b3c93eaeda6a0db9ab", shash::kSuffixHistory));
  TestFixture::CloseReflog(rl1);

  Reflog *rl2 = TestFixture::OpenReflog(rp);
  ASSERT_NE(static_cast<Reflog *>(NULL), rl2);
  EXPECT_EQ(TestFixture::fqrn, rl2->fqrn());
  EXPECT_EQ(4u, rl2->CountEntries());
  TestFixture::CloseReflog(rl2);
}


TYPED_TEST(T_Reflog, List) {
  const std::string rp = TestFixture::GetReflogFilename();
  typedef TypeParam Reflog;

  Reflog *rl1 = TestFixture::CreateReflog(rp);
  ASSERT_NE(static_cast<Reflog *>(NULL), rl1);
  EXPECT_EQ(TestFixture::fqrn, rl1->fqrn());

  rl1->AddCatalog(
      h("b99a789dcdffff8f95b977cc8e2037fcd3960b5b", shash::kSuffixCatalog));
  rl1->AddCatalog(
      h("c5501bd0142cad45c4f0957cbf307e184ac1f661", shash::kSuffixCatalog));
  rl1->AddCertificate(
      h("b778b910390254b37ec66366aeef04f034c51941", shash::kSuffixCertificate));
  rl1->AddCertificate(
      h("b778b910390254b37ec66366aeef04f034c51942", shash::kSuffixCertificate));
  rl1->AddCertificate(
      h("b778b910390254b37ec66366aeef04f034c51943", shash::kSuffixCertificate));
  rl1->AddMetainfo(
      h("8de3e8cbd611ce225d62341698b9408a47edf76b", shash::kSuffixMetainfo));
  rl1->AddMetainfo(
      h("8de3e8cbd611ce225d62341698b9408a47edf76c", shash::kSuffixMetainfo));
  rl1->AddMetainfo(
      h("8de3e8cbd611ce225d62341698b9408a47edf76d", shash::kSuffixMetainfo));
  rl1->AddMetainfo(
      h("8de3e8cbd611ce225d62341698b9408a47edf76e", shash::kSuffixMetainfo));
  rl1->AddHistory(
      h("cab790100c3b10afd7e755b3c93eaeda6a0db9ab", shash::kSuffixHistory));
  TestFixture::CloseReflog(rl1);

  Reflog *rl2 = TestFixture::OpenReflog(rp);
  ASSERT_NE(static_cast<Reflog *>(NULL), rl2);
  EXPECT_EQ(TestFixture::fqrn, rl2->fqrn());
  EXPECT_EQ(10u, rl2->CountEntries());

  std::vector<shash::Any> catalog_hashes;
  ASSERT_TRUE(rl2->List(SqlReflog::kRefCatalog, &catalog_hashes));
  EXPECT_EQ(2u, catalog_hashes.size());

  std::vector<shash::Any> certificate_hashes;
  ASSERT_TRUE(rl2->List(SqlReflog::kRefCertificate, &certificate_hashes));
  EXPECT_EQ(3u, certificate_hashes.size());

  std::vector<shash::Any> metainfo_hashes;
  ASSERT_TRUE(rl2->List(SqlReflog::kRefMetainfo, &metainfo_hashes));
  EXPECT_EQ(4u, metainfo_hashes.size());

  std::vector<shash::Any> history_hashes;
  ASSERT_TRUE(rl2->List(SqlReflog::kRefHistory, &history_hashes));
  EXPECT_EQ(1u, history_hashes.size());

  TestFixture::CloseReflog(rl2);
}


TYPED_TEST(T_Reflog, ListOlderThan) {
  const std::string rp = TestFixture::GetReflogFilename();
  typedef TypeParam Reflog;

  Reflog *rl = TestFixture::CreateReflog(rp);
  ASSERT_NE(static_cast<Reflog *>(NULL), rl);

  rl->AddCatalog(
      h("b99a789dcdffff8f95b977cc8e2037fcd3960b5b", shash::kSuffixCatalog));
  uint64_t t1 = time(NULL);
  uint64_t t2;
  do {
    t2 = time(NULL);
  } while (t1 >= t2);
  rl->AddCatalog(
      h("b99a789dcdffff8f95b977cc8e2037fcd3960b5c", shash::kSuffixCatalog));
  std::vector<shash::Any> hashes;
  EXPECT_TRUE(rl->ListOlderThan(
      SqlReflog::kRefCatalog, static_cast<uint64_t>(-1), &hashes));
  EXPECT_EQ(2U, hashes.size());
  EXPECT_TRUE(rl->ListOlderThan(SqlReflog::kRefCatalog, 0, &hashes));
  EXPECT_EQ(0U, hashes.size());
  EXPECT_TRUE(rl->ListOlderThan(SqlReflog::kRefCatalog, t2, &hashes));
  ASSERT_EQ(1U, hashes.size());
  EXPECT_EQ("b99a789dcdffff8f95b977cc8e2037fcd3960b5b", hashes[0].ToString());

  TestFixture::CloseReflog(rl);
}


TYPED_TEST(T_Reflog, Remove) {
  const std::string rp = TestFixture::GetReflogFilename();
  typedef TypeParam Reflog;

  Reflog *rl1 = TestFixture::CreateReflog(rp);
  ASSERT_NE(static_cast<Reflog *>(NULL), rl1);
  EXPECT_EQ(TestFixture::fqrn, rl1->fqrn());

  rl1->AddCatalog(
      h("b99a789dcdffff8f95b977cc8e2037fcd3960b5b", shash::kSuffixCatalog));
  rl1->AddCatalog(
      h("c5501bd0142cad45c4f0957cbf307e184ac1f661", shash::kSuffixCatalog));
  rl1->AddCertificate(
      h("b778b910390254b37ec66366aeef04f034c51941", shash::kSuffixCertificate));
  rl1->AddMetainfo(
      h("8de3e8cbd611ce225d62341698b9408a47edf76b", shash::kSuffixMetainfo));
  rl1->AddHistory(
      h("cab790100c3b10afd7e755b3c93eaeda6a0db9ab", shash::kSuffixHistory));
  TestFixture::CloseReflog(rl1);

  Reflog *rl2 = TestFixture::OpenReflog(rp);
  ASSERT_NE(static_cast<Reflog *>(NULL), rl2);
  EXPECT_EQ(TestFixture::fqrn, rl2->fqrn());
  EXPECT_EQ(5u, rl2->CountEntries());

  ASSERT_TRUE(rl2->Remove(
      h("c5501bd0142cad45c4f0957cbf307e184ac1f661", shash::kSuffixCatalog)));
  EXPECT_EQ(4u, rl2->CountEntries());
  ASSERT_TRUE(rl2->Remove(h("b778b910390254b37ec66366aeef04f034c51941",
                            shash::kSuffixCertificate)));
  EXPECT_EQ(3u, rl2->CountEntries());
  ASSERT_TRUE(rl2->Remove(
      h("8de3e8cbd611ce225d62341698b9408a47edf76b", shash::kSuffixMetainfo)));
  EXPECT_EQ(2u, rl2->CountEntries());
  ASSERT_TRUE(rl2->Remove(
      h("cab790100c3b10afd7e755b3c93eaeda6a0db9ab", shash::kSuffixHistory)));
  EXPECT_EQ(1u, rl2->CountEntries());

  TestFixture::CloseReflog(rl2);
}


TYPED_TEST(T_Reflog, ContainsObject) {
  const std::string rp = TestFixture::GetReflogFilename();
  typedef TypeParam Reflog;

  Reflog *rl1 = TestFixture::CreateReflog(rp);
  ASSERT_NE(static_cast<Reflog *>(NULL), rl1);
  EXPECT_EQ(TestFixture::fqrn, rl1->fqrn());

  rl1->AddCatalog(
      h("b99a789dcdffff8f95b977cc8e2037fcd3960b5b", shash::kSuffixCatalog));
  rl1->AddCatalog(
      h("c5501bd0142cad45c4f0957cbf307e184ac1f661", shash::kSuffixCatalog));
  rl1->AddCertificate(
      h("b778b910390254b37ec66366aeef04f034c51941", shash::kSuffixCertificate));
  rl1->AddMetainfo(
      h("8de3e8cbd611ce225d62341698b9408a47edf76b", shash::kSuffixMetainfo));
  rl1->AddHistory(
      h("cab790100c3b10afd7e755b3c93eaeda6a0db9ab", shash::kSuffixHistory));

  EXPECT_TRUE(rl1->ContainsCatalog(
      h("b99a789dcdffff8f95b977cc8e2037fcd3960b5b", shash::kSuffixCatalog)));
  EXPECT_FALSE(rl1->ContainsCatalog(
      h("abcde89dcdffff8f95b977cc8e2037fcd3960b5b", shash::kSuffixCatalog)));
  EXPECT_TRUE(rl1->ContainsCertificate(h(
      "b778b910390254b37ec66366aeef04f034c51941", shash::kSuffixCertificate)));
  EXPECT_FALSE(rl1->ContainsCertificate(h(
      "abcde910390254b37ec66366aeef04f034c51941", shash::kSuffixCertificate)));
  EXPECT_TRUE(rl1->ContainsMetainfo(
      h("8de3e8cbd611ce225d62341698b9408a47edf76b", shash::kSuffixMetainfo)));
  EXPECT_FALSE(rl1->ContainsMetainfo(
      h("abcde8cbd611ce225d62341698b9408a47edf76b", shash::kSuffixMetainfo)));
  EXPECT_TRUE(rl1->ContainsHistory(
      h("cab790100c3b10afd7e755b3c93eaeda6a0db9ab", shash::kSuffixHistory)));
  EXPECT_FALSE(rl1->ContainsHistory(
      h("abcde0100c3b10afd7e755b3c93eaeda6a0db9ab", shash::kSuffixHistory)));

  TestFixture::CloseReflog(rl1);

  Reflog *rl2 = TestFixture::OpenReflog(rp);
  ASSERT_NE(static_cast<Reflog *>(NULL), rl2);
  EXPECT_EQ(TestFixture::fqrn, rl2->fqrn());
  EXPECT_EQ(5u, rl2->CountEntries());

  EXPECT_TRUE(rl2->ContainsCatalog(
      h("c5501bd0142cad45c4f0957cbf307e184ac1f661", shash::kSuffixCatalog)));
  EXPECT_FALSE(rl2->ContainsCatalog(
      h("abcdef01422cad45c4f0957cbf307e184ac1f661", shash::kSuffixCatalog)));
  EXPECT_TRUE(rl2->ContainsCertificate(h(
      "b778b910390254b37ec66366aeef04f034c51941", shash::kSuffixCertificate)));
  EXPECT_FALSE(rl2->ContainsCertificate(h(
      "abcde910390254b37ec66366aeef04f034c51941", shash::kSuffixCertificate)));
  EXPECT_TRUE(rl2->ContainsMetainfo(
      h("8de3e8cbd611ce225d62341698b9408a47edf76b", shash::kSuffixMetainfo)));
  EXPECT_FALSE(rl2->ContainsMetainfo(
      h("abcde8cbd611ce225d62341698b9408a47edf76b", shash::kSuffixMetainfo)));
  EXPECT_TRUE(rl2->ContainsHistory(
      h("cab790100c3b10afd7e755b3c93eaeda6a0db9ab", shash::kSuffixHistory)));
  EXPECT_FALSE(rl2->ContainsHistory(
      h("abcde0100c3b10afd7e755b3c93eaeda6a0db9ab", shash::kSuffixHistory)));

  TestFixture::CloseReflog(rl2);
}


TYPED_TEST(T_Reflog, GetTimestamp) {
  const std::string rp = TestFixture::GetReflogFilename();
  typedef TypeParam Reflog;

  Reflog *rl = TestFixture::CreateReflog(rp);
  ASSERT_NE(static_cast<Reflog *>(NULL), rl);

  uint64_t t1 = time(NULL);
  rl->AddCatalog(
      h("b99a789dcdffff8f95b977cc8e2037fcd3960b5b", shash::kSuffixCatalog));
  rl->AddCertificate(
      h("b778b910390254b37ec66366aeef04f034c51941", shash::kSuffixCertificate));
  uint64_t t2 = time(NULL);

  uint64_t timestamp;
  EXPECT_FALSE(rl->GetCatalogTimestamp(
      h("1234567812345678123456781234567812345678", shash::kSuffixCatalog),
      &timestamp));
  EXPECT_TRUE(rl->GetCatalogTimestamp(
      h("b99a789dcdffff8f95b977cc8e2037fcd3960b5b", shash::kSuffixCatalog),
      &timestamp));

  EXPECT_LE(t1, timestamp);
  EXPECT_LE(timestamp, t2);
}
