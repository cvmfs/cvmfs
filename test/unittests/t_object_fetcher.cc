#include <gtest/gtest.h>

#include "../../cvmfs/util.h"
#include "../../cvmfs/history_sqlite.h"
#include "../../cvmfs/compression.h"

#include "testutil.h"

using namespace history;

template <class ObjectFetcherT>
class T_ObjectFetcher : public ::testing::Test {
 protected:
  static const std::string sandbox;
  static const std::string fqrn;
  static const std::string backend_storage;
  static const std::string backend_storage_dir;
  static const std::string manifest_path;
  static const std::string temp_directory;

  static const shash::Any previous_history_hash;

 protected:
  virtual void SetUp() {
    if (NeedsSandbox()) {
      const bool retval = MkdirDeep(sandbox,                        0700) &&
                          MkdirDeep(backend_storage,                0700) &&
                          MkdirDeep(backend_storage_dir,            0700) &&
                          MkdirDeep(temp_directory,                 0700) &&
                          MakeCacheDirectories(backend_storage_dir, 0700);
      ASSERT_TRUE (retval) << "failed to create sandbox";
    }

    InitializeSandbox();
  }

  virtual void TearDown() {
    if (NeedsSandbox()) {
      const bool retval = RemoveTree(sandbox);
      ASSERT_TRUE (retval) << "failed to remove sandbox";
    }

    MockHistory::Reset();
    MockCatalog::Reset();
  }

  void InitializeSandbox() {
    if (NeedsSandbox()) {
      // create manifest
      const uint64_t    catalog_size = 0;
      const std::string root_path    = "";
      UniquePtr<manifest::Manifest> manifest(new manifest::Manifest(
                                                   MockCatalog::root_hash,
                                                   catalog_size,
                                                   root_path));
      manifest->set_history(MockHistory::root_hash);
      ASSERT_TRUE (manifest->Export(manifest_path)) << "failed to create manifest";
    }

    // create some history objects
    CreateHistory(previous_history_hash);
    CreateHistory(MockHistory::root_hash, previous_history_hash);
  }

  ObjectFetcherT* GetObjectFetcher() {
    return GetObjectFetcher(type<ObjectFetcherT>());
  }

  void CreateHistory(const shash::Any &content_hash,
                     const shash::Any &previous_revision = shash::Any()) {
    return CreateHistory(type<ObjectFetcherT>(),
                         content_hash,
                         previous_revision);
  }

  bool NeedsSandbox() {
    return NeedsSandbox(type<ObjectFetcherT>());
  }

 private:
  // type-based overloading helper struct
  // Inspired from here:
  //   http://stackoverflow.com/questions/5512910/
  //          explicit-specialization-of-template-class-member-function
  template <typename T> struct type {};

  ObjectFetcherT* GetObjectFetcher(const type<LocalObjectFetcher<> > t) {
    return new LocalObjectFetcher<>(backend_storage, temp_directory);
  }

  ObjectFetcherT* GetObjectFetcher(const type<MockObjectFetcher> t) {
    return new MockObjectFetcher();
  }

  void CreateHistory(const type<LocalObjectFetcher<> > t,
                     const shash::Any &content_hash,
                     const shash::Any &previous_revision) {
    const std::string tmp_path = CreateTempPath(sandbox, 0700);
    ASSERT_FALSE (tmp_path.empty()) << "failed to create tmp in: " << sandbox;

    history::SqliteHistory *history =
                              ObjectFetcherT::history_t::Create(tmp_path, fqrn);
    ASSERT_NE (static_cast<history::SqliteHistory*>(NULL), history) <<
      "failed to create new history database in: " << tmp_path;
    history->SetPreviousRevision(previous_revision);
    delete history;

    const std::string result_path =
                      backend_storage + "/" + content_hash.MakePathWithSuffix();
    ASSERT_TRUE(zlib::CompressPath2Path(tmp_path, result_path)) <<
      "failed to compress file " << tmp_path << " to " << result_path;
  }

  void CreateHistory(const type<MockObjectFetcher> t,
                     const shash::Any &content_hash,
                     const shash::Any &previous_revision) {
    const bool writable = true;
    MockHistory *history = new MockHistory(writable, fqrn);
    history->SetPreviousRevision(previous_revision);
    MockHistory::RegisterObject(content_hash, history);
  }

  bool NeedsSandbox(const type<LocalObjectFetcher<> > t) { return true;  }
  bool NeedsSandbox(const type<MockObjectFetcher>     t) { return false; }
};

template <class ObjectFetcherT>
const std::string T_ObjectFetcher<ObjectFetcherT>::sandbox =
  "/tmp/cvmfs_ut_object_fetcher";

template <class ObjectFetcherT>
const std::string T_ObjectFetcher<ObjectFetcherT>::fqrn    = "test.cern.ch";

template <class ObjectFetcherT>
const std::string T_ObjectFetcher<ObjectFetcherT>::backend_storage =
  T_ObjectFetcher<ObjectFetcherT>::sandbox + "/backend";

template <class ObjectFetcherT>
const std::string T_ObjectFetcher<ObjectFetcherT>::backend_storage_dir =
  T_ObjectFetcher<ObjectFetcherT>::sandbox + "/backend/data";

template <class ObjectFetcherT>
const std::string T_ObjectFetcher<ObjectFetcherT>::manifest_path =
  T_ObjectFetcher<ObjectFetcherT>::backend_storage + "/.cvmfspublished";

template <class ObjectFetcherT>
const std::string T_ObjectFetcher<ObjectFetcherT>::temp_directory =
  T_ObjectFetcher<ObjectFetcherT>::sandbox + "/tmp";

template <class ObjectFetcherT>
const shash::Any T_ObjectFetcher<ObjectFetcherT>::previous_history_hash =
  h("200176676aa95c7e3053104c5d9e88f98febc671", shash::kSuffixHistory);



typedef ::testing::Types<
  MockObjectFetcher,
  LocalObjectFetcher<> > ObjectFetcherTypes;
TYPED_TEST_CASE(T_ObjectFetcher, ObjectFetcherTypes);


TYPED_TEST(T_ObjectFetcher, Initialize) {
  UniquePtr<TypeParam> object_fetcher(TestFixture::GetObjectFetcher());
  EXPECT_TRUE (object_fetcher.IsValid());
}


TYPED_TEST(T_ObjectFetcher, FetchManifest) {
  UniquePtr<TypeParam> object_fetcher(TestFixture::GetObjectFetcher());
  ASSERT_TRUE (object_fetcher.IsValid());

  UniquePtr<manifest::Manifest> manifest(object_fetcher->FetchManifest());
  ASSERT_TRUE (manifest.IsValid());

  EXPECT_EQ (MockCatalog::root_hash, manifest->catalog_hash());
  EXPECT_EQ (MockHistory::root_hash, manifest->history());
}


TYPED_TEST(T_ObjectFetcher, FetchHistory) {
  UniquePtr<TypeParam> object_fetcher(TestFixture::GetObjectFetcher());
  ASSERT_TRUE (object_fetcher.IsValid());

  UniquePtr<typename TypeParam::history_t> history(object_fetcher->FetchHistory());
  ASSERT_TRUE (history.IsValid());
  EXPECT_EQ (TestFixture::previous_history_hash, history->previous_revision());
}


TYPED_TEST(T_ObjectFetcher, FetchLegacyHistory) {
  UniquePtr<TypeParam> object_fetcher(TestFixture::GetObjectFetcher());
  ASSERT_TRUE (object_fetcher.IsValid());

  UniquePtr<typename TypeParam::history_t> history(
              object_fetcher->FetchHistory(TestFixture::previous_history_hash));
  ASSERT_TRUE (history.IsValid());
  EXPECT_TRUE (history->previous_revision().IsNull());
}


TYPED_TEST(T_ObjectFetcher, FetchInvalidHistory) {
  UniquePtr<TypeParam> object_fetcher(TestFixture::GetObjectFetcher());
  ASSERT_TRUE (object_fetcher.IsValid());

  UniquePtr<typename TypeParam::history_t> history(
      object_fetcher->FetchHistory(h("400d35465f179a4acacb5fe749e6ce20a0bbdb84",
                                     shash::kSuffixHistory)));
  ASSERT_FALSE (history.IsValid()) << "History found: " << history.weak_ref();
}


