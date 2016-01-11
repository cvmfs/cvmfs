/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <cassert>
#include <map>
#include <string>

#include "../../cvmfs/catalog_traversal.h"
#include "../../cvmfs/garbage_collection/garbage_collector.h"
#include "../../cvmfs/garbage_collection/hash_filter.h"
#include "../../cvmfs/hash.h"
#include "../../cvmfs/manifest.h"
#include "../../cvmfs/prng.h"
#include "../../cvmfs/util.h"
#include "testutil.h"

using swissknife::CatalogTraversal;
using upload::SpoolerDefinition;

typedef CatalogTraversal<MockObjectFetcher>  MockedCatalogTraversal;
typedef MockedCatalogTraversal::Parameters   TraversalParams;

class GC_MockUploader : public AbstractMockUploader<GC_MockUploader> {
 public:
  explicit GC_MockUploader(const SpoolerDefinition &spooler_definition) :
    AbstractMockUploader<GC_MockUploader>(spooler_definition) {}

  upload::UploadStreamHandle* InitStreamedUpload(
                                            const CallbackTN *callback = NULL) {
    return NULL;
  }

  void Upload(upload::UploadStreamHandle  *abstract_handle,
              upload::CharBuffer          *buffer,
              const CallbackTN            *callback = NULL) {
    assert(AbstractMockUploader<GC_MockUploader>::not_implemented);
  }

  void FinalizeStreamedUpload(upload::UploadStreamHandle *abstract_handle,
                              const shash::Any            content_hash,
                              const std::string           hash_suffix) {
    assert(AbstractMockUploader<GC_MockUploader>::not_implemented);
  }

  bool Remove(const shash::Any &hash_to_delete) {
    deleted_hashes.insert(hash_to_delete);
    return true;
  }

  bool HasDeleted(const shash::Any &hash) const {
    return deleted_hashes.find(hash) != deleted_hashes.end();
  }

 public:
  std::set<shash::Any> deleted_hashes;
};

class T_GarbageCollector : public ::testing::Test {
 public:
  static const std::string fqrn;

 public:
  MockCatalog *dummy_catalog_hierarchy;

 protected:
  typedef std::map<std::pair<unsigned int, std::string>, MockCatalog*>
    RevisionMap;
  typedef GarbageCollector<MockedCatalogTraversal, SimpleHashFilter>
    MyGarbageCollector;
  typedef MyGarbageCollector::Configuration
    GcConfiguration;

 protected:
  void SetUp() {
    dice_.InitLocaltime();
    SetupDummyCatalogs();
    uploader_ = GC_MockUploader::MockConstruct();
  }

  void TearDown() {
    MockCatalog::Reset();
    MockHistory::Reset();
    EXPECT_EQ(0u, MockCatalog::instances);
    ASSERT_NE(static_cast<GC_MockUploader*>(NULL), uploader_);
    uploader_->TearDown();
    delete uploader_;
  }

  FILE *CreateTemporaryFile(std::string *path) const {
    return CreateTempFile(GetCurrentWorkingDirectory() + "/cvmfs_ut_gc",
                          0600, "w+", path);
  }

  GcConfiguration GetStandardGarbageCollectorConfiguration() {
    GcConfiguration config;
    config.keep_history_depth = 1;
    config.dry_run            = false;
    config.uploader           = uploader_;
    config.object_fetcher     = &object_fetcher_;
    return config;
  }

  void SetupDummyCatalogs() {
    /**
     * Dummy catalog hierarchy:
     *
     *  0-0 HEAD
     *   |
     *   |
     *   +----+
     *   |    |
     *  1-0  1-1
     *   |
     *   +
     *   |
     *  2-0
     *
     * Revision time stamps:
     *   1   27.11.1987
     *   2   03.03.2000
     *   3   24.12.2004
     *   4   25.12.2004
     *   5   26.12.2004
     *
     */

    RevisionMap &c = catalogs_;

    //
    // # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    // # REVISION 1
    // # Adds an initial set of files. Some of those files will directly fade
    // # out of existence in the next revision. They are marked with an asterisk
    // #
    //

    c[mp(1, "00")] =
      CreateAndRegisterCatalog("", 1, t(27, 11, 1987), NULL, NULL);
    c[mp(1, "10")] =
      CreateAndRegisterCatalog("/00/10", 1, t(27, 11, 1987) + 50,
                               c[mp(1, "00")], NULL);
    c[mp(1, "11")] =
      CreateAndRegisterCatalog("/00/11", 1, t(27, 11, 1987) + 100,
                               c[mp(1, "00")], NULL);

    c[mp(1, "00")]->AddFile(h("c05b6c2319608d2dd03c0d19dba586682772b953"),
                            1337);  // 1
    c[mp(1, "00")]->AddFile(h("2d8f9f90d6914eb52fed7a0548dd1fbcbea281f1"),
                            42);  // 1
    c[mp(1, "00")]->AddFile(h("20c2e6328f943003254693a66434ff01ebba26f0"),
                            32000);  // 1*
    c[mp(1, "00")]->AddFile(h("219d1ca4c958bd615822f8c125701e73ce379428"),
                            1232);  // 1*
    c[mp(1, "00")]->AddChunk(h("8d02b1f7ca8e6f925e308994da4248b6309293ba",
                             'P'),  3462);  // 1
    c[mp(1, "00")]->AddChunk(h("6eebfa4eb98dfa5657afeb0e15361f31288ad339",
                             'P'),  3462);  // 1

    c[mp(1, "10")]->AddFile(h("213bec88ed6729219d94fc9281893ba93fca2a02"),
                            13424);  // 1
    c[mp(1, "10")]->AddFile(h("1e94ba5dfe746a7e4e55b62bad21666bc9770ce9"),
                            6374);  // 1*
    c[mp(1, "10")]->AddFile(h("8031b9ad81b52cd772db9b1b12d38994fdd9dbe4"),
                            89765);  // 1*

    c[mp(1, "11")]->AddFile(h("915614a7871a0ffc50abde2885a35545023a6a64"),
                            99);  // 1
    c[mp(1, "11")]->AddFile(h("59b63e8478fb7fc02c54a85767c7116573907364"),
                            1240);  // 1
    c[mp(1, "11")]->AddFile(h("c4cbd93ce625b1829a99eeef415f7237ea5d1f02"),
                            0);  // 1

    //
    // # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    // # REVISION 2
    // # Some files from revision 1 will be removed (marked with an asterisk in
    // # the listing for revision 1). We will reuse one of the catalogs.
    // # Additionally there will be some more files added to the listing.
    // #
    //

    c[mp(2, "00")] =
      CreateAndRegisterCatalog("", 2, t(3, 3, 2000), NULL, c[mp(1, "00")]);
    c[mp(2, "10")] =
      CreateAndRegisterCatalog("/00/10", 2, t(3, 3, 2000) + 20,
                               c[mp(2, "00")], c[mp(1, "10")]);
    c[mp(2, "11")] = ReuseCatalog(c[mp(1, "11")], c[mp(2, "00")]);

    c[mp(2, "00")]->AddFile(h("c05b6c2319608d2dd03c0d19dba586682772b953"),
                            1337);  // 1
    c[mp(2, "00")]->AddFile(h("2d8f9f90d6914eb52fed7a0548dd1fbcbea281f1"),
                            42);  // 1
    c[mp(2, "00")]->AddChunk(h("8d02b1f7ca8e6f925e308994da4248b6309293ba",
                             'P'),  3462);  // 1
    c[mp(2, "00")]->AddChunk(h("6eebfa4eb98dfa5657afeb0e15361f31288ad339",
                             'P'),  3462);  // 1

    c[mp(2, "10")]->AddFile(h("213bec88ed6729219d94fc9281893ba93fca2a02"),
                            13424);  // 1
    c[mp(2, "10")]->AddFile(h("09fd3486d370013d859651eb164ec71a3a09f5cb"),
                            87541);  // 2
    c[mp(2, "10")]->AddFile(h("380fe86b4cc68164afd5578eb21a32ab397e6d13"),
                            96);  // 2
    c[mp(2, "10")]->AddFile(h("59b63e8478fb7fc02c54a85767c7116573907364"),
                            1240);  // 1
    c[mp(2, "10")]->AddFile(h("09fd3486d370013d859651eb164ec71a3a09f5cb"),
                            87541);  // 2
    c[mp(2, "10")]->AddFile(h("1a9ef17ae3597bf61d8229dc2bf6ec12ebb42d44"),
                            9865);  // 2

    //
    // # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    // # REVISION 3
    // # This revision does not delete any files available in revision 2 but
    // # adds a couple of more (new) files.
    // #
    //

    c[mp(3, "00")] =
      CreateAndRegisterCatalog("", 3, t(24, 12, 2004), NULL, c[mp(2, "00")]);
    c[mp(3, "10")] =
      CreateAndRegisterCatalog("/00/10", 3, t(24, 12, 2004) +  1,
                               c[mp(3, "00")], c[mp(2, "10")]);
    c[mp(3, "11")] =
      CreateAndRegisterCatalog("/00/11", 3, t(24, 12, 2004) + 30,
                               c[mp(3, "00")], c[mp(2, "11")]);

    c[mp(3, "00")]->AddFile(h("c05b6c2319608d2dd03c0d19dba586682772b953"),
                            1337);  // 1
    c[mp(3, "00")]->AddFile(h("2d8f9f90d6914eb52fed7a0548dd1fbcbea281f1"),
                            42);  // 1*
    c[mp(3, "00")]->AddFile(h("d2068490d25c1bd4ef2f3d3a0568a76046466860"),
                            123);  // 3
    c[mp(3, "00")]->AddFile(h("283144632474a0e553e3b61c1f272257942e7a61"),
                            3457);  // 3
    c[mp(3, "00")]->AddFile(h("2e87adef242bc67cb66fcd61238ad808a7b44aab"),
                            8761);  // 3*

    c[mp(3, "10")]->AddFile(h("213bec88ed6729219d94fc9281893ba93fca2a02"),
                            13424);  // 1
    c[mp(3, "10")]->AddFile(h("09fd3486d370013d859651eb164ec71a3a09f5cb"),
                            87541);  // 2
    c[mp(3, "10")]->AddFile(h("380fe86b4cc68164afd5578eb21a32ab397e6d13"),
                            96);  // 2*
    c[mp(3, "10")]->AddFile(h("7d4d0ec225ebe13839d71c0dc0982567cc810402"),
                            213);  // 3
    c[mp(3, "10")]->AddFile(h("3bf4854891899670727fc8e9c6e454f7e4058454"),
                            1439);  // 3*
    c[mp(3, "10")]->AddFile(h("12ea064b069d98cb9da09219568ff2f8dd7d0a7e"),
                            2);  // 3*
    c[mp(3, "10")]->AddFile(h("bb5a7bbe8410f0268a9b12285b6f1fd26e038023"),
                            415);  // 3
    c[mp(3, "10")]->AddChunk(h("8d02b1f7ca8e6f925e308994da4248b6309293ba",
                             'P'),  3462);  // 1*
    c[mp(3, "10")]->AddChunk(h("6eebfa4eb98dfa5657afeb0e15361f31288ad339",
                             'P'),  3462);  // 1*

    c[mp(3, "11")]->AddFile(h("59b63e8478fb7fc02c54a85767c7116573907364"),
                            1240);  // 1
    c[mp(3, "11")]->AddFile(h("09fd3486d370013d859651eb164ec71a3a09f5cb"),
                            87541);  // 2
    c[mp(3, "11")]->AddFile(h("1a9ef17ae3597bf61d8229dc2bf6ec12ebb42d44"),
                            9865);  // 2*
    c[mp(3, "11")]->AddFile(h("e0862f1d936037eb0c2be7ccf289f5dbf469244b"),
                            152);  // 3

    //
    // # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    // # REVISION 4
    // # We are again removing some old files (marked with an asterisk in
    // # revision 3). Additionally there is a file from revision 1 re-appearing.
    // # Futhermore this revision adds one additional nested catalog.
    // #
    //

    c[mp(4, "00")] =
       CreateAndRegisterCatalog("", 4, t(25, 12, 2004), NULL, c[mp(3, "00")]);
    c[mp(4, "10")] =
      CreateAndRegisterCatalog("/00/10", 4, t(25, 12, 2004) + 12,
                               c[mp(4, "00")], c[mp(3, "10")]);
    c[mp(4, "11")] =
      CreateAndRegisterCatalog("/00/11", 4, t(25, 12, 2004) + 24,
                               c[mp(4, "00")], c[mp(3, "11")]);
    c[mp(4, "20")] =
      CreateAndRegisterCatalog("/00/10/20", 4, t(25, 12, 2004) + 36,
                               c[mp(4, "10")], NULL);

    c[mp(4, "00")]->AddFile(h("c05b6c2319608d2dd03c0d19dba586682772b953"),
                            1337);  // 1
    c[mp(4, "00")]->AddFile(h("d2068490d25c1bd4ef2f3d3a0568a76046466860"),
                            123);  // 3
    c[mp(4, "00")]->AddFile(h("283144632474a0e553e3b61c1f272257942e7a61"),
                            3457);  // 3

    c[mp(4, "10")]->AddFile(h("213bec88ed6729219d94fc9281893ba93fca2a02"),
                            13424);  // 1
    c[mp(4, "10")]->AddFile(h("09fd3486d370013d859651eb164ec71a3a09f5cb"),
                            87541);  // 2
    c[mp(4, "10")]->AddFile(h("7d4d0ec225ebe13839d71c0dc0982567cc810402"),
                            213);  // 3
    c[mp(4, "10")]->AddFile(h("bb5a7bbe8410f0268a9b12285b6f1fd26e038023"),
                            415);  // 3

    c[mp(4, "11")]->AddFile(h("59b63e8478fb7fc02c54a85767c7116573907364"),
                              1240);  // 1
    c[mp(4, "11")]->AddFile(h("09fd3486d370013d859651eb164ec71a3a09f5cb"),
                            87541);  // 2
    c[mp(4, "11")]->AddFile(h("e0862f1d936037eb0c2be7ccf289f5dbf469244b"),
                            152);  // 3
    c[mp(4, "11")]->AddChunk(h("defae1853b929bbbdbc7c6d4e75531273f1ae4cb",
                            'P'),  9999);  // 4
    c[mp(4, "11")]->AddChunk(h("24bf4276fcdbe57e648b82af4e8fece5bd3581c7",
                            'P'),  9991);  // 4
    c[mp(4, "11")]->AddChunk(h("acc4c10cf875861ec8d6744a9ab81cb2abe433b4",
                            'P'),  9992);  // 4
    c[mp(4, "11")]->AddChunk(h("654be8b6938b3fb30be3e9476f3ed26db74e0a9e",
                            'P'),  9993);  // 4
    c[mp(4, "11")]->AddChunk(h("1a17be523120c7d3a7be745ada1658cc74e8507b",
                            'P'),  9994);  // 4

    c[mp(4, "20")]->AddFile(h("8031b9ad81b52cd772db9b1b12d38994fdd9dbe4"),
                            89765);  // 1+
    c[mp(4, "20")]->AddFile(h("18588c597700a7e2d3b4ce91bdf5a947a4ad13fc"),
                            13254);  // 4
    c[mp(4, "20")]->AddFile(h("fea3b5156ebbeddb89c85bc14c8e9caa185c10c7"),
                            4112);  // 4
    c[mp(4, "20")]->AddFile(h("0aceb47a362df1522a69217736617493bef07d5a"),
                            1422);  // 4

    //
    // # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    // # REVISION 5
    // # In the final revision we replace everything by a set of new files. One
    // # file hash is twice in the list (marked by an asterisk).
    // #
    //

    c[mp(5, "00")] =
      CreateAndRegisterCatalog("", 5, t(26, 12, 2004), NULL,
                               c[mp(4, "00")], MockCatalog::root_hash);
    c[mp(5, "10")] =
      CreateAndRegisterCatalog("/00/10", 5, t(26, 12, 2004) + 10,
                               c[mp(5, "00")], c[mp(4, "10")]);
    c[mp(5, "11")] =
      CreateAndRegisterCatalog("/00/11", 5, t(26, 12, 2004) + 20,
                               c[mp(5, "00")], c[mp(4, "11")]);
    c[mp(5, "20")] =
      CreateAndRegisterCatalog("/00/10/20", 5, t(26, 12, 2004) + 30,
                               c[mp(5, "10")], c[mp(4, "20")]);

    c[mp(5, "00")]->AddFile(h("b52945d780f8cc16711d4e670d82499dad99032d"),
                            1331);  // 5
    c[mp(5, "00")]->AddFile(h("d650d325d59ea9ca754f9b37293cd08d0b12584c"),
                            513);  // 5

    c[mp(5, "10")]->AddFile(h("4083d30ba1f72e1dfad4cdbfc60ea3c38bfa600d"),
                            5123);  // 5
    c[mp(5, "10")]->AddFile(h("c308c87d518c86130d9b9d34723b2a7d4e232ce9"),
                            124);  // 5*
    c[mp(5, "10")]->AddFile(h("8967a86ddf51d89aaad5ad0b7f29bdfc7f7aef2a"),
                            1453);  // 5
    c[mp(5, "10")]->AddChunk(h("372e393bb9f5c33440f842b47b8f6aa3ed4f2943",
                            'P'),  8813);  // 5*

    c[mp(5, "11")]->AddFile(h("50c44954ab4348a6a3772ee5bd30ab7a1494c692"),
                            76125);  // 5
    c[mp(5, "11")]->AddFile(h("c308c87d518c86130d9b9d34723b2a7d4e232ce9"),
                            124);  // 5*

    c[mp(5, "20")]->AddFile(h("2dc2b87b8ac840e4fb1cad25c806395c931f7b31"),
                            9816);  // 5
    c[mp(5, "20")]->AddChunk(h("a727b47d99fba5fe196400a3c7bc1738172dff71",
                             'P'),  8811);  // 5
    c[mp(5, "20")]->AddChunk(h("80b59550342b6f5141b42e5b2d58ce453f12d710",
                             'P'),  8812);  // 5
    c[mp(5, "20")]->AddChunk(h("372e393bb9f5c33440f842b47b8f6aa3ed4f2943",
                             'P'),  8813);  // 5*

    //
    // # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    // # REGISTERING OF NAMED SNAPSHOTS
    // # We register revision 2, 4 and 5 as named snapshots in a mocked history.
    // # Furthermore revision 5 is marked as the current trunk (HEAD).
    // #
    //

    const bool writable_history = false;  // MockHistory doesn't care!
    MockHistory *history = new MockHistory(writable_history,
                                           T_GarbageCollector::fqrn);
    MockHistory::RegisterObject(MockHistory::root_hash, history);

    history->BeginTransaction();
    ASSERT_TRUE(history->Insert(history::History::Tag(
                                     "Revision2", c[mp(2, "00")]->hash(),
                                     1337, 2, t(27, 11, 1987),
                                     history::History::kChannelProd,
                                     "this is rev 2")));
    ASSERT_TRUE(history->Insert(history::History::Tag(
                                     "Revision4", c[mp(4, "00")]->hash(),
                                     42, 4, t(11, 9, 2001),
                                     history::History::kChannelProd,
                                     "this is revision 4")));
    ASSERT_TRUE(history->Insert(history::History::Tag(
                                     "Revision5", c[mp(5, "00")]->hash(),
                                     7, 5, t(10, 7, 2014),
                                     history::History::kChannelTrunk,
                                     "this is revision 5 - the newest!")));
    history->CommitTransaction();
  }

  MockCatalog* CreateAndRegisterCatalog(
                  const std::string  &root_path,
                  const unsigned int  revision,
                  const time_t        last_modified,
                  MockCatalog        *parent       = NULL,
                  MockCatalog        *previous     = NULL,
                  const shash::Any   &catalog_hash = shash::Any(shash::kSha1)) {
    // produce a random hash if no catalog has was given
    shash::Any effective_clg_hash = catalog_hash;
    effective_clg_hash.suffix = 'C';
    if (effective_clg_hash.IsNull()) {
      effective_clg_hash.Randomize(&dice_);
    }

    // produce the new catalog with references to it's predecessor and parent
    const bool is_root = (parent == NULL);
    MockCatalog *catalog = new MockCatalog(root_path,
                                           effective_clg_hash,
                                           dice_.Next(10000),
                                           revision,
                                           last_modified,
                                           is_root,
                                           parent,
                                           previous);

    // register the new catalog in the data structures
    MockCatalog::RegisterObject(catalog->hash(), catalog);
    return catalog;
  }

  MockCatalog* ReuseCatalog(MockCatalog *legacy_catalog,
                            MockCatalog *additional_parent_catalog) {
    additional_parent_catalog->RegisterNestedCatalog(legacy_catalog);
    return legacy_catalog;
  }

  MockCatalog* GetCatalog(const unsigned int   revision,
                          const std::string   &clg_index) {
    RevisionMap::const_iterator i = catalogs_.find(mp(revision, clg_index));
    assert(i != catalogs_.end());
    return i->second;
  }

  std::pair<unsigned int, std::string> mp(const unsigned int   revision,
                                          const std::string   &clg_index) {
    return std::make_pair(revision, clg_index);
  }

 private:
  void CheckEmpty(const std::string &str) const {
    ASSERT_FALSE(str.empty());
  }

 protected:
  RevisionMap                  catalogs_;

 private:
  Prng               dice_;
  MockObjectFetcher  object_fetcher_;
  GC_MockUploader   *uploader_;
};

const std::string T_GarbageCollector::fqrn = "test.cern.ch";


TEST_F(T_GarbageCollector, InitializeGarbageCollector) {
  GcConfiguration config = GetStandardGarbageCollectorConfiguration();
  MyGarbageCollector gc(config);
  EXPECT_EQ(0u, gc.preserved_catalog_count());
  EXPECT_EQ(0u, gc.condemned_catalog_count());
}


TEST_F(T_GarbageCollector, KeepEverything) {
  GcConfiguration config = GetStandardGarbageCollectorConfiguration();
  config.keep_history_depth   = TraversalParams::kFullHistory;

  MyGarbageCollector gc(config);
  const bool gc1 = gc.Collect();

  EXPECT_TRUE(gc1);
  EXPECT_EQ(16u, gc.preserved_catalog_count());
  EXPECT_EQ(0u, gc.condemned_catalog_count());
  EXPECT_EQ(0u, gc.condemned_objects_count());
}


TEST_F(T_GarbageCollector, KeepLastRevision) {
  GcConfiguration config = GetStandardGarbageCollectorConfiguration();
  config.keep_history_depth   = 0;  // no history preservation

  MyGarbageCollector gc(config);
  const bool gc1 = gc.Collect();
  EXPECT_TRUE(gc1);
  EXPECT_EQ(11u, gc.preserved_catalog_count());
  EXPECT_EQ(5u, gc.condemned_catalog_count());

  GC_MockUploader *upl = static_cast<GC_MockUploader*>(config.uploader);
  RevisionMap     &c   = catalogs_;
  EXPECT_FALSE(upl->HasDeleted(h("b52945d780f8cc16711d4e670d82499dad99032d")));
  EXPECT_FALSE(upl->HasDeleted(h("d650d325d59ea9ca754f9b37293cd08d0b12584c")));
  EXPECT_FALSE(upl->HasDeleted(h("4083d30ba1f72e1dfad4cdbfc60ea3c38bfa600d")));
  EXPECT_FALSE(upl->HasDeleted(h("c308c87d518c86130d9b9d34723b2a7d4e232ce9")));
  EXPECT_FALSE(upl->HasDeleted(h("8967a86ddf51d89aaad5ad0b7f29bdfc7f7aef2a")));
  EXPECT_FALSE(upl->HasDeleted(h("372e393bb9f5c33440f842b47b8f6aa3ed4f2943")));
  EXPECT_FALSE(upl->HasDeleted(h("50c44954ab4348a6a3772ee5bd30ab7a1494c692")));
  EXPECT_FALSE(upl->HasDeleted(h("2dc2b87b8ac840e4fb1cad25c806395c931f7b31")));
  EXPECT_FALSE(upl->HasDeleted(h("a727b47d99fba5fe196400a3c7bc1738172dff71")));
  EXPECT_FALSE(upl->HasDeleted(h("80b59550342b6f5141b42e5b2d58ce453f12d710")));
  EXPECT_FALSE(
    upl->HasDeleted(h("defae1853b929bbbdbc7c6d4e75531273f1ae4cb", 'P')));
  EXPECT_FALSE(
    upl->HasDeleted(h("24bf4276fcdbe57e648b82af4e8fece5bd3581c7", 'P')));
  EXPECT_FALSE(
    upl->HasDeleted(h("acc4c10cf875861ec8d6744a9ab81cb2abe433b4", 'P')));
  EXPECT_FALSE(
    upl->HasDeleted(h("654be8b6938b3fb30be3e9476f3ed26db74e0a9e", 'P')));
  EXPECT_FALSE(
    upl->HasDeleted(h("1a17be523120c7d3a7be745ada1658cc74e8507b", 'P')));
  EXPECT_FALSE(upl->HasDeleted(h("18588c597700a7e2d3b4ce91bdf5a947a4ad13fc")));
  EXPECT_FALSE(upl->HasDeleted(h("fea3b5156ebbeddb89c85bc14c8e9caa185c10c7")));
  EXPECT_FALSE(upl->HasDeleted(h("0aceb47a362df1522a69217736617493bef07d5a")));
  EXPECT_FALSE(upl->HasDeleted(h("d2068490d25c1bd4ef2f3d3a0568a76046466860")));
  EXPECT_FALSE(upl->HasDeleted(h("283144632474a0e553e3b61c1f272257942e7a61")));
  EXPECT_FALSE(upl->HasDeleted(h("213bec88ed6729219d94fc9281893ba93fca2a02")));
  EXPECT_FALSE(upl->HasDeleted(h("7d4d0ec225ebe13839d71c0dc0982567cc810402")));
  EXPECT_FALSE(upl->HasDeleted(h("bb5a7bbe8410f0268a9b12285b6f1fd26e038023")));
  EXPECT_FALSE(upl->HasDeleted(h("59b63e8478fb7fc02c54a85767c7116573907364")));
  EXPECT_FALSE(upl->HasDeleted(h("09fd3486d370013d859651eb164ec71a3a09f5cb")));
  EXPECT_FALSE(upl->HasDeleted(h("e0862f1d936037eb0c2be7ccf289f5dbf469244b")));
  EXPECT_FALSE(upl->HasDeleted(h("8031b9ad81b52cd772db9b1b12d38994fdd9dbe4")));

  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "20")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(2, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(2, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(2, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "20")]->hash()));

  EXPECT_TRUE(upl->HasDeleted(h("2e87adef242bc67cb66fcd61238ad808a7b44aab")));
  EXPECT_TRUE(upl->HasDeleted(h("3bf4854891899670727fc8e9c6e454f7e4058454")));
  EXPECT_TRUE(upl->HasDeleted(h("12ea064b069d98cb9da09219568ff2f8dd7d0a7e")));
  EXPECT_TRUE(upl->HasDeleted(h("20c2e6328f943003254693a66434ff01ebba26f0")));
  EXPECT_TRUE(upl->HasDeleted(h("219d1ca4c958bd615822f8c125701e73ce379428")));
  EXPECT_TRUE(upl->HasDeleted(c[mp(1, "00")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(1, "10")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(3, "00")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(3, "10")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(3, "11")]->hash()));

  EXPECT_EQ(11u, upl->deleted_hashes.size());

  // TODO(rmeusel): Once history handling is complete, one could delete a named
  // snapshot and check if it is gone after another collection run...
}


TEST_F(T_GarbageCollector, KeepLastThreeRevisions) {
  GcConfiguration config = GetStandardGarbageCollectorConfiguration();
  config.keep_history_depth   = 2;  // preserve two historic revisions

  MyGarbageCollector gc(config);
  const bool gc1 = gc.Collect();
  EXPECT_TRUE(gc1);
  EXPECT_EQ(14u, gc.preserved_catalog_count());
  EXPECT_EQ(2u, gc.condemned_catalog_count());

  GC_MockUploader *upl = static_cast<GC_MockUploader*>(config.uploader);
  RevisionMap     &c   = catalogs_;
  EXPECT_FALSE(upl->HasDeleted(h("c05b6c2319608d2dd03c0d19dba586682772b953")));
  EXPECT_FALSE(upl->HasDeleted(h("2d8f9f90d6914eb52fed7a0548dd1fbcbea281f1")));
  EXPECT_FALSE(upl->HasDeleted(h("d2068490d25c1bd4ef2f3d3a0568a76046466860")));
  EXPECT_FALSE(upl->HasDeleted(h("283144632474a0e553e3b61c1f272257942e7a61")));
  EXPECT_FALSE(upl->HasDeleted(h("2e87adef242bc67cb66fcd61238ad808a7b44aab")));
  EXPECT_FALSE(upl->HasDeleted(h("213bec88ed6729219d94fc9281893ba93fca2a02")));
  EXPECT_FALSE(upl->HasDeleted(h("09fd3486d370013d859651eb164ec71a3a09f5cb")));
  EXPECT_FALSE(upl->HasDeleted(h("380fe86b4cc68164afd5578eb21a32ab397e6d13")));
  EXPECT_FALSE(upl->HasDeleted(h("7d4d0ec225ebe13839d71c0dc0982567cc810402")));
  EXPECT_FALSE(upl->HasDeleted(h("3bf4854891899670727fc8e9c6e454f7e4058454")));
  EXPECT_FALSE(upl->HasDeleted(h("12ea064b069d98cb9da09219568ff2f8dd7d0a7e")));
  EXPECT_FALSE(upl->HasDeleted(h("bb5a7bbe8410f0268a9b12285b6f1fd26e038023")));
  EXPECT_FALSE(upl->HasDeleted(h("8d02b1f7ca8e6f925e308994da4248b6309293ba")));
  EXPECT_FALSE(upl->HasDeleted(h("6eebfa4eb98dfa5657afeb0e15361f31288ad339")));
  EXPECT_FALSE(upl->HasDeleted(h("59b63e8478fb7fc02c54a85767c7116573907364")));
  EXPECT_FALSE(upl->HasDeleted(h("1a9ef17ae3597bf61d8229dc2bf6ec12ebb42d44")));
  EXPECT_FALSE(upl->HasDeleted(h("e0862f1d936037eb0c2be7ccf289f5dbf469244b")));
  EXPECT_FALSE(upl->HasDeleted(h("defae1853b929bbbdbc7c6d4e75531273f1ae4cb")));
  EXPECT_FALSE(upl->HasDeleted(h("24bf4276fcdbe57e648b82af4e8fece5bd3581c7")));
  EXPECT_FALSE(upl->HasDeleted(h("acc4c10cf875861ec8d6744a9ab81cb2abe433b4")));
  EXPECT_FALSE(upl->HasDeleted(h("654be8b6938b3fb30be3e9476f3ed26db74e0a9e")));
  EXPECT_FALSE(upl->HasDeleted(h("1a17be523120c7d3a7be745ada1658cc74e8507b")));
  EXPECT_FALSE(upl->HasDeleted(h("8031b9ad81b52cd772db9b1b12d38994fdd9dbe4")));
  EXPECT_FALSE(upl->HasDeleted(h("18588c597700a7e2d3b4ce91bdf5a947a4ad13fc")));
  EXPECT_FALSE(upl->HasDeleted(h("fea3b5156ebbeddb89c85bc14c8e9caa185c10c7")));
  EXPECT_FALSE(upl->HasDeleted(h("0aceb47a362df1522a69217736617493bef07d5a")));
  EXPECT_FALSE(upl->HasDeleted(h("b52945d780f8cc16711d4e670d82499dad99032d")));
  EXPECT_FALSE(upl->HasDeleted(h("d650d325d59ea9ca754f9b37293cd08d0b12584c")));
  EXPECT_FALSE(upl->HasDeleted(h("4083d30ba1f72e1dfad4cdbfc60ea3c38bfa600d")));
  EXPECT_FALSE(upl->HasDeleted(h("c308c87d518c86130d9b9d34723b2a7d4e232ce9")));
  EXPECT_FALSE(upl->HasDeleted(h("8967a86ddf51d89aaad5ad0b7f29bdfc7f7aef2a")));
  EXPECT_FALSE(upl->HasDeleted(h("372e393bb9f5c33440f842b47b8f6aa3ed4f2943")));
  EXPECT_FALSE(upl->HasDeleted(h("50c44954ab4348a6a3772ee5bd30ab7a1494c692")));
  EXPECT_FALSE(upl->HasDeleted(h("c308c87d518c86130d9b9d34723b2a7d4e232ce9")));
  EXPECT_FALSE(upl->HasDeleted(h("2dc2b87b8ac840e4fb1cad25c806395c931f7b31")));
  EXPECT_FALSE(upl->HasDeleted(h("a727b47d99fba5fe196400a3c7bc1738172dff71")));
  EXPECT_FALSE(upl->HasDeleted(h("80b59550342b6f5141b42e5b2d58ce453f12d710")));
  EXPECT_FALSE(upl->HasDeleted(h("372e393bb9f5c33440f842b47b8f6aa3ed4f2943")));
  EXPECT_FALSE(upl->HasDeleted(c[mp(3, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(3, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(3, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "20")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "20")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(2, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(2, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(2, "11")]->hash()));

  EXPECT_TRUE(upl->HasDeleted(h("20c2e6328f943003254693a66434ff01ebba26f0")));
  EXPECT_TRUE(upl->HasDeleted(h("219d1ca4c958bd615822f8c125701e73ce379428")));
  EXPECT_TRUE(upl->HasDeleted(h("1e94ba5dfe746a7e4e55b62bad21666bc9770ce9")));
  EXPECT_TRUE(upl->HasDeleted(c[mp(1, "00")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(1, "10")]->hash()));

  EXPECT_EQ(5u, upl->deleted_hashes.size());
}


TEST_F(T_GarbageCollector, KeepOnlyNamedSnapshots) {
  GcConfiguration config = GetStandardGarbageCollectorConfiguration();
  config.keep_history_depth = 0;

  MyGarbageCollector gc(config);
  const bool gc1 = gc.Collect();
  EXPECT_TRUE(gc1);
  EXPECT_EQ(11u, gc.preserved_catalog_count());
  EXPECT_EQ(5u, gc.condemned_catalog_count());

  GC_MockUploader *upl = static_cast<GC_MockUploader*>(config.uploader);
  RevisionMap     &c   = catalogs_;

  EXPECT_FALSE(upl->HasDeleted(c[mp(2, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(2, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(1, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(2, "11")]->hash()));  // 1,"11" == 2,"11"
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "20")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "20")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(h("915614a7871a0ffc50abde2885a35545023a6a64")));
  EXPECT_FALSE(upl->HasDeleted(h("c4cbd93ce625b1829a99eeef415f7237ea5d1f02")));

  EXPECT_TRUE(upl->HasDeleted(h("20c2e6328f943003254693a66434ff01ebba26f0")));
  EXPECT_TRUE(upl->HasDeleted(h("219d1ca4c958bd615822f8c125701e73ce379428")));
  EXPECT_TRUE(upl->HasDeleted(h("1e94ba5dfe746a7e4e55b62bad21666bc9770ce9")));
  EXPECT_TRUE(upl->HasDeleted(h("2e87adef242bc67cb66fcd61238ad808a7b44aab")));
  EXPECT_TRUE(upl->HasDeleted(h("3bf4854891899670727fc8e9c6e454f7e4058454")));
  EXPECT_TRUE(upl->HasDeleted(h("12ea064b069d98cb9da09219568ff2f8dd7d0a7e")));
  EXPECT_TRUE(upl->HasDeleted(c[mp(1, "00")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(1, "10")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(3, "00")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(3, "10")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(3, "11")]->hash()));

  EXPECT_EQ(11u, upl->deleted_hashes.size());
}


TEST_F(T_GarbageCollector, KeepNamedSnapshotsWithAlreadySweepedRevisions) {
  GcConfiguration config = GetStandardGarbageCollectorConfiguration();
  config.keep_history_depth = TraversalParams::kFullHistory;
  MyGarbageCollector gc(config);

  GC_MockUploader *upl = static_cast<GC_MockUploader*>(config.uploader);
  RevisionMap     &c   = catalogs_;

  std::set<shash::Any> deleted_catalogs;
  deleted_catalogs.insert(c[mp(1, "00")]->hash());
  deleted_catalogs.insert(c[mp(1, "10")]->hash());
  deleted_catalogs.insert(c[mp(3, "00")]->hash());
  deleted_catalogs.insert(c[mp(3, "10")]->hash());
  deleted_catalogs.insert(c[mp(3, "11")]->hash());
  MockCatalog::s_deleted_objects = &deleted_catalogs;

  EXPECT_FALSE(upl->HasDeleted(c[mp(2, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(2, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(2, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "20")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "20")]->hash()));

  const bool gc1 = gc.Collect();
  EXPECT_TRUE(gc1);
  EXPECT_EQ(11u, gc.preserved_catalog_count());
  EXPECT_EQ(0u, gc.condemned_catalog_count());
}


TEST_F(T_GarbageCollector, UnreachableNestedCatalog) {
  GcConfiguration config = GetStandardGarbageCollectorConfiguration();
  config.keep_history_depth = 1;
  MyGarbageCollector gc(config);

  RevisionMap &c   = catalogs_;

  std::set<shash::Any> deleted_catalogs;
  deleted_catalogs.insert(c[mp(3, "10")]->hash());
  MockCatalog::s_deleted_objects = &deleted_catalogs;

  history::History *history = MockHistory::Get(MockHistory::root_hash);;
  ASSERT_NE(static_cast<history::History*>(NULL), history);
  ASSERT_TRUE(history->Remove("Revision2"));  // remove all named snapshots to
  ASSERT_TRUE(history->Remove("Revision4"));  // allow to delete every catalog
  ASSERT_TRUE(history->Remove("Revision5"));  // revision

  const bool gc1 = gc.Collect();
  EXPECT_TRUE(gc1);

  EXPECT_EQ(8u, gc.preserved_catalog_count());
  // Note: should be 8 but (3,"10") was already gone!
  EXPECT_EQ(7u, gc.condemned_catalog_count());

  GC_MockUploader *upl = static_cast<GC_MockUploader*>(config.uploader);

  // preserved by the garbage collection run
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "20")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "20")]->hash()));

  // deleted by the garbage collection run
  EXPECT_TRUE(upl->HasDeleted(c[mp(1, "00")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(1, "10")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(1, "11")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(2, "00")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(2, "10")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(3, "00")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(3, "11")]->hash()));

  // was gone before (hence not deleted by GC)
  // Note: (3,"00") and (3,"11") are from the same revision and got properly
  //       swept by the garbage collection run (see above)
  EXPECT_FALSE(upl->HasDeleted(c[mp(3, "10")]->hash()));

  EXPECT_FALSE(upl->HasDeleted(h("c05b6c2319608d2dd03c0d19dba586682772b953")));
  EXPECT_FALSE(upl->HasDeleted(h("d2068490d25c1bd4ef2f3d3a0568a76046466860")));
  EXPECT_FALSE(upl->HasDeleted(h("283144632474a0e553e3b61c1f272257942e7a61")));
  EXPECT_FALSE(upl->HasDeleted(h("213bec88ed6729219d94fc9281893ba93fca2a02")));
  EXPECT_FALSE(upl->HasDeleted(h("09fd3486d370013d859651eb164ec71a3a09f5cb")));
  EXPECT_FALSE(upl->HasDeleted(h("7d4d0ec225ebe13839d71c0dc0982567cc810402")));
  EXPECT_FALSE(upl->HasDeleted(h("bb5a7bbe8410f0268a9b12285b6f1fd26e038023")));
  EXPECT_FALSE(upl->HasDeleted(h("59b63e8478fb7fc02c54a85767c7116573907364")));
  EXPECT_FALSE(upl->HasDeleted(h("e0862f1d936037eb0c2be7ccf289f5dbf469244b")));
  EXPECT_FALSE(upl->HasDeleted(h("defae1853b929bbbdbc7c6d4e75531273f1ae4cb")));
  EXPECT_FALSE(upl->HasDeleted(h("24bf4276fcdbe57e648b82af4e8fece5bd3581c7")));
  EXPECT_FALSE(upl->HasDeleted(h("acc4c10cf875861ec8d6744a9ab81cb2abe433b4")));
  EXPECT_FALSE(upl->HasDeleted(h("654be8b6938b3fb30be3e9476f3ed26db74e0a9e")));
  EXPECT_FALSE(upl->HasDeleted(h("1a17be523120c7d3a7be745ada1658cc74e8507b")));
  EXPECT_FALSE(upl->HasDeleted(h("8031b9ad81b52cd772db9b1b12d38994fdd9dbe4")));
  EXPECT_FALSE(upl->HasDeleted(h("18588c597700a7e2d3b4ce91bdf5a947a4ad13fc")));
  EXPECT_FALSE(upl->HasDeleted(h("fea3b5156ebbeddb89c85bc14c8e9caa185c10c7")));
  EXPECT_FALSE(upl->HasDeleted(h("0aceb47a362df1522a69217736617493bef07d5a")));
  EXPECT_FALSE(upl->HasDeleted(h("b52945d780f8cc16711d4e670d82499dad99032d")));
  EXPECT_FALSE(upl->HasDeleted(h("d650d325d59ea9ca754f9b37293cd08d0b12584c")));
  EXPECT_FALSE(upl->HasDeleted(h("4083d30ba1f72e1dfad4cdbfc60ea3c38bfa600d")));
  EXPECT_FALSE(upl->HasDeleted(h("c308c87d518c86130d9b9d34723b2a7d4e232ce9")));
  EXPECT_FALSE(upl->HasDeleted(h("8967a86ddf51d89aaad5ad0b7f29bdfc7f7aef2a")));
  EXPECT_FALSE(upl->HasDeleted(h("372e393bb9f5c33440f842b47b8f6aa3ed4f2943")));
  EXPECT_FALSE(upl->HasDeleted(h("50c44954ab4348a6a3772ee5bd30ab7a1494c692")));
  EXPECT_FALSE(upl->HasDeleted(h("c308c87d518c86130d9b9d34723b2a7d4e232ce9")));
  EXPECT_FALSE(upl->HasDeleted(h("2dc2b87b8ac840e4fb1cad25c806395c931f7b31")));
  EXPECT_FALSE(upl->HasDeleted(h("a727b47d99fba5fe196400a3c7bc1738172dff71")));
  EXPECT_FALSE(upl->HasDeleted(h("80b59550342b6f5141b42e5b2d58ce453f12d710")));
  EXPECT_FALSE(upl->HasDeleted(h("372e393bb9f5c33440f842b47b8f6aa3ed4f2943")));

  // those are only referenced in (3,"10") and should be deleted.
  // However, (3, "10") was gone before GC ran and couldn't be located anymore!
  EXPECT_FALSE(upl->HasDeleted(h("3bf4854891899670727fc8e9c6e454f7e4058454")));
  EXPECT_FALSE(upl->HasDeleted(h("12ea064b069d98cb9da09219568ff2f8dd7d0a7e")));

  EXPECT_TRUE(upl->HasDeleted(h("20c2e6328f943003254693a66434ff01ebba26f0")));
  EXPECT_TRUE(upl->HasDeleted(h("219d1ca4c958bd615822f8c125701e73ce379428")));
  EXPECT_TRUE(upl->HasDeleted(h("1e94ba5dfe746a7e4e55b62bad21666bc9770ce9")));
  EXPECT_TRUE(upl->HasDeleted(h("2d8f9f90d6914eb52fed7a0548dd1fbcbea281f1")));
  EXPECT_TRUE(upl->HasDeleted(h("2e87adef242bc67cb66fcd61238ad808a7b44aab")));
  EXPECT_TRUE(upl->HasDeleted(h("380fe86b4cc68164afd5578eb21a32ab397e6d13")));
  EXPECT_TRUE(upl->HasDeleted(h("8d02b1f7ca8e6f925e308994da4248b6309293ba")));
  EXPECT_TRUE(upl->HasDeleted(h("6eebfa4eb98dfa5657afeb0e15361f31288ad339")));
  EXPECT_TRUE(upl->HasDeleted(h("1a9ef17ae3597bf61d8229dc2bf6ec12ebb42d44")));
  EXPECT_TRUE(upl->HasDeleted(h("915614a7871a0ffc50abde2885a35545023a6a64")));
  EXPECT_TRUE(upl->HasDeleted(h("c4cbd93ce625b1829a99eeef415f7237ea5d1f02")));

  EXPECT_EQ(18u, upl->deleted_hashes.size());
}


TEST_F(T_GarbageCollector, OnTheFlyDeletionOfCatalogs) {
  GcConfiguration config = GetStandardGarbageCollectorConfiguration();
  config.keep_history_depth   = 0;  // no history preservation
  MyGarbageCollector gc(config);

  // wire up std::set<> deleted_hashes in uploader with the MockObjectFetcher
  // to simulate the actual deletion of objects
  RevisionMap     &c   = catalogs_;
  GC_MockUploader *upl = static_cast<GC_MockUploader*>(config.uploader);
  MockCatalog::s_deleted_objects = &upl->deleted_hashes;

  const bool gc1 = gc.Collect();
  EXPECT_TRUE(gc1);

  EXPECT_EQ(11u, gc.preserved_catalog_count());
  EXPECT_EQ(5u, gc.condemned_catalog_count());

  EXPECT_FALSE(upl->HasDeleted(h("b52945d780f8cc16711d4e670d82499dad99032d")));
  EXPECT_FALSE(upl->HasDeleted(h("d650d325d59ea9ca754f9b37293cd08d0b12584c")));
  EXPECT_FALSE(upl->HasDeleted(h("4083d30ba1f72e1dfad4cdbfc60ea3c38bfa600d")));
  EXPECT_FALSE(upl->HasDeleted(h("c308c87d518c86130d9b9d34723b2a7d4e232ce9")));
  EXPECT_FALSE(upl->HasDeleted(h("8967a86ddf51d89aaad5ad0b7f29bdfc7f7aef2a")));
  EXPECT_FALSE(upl->HasDeleted(h("372e393bb9f5c33440f842b47b8f6aa3ed4f2943")));
  EXPECT_FALSE(upl->HasDeleted(h("50c44954ab4348a6a3772ee5bd30ab7a1494c692")));
  EXPECT_FALSE(upl->HasDeleted(h("2dc2b87b8ac840e4fb1cad25c806395c931f7b31")));
  EXPECT_FALSE(upl->HasDeleted(h("a727b47d99fba5fe196400a3c7bc1738172dff71")));
  EXPECT_FALSE(upl->HasDeleted(h("80b59550342b6f5141b42e5b2d58ce453f12d710")));
  EXPECT_FALSE(
    upl->HasDeleted(h("defae1853b929bbbdbc7c6d4e75531273f1ae4cb", 'P')));
  EXPECT_FALSE(
    upl->HasDeleted(h("24bf4276fcdbe57e648b82af4e8fece5bd3581c7", 'P')));
  EXPECT_FALSE(
    upl->HasDeleted(h("acc4c10cf875861ec8d6744a9ab81cb2abe433b4", 'P')));
  EXPECT_FALSE(
    upl->HasDeleted(h("654be8b6938b3fb30be3e9476f3ed26db74e0a9e", 'P')));
  EXPECT_FALSE(
    upl->HasDeleted(h("1a17be523120c7d3a7be745ada1658cc74e8507b", 'P')));
  EXPECT_FALSE(upl->HasDeleted(h("18588c597700a7e2d3b4ce91bdf5a947a4ad13fc")));
  EXPECT_FALSE(upl->HasDeleted(h("fea3b5156ebbeddb89c85bc14c8e9caa185c10c7")));
  EXPECT_FALSE(upl->HasDeleted(h("0aceb47a362df1522a69217736617493bef07d5a")));
  EXPECT_FALSE(upl->HasDeleted(h("d2068490d25c1bd4ef2f3d3a0568a76046466860")));
  EXPECT_FALSE(upl->HasDeleted(h("283144632474a0e553e3b61c1f272257942e7a61")));
  EXPECT_FALSE(upl->HasDeleted(h("213bec88ed6729219d94fc9281893ba93fca2a02")));
  EXPECT_FALSE(upl->HasDeleted(h("7d4d0ec225ebe13839d71c0dc0982567cc810402")));
  EXPECT_FALSE(upl->HasDeleted(h("bb5a7bbe8410f0268a9b12285b6f1fd26e038023")));
  EXPECT_FALSE(upl->HasDeleted(h("59b63e8478fb7fc02c54a85767c7116573907364")));
  EXPECT_FALSE(upl->HasDeleted(h("09fd3486d370013d859651eb164ec71a3a09f5cb")));
  EXPECT_FALSE(upl->HasDeleted(h("e0862f1d936037eb0c2be7ccf289f5dbf469244b")));
  EXPECT_FALSE(upl->HasDeleted(h("8031b9ad81b52cd772db9b1b12d38994fdd9dbe4")));

  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "20")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(2, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(2, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(2, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "20")]->hash()));

  EXPECT_TRUE(upl->HasDeleted(h("2e87adef242bc67cb66fcd61238ad808a7b44aab")));
  EXPECT_TRUE(upl->HasDeleted(h("3bf4854891899670727fc8e9c6e454f7e4058454")));
  EXPECT_TRUE(upl->HasDeleted(h("12ea064b069d98cb9da09219568ff2f8dd7d0a7e")));
  EXPECT_TRUE(upl->HasDeleted(h("20c2e6328f943003254693a66434ff01ebba26f0")));
  EXPECT_TRUE(upl->HasDeleted(h("219d1ca4c958bd615822f8c125701e73ce379428")));
  EXPECT_TRUE(upl->HasDeleted(c[mp(1, "00")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(1, "10")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(3, "00")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(3, "10")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(3, "11")]->hash()));

  EXPECT_EQ(11u, upl->deleted_hashes.size());
}


TEST_F(T_GarbageCollector, KeepRevisionsBasedOnTimestamp) {
  GcConfiguration config = GetStandardGarbageCollectorConfiguration();
  config.keep_history_timestamp = t(24, 12, 2004) - 1;  // just before rev 3
  config.keep_history_depth     = GcConfiguration::kFullHistory;

  MyGarbageCollector gc1(config);
  EXPECT_TRUE(gc1.Collect());
  EXPECT_EQ(14u, gc1.preserved_catalog_count());
  EXPECT_EQ(2u, gc1.condemned_catalog_count());

  GC_MockUploader *upl = static_cast<GC_MockUploader*>(config.uploader);
  RevisionMap     &c   = catalogs_;

  EXPECT_FALSE(upl->HasDeleted(c[mp(2, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(2, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(2, "11")]->hash()));  // same as mp(1,"11")
  EXPECT_FALSE(upl->HasDeleted(c[mp(3, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(3, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(3, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "20")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "20")]->hash()));

  EXPECT_TRUE(upl->HasDeleted(c[mp(1, "00")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(1, "10")]->hash()));

  EXPECT_TRUE(upl->HasDeleted(h("20c2e6328f943003254693a66434ff01ebba26f0")));
  EXPECT_TRUE(upl->HasDeleted(h("219d1ca4c958bd615822f8c125701e73ce379428")));
  EXPECT_TRUE(upl->HasDeleted(h("1e94ba5dfe746a7e4e55b62bad21666bc9770ce9")));

  EXPECT_EQ(5u, upl->deleted_hashes.size());

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

  config.keep_history_timestamp = t(24, 12, 2004);  // just at rev 3
  MyGarbageCollector gc2(config);
  EXPECT_TRUE(gc2.Collect());

  EXPECT_EQ(5u, upl->deleted_hashes.size());
  EXPECT_EQ(14u, gc2.preserved_catalog_count());
  EXPECT_EQ(2u, gc2.condemned_catalog_count());

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

  config.keep_history_timestamp = t(24, 12, 2004) + 1;  // just after rev 3
  MyGarbageCollector gc3(config);
  EXPECT_TRUE(gc3.Collect());

  EXPECT_EQ(14u, gc3.preserved_catalog_count());
  EXPECT_EQ(2u, gc3.condemned_catalog_count());

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

  config.keep_history_timestamp = t(25, 12, 2004) + 1;  // just after rev 4
  MyGarbageCollector gc4(config);
  EXPECT_TRUE(gc4.Collect());

  EXPECT_EQ(11u, gc4.preserved_catalog_count());
  EXPECT_EQ(5u, gc4.condemned_catalog_count());
  EXPECT_EQ(11u, upl->deleted_hashes.size());

  EXPECT_TRUE(upl->HasDeleted(c[mp(1, "00")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(1, "10")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(3, "00")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(3, "10")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(3, "11")]->hash()));

  EXPECT_TRUE(upl->HasDeleted(h("20c2e6328f943003254693a66434ff01ebba26f0")));
  EXPECT_TRUE(upl->HasDeleted(h("219d1ca4c958bd615822f8c125701e73ce379428")));
  EXPECT_TRUE(upl->HasDeleted(h("1e94ba5dfe746a7e4e55b62bad21666bc9770ce9")));
  EXPECT_TRUE(upl->HasDeleted(h("2e87adef242bc67cb66fcd61238ad808a7b44aab")));
  EXPECT_TRUE(upl->HasDeleted(h("3bf4854891899670727fc8e9c6e454f7e4058454")));
  EXPECT_TRUE(upl->HasDeleted(h("12ea064b069d98cb9da09219568ff2f8dd7d0a7e")));

  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "20")]->hash()));

  EXPECT_FALSE(upl->HasDeleted(h("2d8f9f90d6914eb52fed7a0548dd1fbcbea281f1")));
  EXPECT_FALSE(upl->HasDeleted(h("380fe86b4cc68164afd5578eb21a32ab397e6d13")));
  EXPECT_FALSE(
    upl->HasDeleted(h("8d02b1f7ca8e6f925e308994da4248b6309293ba", 'P')));
  EXPECT_FALSE(
    upl->HasDeleted(h("6eebfa4eb98dfa5657afeb0e15361f31288ad339", 'P')));
  EXPECT_FALSE(upl->HasDeleted(h("1a9ef17ae3597bf61d8229dc2bf6ec12ebb42d44")));

  EXPECT_FALSE(upl->HasDeleted(h("c05b6c2319608d2dd03c0d19dba586682772b953")));
  EXPECT_FALSE(upl->HasDeleted(h("d2068490d25c1bd4ef2f3d3a0568a76046466860")));
  EXPECT_FALSE(upl->HasDeleted(h("283144632474a0e553e3b61c1f272257942e7a61")));
  EXPECT_FALSE(upl->HasDeleted(h("213bec88ed6729219d94fc9281893ba93fca2a02")));
  EXPECT_FALSE(upl->HasDeleted(h("09fd3486d370013d859651eb164ec71a3a09f5cb")));
  EXPECT_FALSE(upl->HasDeleted(h("7d4d0ec225ebe13839d71c0dc0982567cc810402")));
  EXPECT_FALSE(upl->HasDeleted(h("bb5a7bbe8410f0268a9b12285b6f1fd26e038023")));
  EXPECT_FALSE(upl->HasDeleted(h("59b63e8478fb7fc02c54a85767c7116573907364")));
  EXPECT_FALSE(upl->HasDeleted(h("09fd3486d370013d859651eb164ec71a3a09f5cb")));
  EXPECT_FALSE(upl->HasDeleted(h("e0862f1d936037eb0c2be7ccf289f5dbf469244b")));
  EXPECT_FALSE(
    upl->HasDeleted(h("defae1853b929bbbdbc7c6d4e75531273f1ae4cb", 'P')));
  EXPECT_FALSE(
    upl->HasDeleted(h("24bf4276fcdbe57e648b82af4e8fece5bd3581c7", 'P')));
  EXPECT_FALSE(
    upl->HasDeleted(h("acc4c10cf875861ec8d6744a9ab81cb2abe433b4", 'P')));
  EXPECT_FALSE(
    upl->HasDeleted(h("654be8b6938b3fb30be3e9476f3ed26db74e0a9e", 'P')));
  EXPECT_FALSE(
    upl->HasDeleted(h("1a17be523120c7d3a7be745ada1658cc74e8507b", 'P')));
  EXPECT_FALSE(upl->HasDeleted(h("8031b9ad81b52cd772db9b1b12d38994fdd9dbe4")));
  EXPECT_FALSE(upl->HasDeleted(h("18588c597700a7e2d3b4ce91bdf5a947a4ad13fc")));
  EXPECT_FALSE(upl->HasDeleted(h("fea3b5156ebbeddb89c85bc14c8e9caa185c10c7")));
  EXPECT_FALSE(upl->HasDeleted(h("0aceb47a362df1522a69217736617493bef07d5a")));

  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "20")]->hash()));

  EXPECT_FALSE(upl->HasDeleted(h("b52945d780f8cc16711d4e670d82499dad99032d")));
  EXPECT_FALSE(upl->HasDeleted(h("d650d325d59ea9ca754f9b37293cd08d0b12584c")));
  EXPECT_FALSE(upl->HasDeleted(h("4083d30ba1f72e1dfad4cdbfc60ea3c38bfa600d")));
  EXPECT_FALSE(upl->HasDeleted(h("c308c87d518c86130d9b9d34723b2a7d4e232ce9")));
  EXPECT_FALSE(upl->HasDeleted(h("8967a86ddf51d89aaad5ad0b7f29bdfc7f7aef2a")));
  EXPECT_FALSE(
    upl->HasDeleted(h("372e393bb9f5c33440f842b47b8f6aa3ed4f2943", 'P')));
  EXPECT_FALSE(upl->HasDeleted(h("50c44954ab4348a6a3772ee5bd30ab7a1494c692")));
  EXPECT_FALSE(upl->HasDeleted(h("c308c87d518c86130d9b9d34723b2a7d4e232ce9")));
  EXPECT_FALSE(upl->HasDeleted(h("2dc2b87b8ac840e4fb1cad25c806395c931f7b31")));
  EXPECT_FALSE(
    upl->HasDeleted(h("a727b47d99fba5fe196400a3c7bc1738172dff71", 'P')));
  EXPECT_FALSE(
    upl->HasDeleted(h("80b59550342b6f5141b42e5b2d58ce453f12d710", 'P')));
  EXPECT_FALSE(
    upl->HasDeleted(h("372e393bb9f5c33440f842b47b8f6aa3ed4f2943", 'P')));

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

  history::History *history = MockHistory::Get(MockHistory::root_hash);;
  ASSERT_NE(static_cast<history::History*>(NULL), history);
  ASSERT_TRUE(history->Remove("Revision4"));  // make Revision4 deletable

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

  config.keep_history_timestamp = t(26, 12, 2004) - 1;  // just before rev 5
  MyGarbageCollector gc5(config);
  EXPECT_TRUE(gc5.Collect());

  EXPECT_EQ(11u, gc5.preserved_catalog_count());
  EXPECT_EQ(5u, gc5.condemned_catalog_count());
  EXPECT_EQ(11u, upl->deleted_hashes.size());

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

  config.keep_history_timestamp = t(26, 12, 2004);  // just at rev 5
  MyGarbageCollector gc6(config);
  EXPECT_TRUE(gc6.Collect());

  EXPECT_EQ(11u, gc6.preserved_catalog_count());
  EXPECT_EQ(5u, gc6.condemned_catalog_count());
  EXPECT_EQ(11u, upl->deleted_hashes.size());

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

  config.keep_history_timestamp = t(26, 12, 2004) + 1;  // just after rev 5
  MyGarbageCollector gc7(config);
  EXPECT_TRUE(gc7.Collect());

  EXPECT_EQ(7u, gc7.preserved_catalog_count());
  EXPECT_EQ(9u, gc7.condemned_catalog_count());
  EXPECT_EQ(29u, upl->deleted_hashes.size());

  EXPECT_TRUE(upl->HasDeleted(c[mp(4, "00")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(4, "10")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(4, "11")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(4, "20")]->hash()));

  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "20")]->hash()));

  EXPECT_FALSE(upl->HasDeleted(c[mp(2, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(2, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(2, "11")]->hash()));
}


TEST_F(T_GarbageCollector, KeepOnlyFutureRevisions) {
  // checks what happens if a future time stamp was given
  GcConfiguration config = GetStandardGarbageCollectorConfiguration();
  config.keep_history_timestamp = t(1, 1, 2014);
  config.keep_history_depth     = GcConfiguration::kFullHistory;

  // remove all named snapshots (GC can potentially delete everything)
  history::History *history = MockHistory::Get(MockHistory::root_hash);;
  ASSERT_NE(static_cast<history::History*>(NULL), history);
  ASSERT_TRUE(history->Remove("Revision2"));
  ASSERT_TRUE(history->Remove("Revision4"));
  ASSERT_TRUE(history->Remove("Revision5"));

  MyGarbageCollector gc1(config);
  EXPECT_TRUE(gc1.Collect());

  GC_MockUploader *upl = static_cast<GC_MockUploader*>(config.uploader);
  RevisionMap     &c   = catalogs_;

  EXPECT_TRUE(upl->HasDeleted(c[mp(1, "00")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(1, "10")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(1, "11")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(2, "00")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(2, "10")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(2, "11")]->hash()));  // same as mp(1, "11")
  EXPECT_TRUE(upl->HasDeleted(c[mp(3, "00")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(3, "10")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(3, "11")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(4, "00")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(4, "10")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(4, "11")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(4, "20")]->hash()));
  // timestamp threshold indicates that everything should be deleted. However,
  // the latest revision will always stay!
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "20")]->hash()));
}


TEST_F(T_GarbageCollector, NamedTagsInRecycleBin) {
  GcConfiguration config = GetStandardGarbageCollectorConfiguration();
  config.keep_history_depth = 0;

  // wire up std::set<> deleted_hashes in uploader with the MockObjectFetcher
  // to simulate the actual deletion of objects
  RevisionMap     &c   = catalogs_;
  GC_MockUploader *upl = static_cast<GC_MockUploader*>(config.uploader);
  MockCatalog::s_deleted_objects = &upl->deleted_hashes;

  // run a first garbage collection (leaving only named snapshots)
  MyGarbageCollector gc1(config);
  EXPECT_TRUE(gc1.Collect());

  EXPECT_EQ(11u, gc1.preserved_catalog_count());
  EXPECT_EQ(5u, gc1.condemned_catalog_count());

  EXPECT_FALSE(upl->HasDeleted(c[mp(2, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(2, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(1, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(2, "11")]->hash()));  // 1,"11" == 2,"11"
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "20")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "20")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(h("915614a7871a0ffc50abde2885a35545023a6a64")));
  EXPECT_FALSE(upl->HasDeleted(h("c4cbd93ce625b1829a99eeef415f7237ea5d1f02")));
  EXPECT_FALSE(upl->HasDeleted(h("380fe86b4cc68164afd5578eb21a32ab397e6d13")));
  EXPECT_FALSE(upl->HasDeleted(h("1a9ef17ae3597bf61d8229dc2bf6ec12ebb42d44")));

  EXPECT_TRUE(upl->HasDeleted(h("20c2e6328f943003254693a66434ff01ebba26f0")));
  EXPECT_TRUE(upl->HasDeleted(h("219d1ca4c958bd615822f8c125701e73ce379428")));
  EXPECT_TRUE(upl->HasDeleted(h("1e94ba5dfe746a7e4e55b62bad21666bc9770ce9")));
  EXPECT_TRUE(upl->HasDeleted(h("2e87adef242bc67cb66fcd61238ad808a7b44aab")));
  EXPECT_TRUE(upl->HasDeleted(h("3bf4854891899670727fc8e9c6e454f7e4058454")));
  EXPECT_TRUE(upl->HasDeleted(h("12ea064b069d98cb9da09219568ff2f8dd7d0a7e")));
  EXPECT_TRUE(upl->HasDeleted(c[mp(1, "00")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(1, "10")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(3, "00")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(3, "10")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(3, "11")]->hash()));

  EXPECT_EQ(11u, upl->deleted_hashes.size());

  // delete named tag to produce a catalog revision that is not referenced by
  // standard CVMFS data structures
  history::History *history = MockHistory::Get(MockHistory::root_hash);
  ASSERT_NE(static_cast<history::History*>(NULL), history);
  ASSERT_TRUE(history->Remove("Revision2"));
  EXPECT_EQ(2u, history->GetNumberOfTags());

  // run a second GarbageCollection to remove revision 2
  MyGarbageCollector gc2(config);
  EXPECT_TRUE(gc2.Collect());

  EXPECT_EQ(8u, gc2.preserved_catalog_count());
  EXPECT_EQ(3u, gc2.condemned_catalog_count());

  EXPECT_TRUE(upl->HasDeleted(c[mp(2, "00")]->hash()));
  EXPECT_TRUE(upl->HasDeleted(c[mp(2, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(4, "20")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "00")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "10")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "11")]->hash()));
  EXPECT_FALSE(upl->HasDeleted(c[mp(5, "20")]->hash()));

  EXPECT_TRUE(upl->HasDeleted(h("380fe86b4cc68164afd5578eb21a32ab397e6d13")));
  EXPECT_TRUE(upl->HasDeleted(h("1a9ef17ae3597bf61d8229dc2bf6ec12ebb42d44")));
}


TEST_F(T_GarbageCollector, LogDeletionToFile) {
  string dest_path;
  FILE *deletion_log = CreateTemporaryFile(&dest_path);
  ASSERT_TRUE(deletion_log != NULL);
  UnlinkGuard unlink_guard(dest_path);

  GcConfiguration config = GetStandardGarbageCollectorConfiguration();
  config.keep_history_depth      = 0;             // no history preservation
  config.deleted_objects_logfile = deletion_log;  // log deletion to tmp file

  MyGarbageCollector gc(config);
  const bool gc1 = gc.Collect();
  EXPECT_TRUE(gc1);
  EXPECT_EQ(11u, gc.preserved_catalog_count());
  EXPECT_EQ(5u, gc.condemned_catalog_count());

  RevisionMap     &c   = catalogs_;

  std::vector<shash::Any> preserved_hashes;
  preserved_hashes.push_back(h("b52945d780f8cc16711d4e670d82499dad99032d"));
  preserved_hashes.push_back(h("d650d325d59ea9ca754f9b37293cd08d0b12584c"));
  preserved_hashes.push_back(h("4083d30ba1f72e1dfad4cdbfc60ea3c38bfa600d"));
  preserved_hashes.push_back(h("c308c87d518c86130d9b9d34723b2a7d4e232ce9"));
  preserved_hashes.push_back(h("8967a86ddf51d89aaad5ad0b7f29bdfc7f7aef2a"));
  preserved_hashes.push_back(h("372e393bb9f5c33440f842b47b8f6aa3ed4f2943"));
  preserved_hashes.push_back(h("50c44954ab4348a6a3772ee5bd30ab7a1494c692"));
  preserved_hashes.push_back(h("2dc2b87b8ac840e4fb1cad25c806395c931f7b31"));
  preserved_hashes.push_back(h("a727b47d99fba5fe196400a3c7bc1738172dff71"));
  preserved_hashes.push_back(h("80b59550342b6f5141b42e5b2d58ce453f12d710"));
  preserved_hashes.push_back(
    h("defae1853b929bbbdbc7c6d4e75531273f1ae4cb", 'P'));
  preserved_hashes.push_back(
    h("24bf4276fcdbe57e648b82af4e8fece5bd3581c7", 'P'));
  preserved_hashes.push_back(
    h("acc4c10cf875861ec8d6744a9ab81cb2abe433b4", 'P'));
  preserved_hashes.push_back(
    h("654be8b6938b3fb30be3e9476f3ed26db74e0a9e", 'P'));
  preserved_hashes.push_back(
    h("1a17be523120c7d3a7be745ada1658cc74e8507b", 'P'));
  preserved_hashes.push_back(h("18588c597700a7e2d3b4ce91bdf5a947a4ad13fc"));
  preserved_hashes.push_back(h("fea3b5156ebbeddb89c85bc14c8e9caa185c10c7"));
  preserved_hashes.push_back(h("0aceb47a362df1522a69217736617493bef07d5a"));
  preserved_hashes.push_back(h("d2068490d25c1bd4ef2f3d3a0568a76046466860"));
  preserved_hashes.push_back(h("283144632474a0e553e3b61c1f272257942e7a61"));
  preserved_hashes.push_back(h("213bec88ed6729219d94fc9281893ba93fca2a02"));
  preserved_hashes.push_back(h("7d4d0ec225ebe13839d71c0dc0982567cc810402"));
  preserved_hashes.push_back(h("bb5a7bbe8410f0268a9b12285b6f1fd26e038023"));
  preserved_hashes.push_back(h("59b63e8478fb7fc02c54a85767c7116573907364"));
  preserved_hashes.push_back(h("09fd3486d370013d859651eb164ec71a3a09f5cb"));
  preserved_hashes.push_back(h("e0862f1d936037eb0c2be7ccf289f5dbf469244b"));
  preserved_hashes.push_back(h("8031b9ad81b52cd772db9b1b12d38994fdd9dbe4"));
  preserved_hashes.push_back(c[mp(5, "00")]->hash());
  preserved_hashes.push_back(c[mp(5, "10")]->hash());
  preserved_hashes.push_back(c[mp(5, "11")]->hash());
  preserved_hashes.push_back(c[mp(5, "20")]->hash());
  preserved_hashes.push_back(c[mp(2, "00")]->hash());
  preserved_hashes.push_back(c[mp(2, "10")]->hash());
  preserved_hashes.push_back(c[mp(2, "11")]->hash());
  preserved_hashes.push_back(c[mp(4, "00")]->hash());
  preserved_hashes.push_back(c[mp(4, "10")]->hash());
  preserved_hashes.push_back(c[mp(4, "11")]->hash());
  preserved_hashes.push_back(c[mp(4, "20")]->hash());

  std::vector<shash::Any> deleted_hashes;
  deleted_hashes.push_back(h("2e87adef242bc67cb66fcd61238ad808a7b44aab"));
  deleted_hashes.push_back(h("3bf4854891899670727fc8e9c6e454f7e4058454"));
  deleted_hashes.push_back(h("12ea064b069d98cb9da09219568ff2f8dd7d0a7e"));
  deleted_hashes.push_back(h("20c2e6328f943003254693a66434ff01ebba26f0"));
  deleted_hashes.push_back(h("219d1ca4c958bd615822f8c125701e73ce379428"));
  deleted_hashes.push_back(c[mp(1, "00")]->hash());
  deleted_hashes.push_back(c[mp(1, "10")]->hash());
  deleted_hashes.push_back(c[mp(3, "00")]->hash());
  deleted_hashes.push_back(c[mp(3, "10")]->hash());
  deleted_hashes.push_back(c[mp(3, "11")]->hash());

  std::set<std::string> log_lines;
  std::string log_line;
  ASSERT_EQ(0, fseek(deletion_log, 0, SEEK_SET));
  while (GetLineFile(deletion_log, &log_line)) {
    log_lines.insert(log_line);
  }
  fclose(deletion_log);

  EXPECT_EQ(11u, log_lines.size());

        std::vector<shash::Any>::const_iterator i    = preserved_hashes.begin();
  const std::vector<shash::Any>::const_iterator iend = preserved_hashes.end();
  for (; i != iend; ++i) {
    EXPECT_EQ(0u, log_lines.count(i->ToStringWithSuffix()));
  }

        std::vector<shash::Any>::const_iterator j    = deleted_hashes.begin();
  const std::vector<shash::Any>::const_iterator jend = deleted_hashes.end();
  for (; j != jend; ++j) {
    EXPECT_EQ(1u, log_lines.count(j->ToStringWithSuffix()));
  }
}


/* TODO(rmeusel): re-enable once the 'orphaned named snapshots' problem is
  solved

TEST_F(T_GarbageCollector, FindAndSweepOrphanedNamedSnapshot) {
 GcConfiguration config = GetStandardGarbageCollectorConfiguration();
 MyGarbageCollector gc(config);

 GC_MockUploader *upl = static_cast<GC_MockUploader*>(config.uploader);
 RevisionMap     &c   = catalogs_;

 // wire up std::set<> deleted_hashes in uploader with the MockObjectFetcher
 // to simulate the actual deletion of objects
 MockCatalog::s_deleted_objects = &upl->deleted_hashes;

 const bool gc1 = gc.Collect();
 EXPECT_TRUE(gc1);

 EXPECT_FALSE(upl->HasDeleted(c[mp(5, "00")]->hash()));
 EXPECT_FALSE(upl->HasDeleted(c[mp(5, "10")]->hash()));
 EXPECT_FALSE(upl->HasDeleted(c[mp(5, "11")]->hash()));
 EXPECT_FALSE(upl->HasDeleted(c[mp(5, "20")]->hash()));
 EXPECT_FALSE(upl->HasDeleted(c[mp(4, "00")]->hash()));
 EXPECT_FALSE(upl->HasDeleted(c[mp(4, "10")]->hash()));
 EXPECT_FALSE(upl->HasDeleted(c[mp(4, "11")]->hash()));
 EXPECT_FALSE(upl->HasDeleted(c[mp(4, "20")]->hash()));
 EXPECT_FALSE(upl->HasDeleted(c[mp(2, "00")]->hash()));
 EXPECT_FALSE(upl->HasDeleted(c[mp(2, "10")]->hash()));
 EXPECT_FALSE(upl->HasDeleted(c[mp(2, "11")]->hash()));

 EXPECT_TRUE(upl->HasDeleted(c[mp(3, "00")]->hash()));
 EXPECT_TRUE(upl->HasDeleted(c[mp(3, "10")]->hash()));
 EXPECT_TRUE(upl->HasDeleted(c[mp(3, "11")]->hash()));
 EXPECT_TRUE(upl->HasDeleted(c[mp(1, "00")]->hash()));
 EXPECT_TRUE(upl->HasDeleted(c[mp(1, "10")]->hash()));

 EXPECT_EQ(11u, gc.preserved_catalog_count());

 // mock a history database chain that contains the information of the
deleted
 // snapshot "Revision2" in its recycle bin and remove it entirely from the
 // latest history database
 MockHistory *history         = MockHistory::Get(MockHistory::root_hash);
 MockHistory *old_history     = static_cast<MockHistory*>(history->Clone());
 MockHistory *initial_history = static_cast<MockHistory*>(history->Clone());

 old_history->Remove("Revision2");
 history->Remove("Revision2");
 history->EmptyRecycleBin();

 shash::Any old_history_hash     = h("cb431d5bd49df9ba5f1be54642bb8790477ee7f7",
                                     shash::kSuffixHistory);
 shash::Any initial_history_hash = h("963f943b84c478731329709ff90d64978f7feeb4",
                                     shash::kSuffixHistory);

 history->SetPreviousRevision(old_history_hash);
 old_history->SetPreviousRevision(initial_history_hash);
 MockHistory::RegisterObject(old_history_hash, old_history);
 MockHistory::RegisterObject(initial_history_hash, initial_history);

 // + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + - + -

 MyGarbageCollector new_gc(config);
 const bool gc2 = new_gc.Collect();
 EXPECT_TRUE(gc2);

 EXPECT_FALSE(upl->HasDeleted(c[mp(5, "00")]->hash()));
 EXPECT_FALSE(upl->HasDeleted(c[mp(5, "10")]->hash()));
 EXPECT_FALSE(upl->HasDeleted(c[mp(5, "11")]->hash()));
 EXPECT_FALSE(upl->HasDeleted(c[mp(5, "20")]->hash()));
 EXPECT_FALSE(upl->HasDeleted(c[mp(4, "00")]->hash()));
 EXPECT_FALSE(upl->HasDeleted(c[mp(4, "10")]->hash()));
 EXPECT_FALSE(upl->HasDeleted(c[mp(4, "11")]->hash()));
 EXPECT_FALSE(upl->HasDeleted(c[mp(4, "20")]->hash()));

 EXPECT_TRUE(upl->HasDeleted(c[mp(3, "00")]->hash()));
 EXPECT_TRUE(upl->HasDeleted(c[mp(3, "10")]->hash()));
 EXPECT_TRUE(upl->HasDeleted(c[mp(3, "11")]->hash()));
 EXPECT_TRUE(upl->HasDeleted(c[mp(2, "00")]->hash()));
 EXPECT_TRUE(upl->HasDeleted(c[mp(2, "10")]->hash()));
 EXPECT_TRUE(upl->HasDeleted(c[mp(2, "11")]->hash()));
 EXPECT_TRUE(upl->HasDeleted(c[mp(1, "00")]->hash()));
 EXPECT_TRUE(upl->HasDeleted(c[mp(1, "10")]->hash()));

 EXPECT_EQ(8u, new_gc.preserved_catalog_count());
}
*/
