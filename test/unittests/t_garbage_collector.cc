#include <gtest/gtest.h>
#include <string>
#include <map>
#include <cassert>
#include "../../cvmfs/prng.h"

#include "../../cvmfs/catalog_traversal.h"
#include "../../cvmfs/manifest.h"
#include "../../cvmfs/hash.h"

#include "../../cvmfs/garbage_collection/garbage_detector.h"
#include "../../cvmfs/garbage_collection/hash_filter.h"

#include "testutil.h"

using namespace swissknife;

typedef CatalogTraversal<MockCatalog, MockObjectFetcher> MockedCatalogTraversal;

class T_GarbageCollector : public ::testing::Test {
 public:
  MockCatalog *dummy_catalog_hierarchy;

 protected:
  typedef std::map<std::pair<unsigned int, std::string>, MockCatalog*> RevisionMap;

 protected:
  void SetUp() {
    dice_.InitLocaltime();
    MockCatalog::Reset();
    SetupDummyCatalogs();
  }

  void TearDown() {
    MockCatalog::UnregisterCatalogs();
    EXPECT_EQ (0u, MockCatalog::instances);
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
     */

    RevisionMap &c = catalogs_;

    //
    // # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    // # REVISION 1
    // # Adds an initial set of files. Some of those files will directly fade
    // # out of existence in the next revision. They are marked with an asterisk
    // #
    //

    c[mp(1,"00")] = CreateAndRegisterCatalog("",        1,  NULL,           NULL);
    c[mp(1,"10")] = CreateAndRegisterCatalog("/00/10",  1,  c[mp(1,"10")],  NULL);
    c[mp(1,"11")] = CreateAndRegisterCatalog("/00/11",  1,  c[mp(1,"10")],  NULL);

    c[mp(1,"00")]->AddFile (h("c05b6c2319608d2dd03c0d19dba586682772b953"),  1337); // 1
    c[mp(1,"00")]->AddFile (h("2d8f9f90d6914eb52fed7a0548dd1fbcbea281f1"),    42); // 1
    c[mp(1,"00")]->AddFile (h("20c2e6328f943003254693a66434ff01ebba26f0"), 32000); // 1*
    c[mp(1,"00")]->AddFile (h("219d1ca4c958bd615822f8c125701e73ce379428"),  1232); // 1*
    c[mp(1,"00")]->AddChunk(h("8d02b1f7ca8e6f925e308994da4248b6309293ba"),  3462); // 1
    c[mp(1,"00")]->AddChunk(h("6eebfa4eb98dfa5657afeb0e15361f31288ad339"),  3462); // 1

    c[mp(1,"10")]->AddFile (h("213bec88ed6729219d94fc9281893ba93fca2a02"), 13424); // 1
    c[mp(1,"10")]->AddFile (h("1e94ba5dfe746a7e4e55b62bad21666bc9770ce9"),  6374); // 1*
    c[mp(1,"10")]->AddFile (h("8031b9ad81b52cd772db9b1b12d38994fdd9dbe4"), 89765); // 1*

    c[mp(1,"11")]->AddFile (h("915614a7871a0ffc50abde2885a35545023a6a64"),    99); // 1*
    c[mp(1,"11")]->AddFile (h("59b63e8478fb7fc02c54a85767c7116573907364"),  1240); // 1
    c[mp(1,"11")]->AddFile (h("c4cbd93ce625b1829a99eeef415f7237ea5d1f02"),     0); // 1*

    //
    // # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    // # REVISION 2
    // # Some files from revision 1 will be removed (marked with an asterisk in
    // # the listing for revision 1). Additionally there will be some more files
    // # added to the listing.
    // #
    //

    c[mp(2,"00")] = CreateAndRegisterCatalog("",        2,  NULL,          c[mp(1,"00")]);
    c[mp(2,"10")] = CreateAndRegisterCatalog("/00/10",  2,  c[mp(2,"00")], c[mp(1,"10")]);
    c[mp(2,"11")] = CreateAndRegisterCatalog("/00/11",  2,  c[mp(2,"00")], c[mp(1,"11")]);

    c[mp(2,"00")]->AddFile (h("c05b6c2319608d2dd03c0d19dba586682772b953"),  1337); // 1
    c[mp(2,"00")]->AddFile (h("2d8f9f90d6914eb52fed7a0548dd1fbcbea281f1"),    42); // 1
    c[mp(2,"00")]->AddChunk(h("8d02b1f7ca8e6f925e308994da4248b6309293ba"),  3462); // 1
    c[mp(2,"00")]->AddChunk(h("6eebfa4eb98dfa5657afeb0e15361f31288ad339"),  3462); // 1

    c[mp(2,"10")]->AddFile (h("213bec88ed6729219d94fc9281893ba93fca2a02"), 13424); // 1
    c[mp(2,"10")]->AddFile (h("09fd3486d370013d859651eb164ec71a3a09f5cb"), 87541); // 2
    c[mp(2,"10")]->AddFile (h("380fe86b4cc68164afd5578eb21a32ab397e6d13"),    96); // 2

    c[mp(2,"11")]->AddFile (h("59b63e8478fb7fc02c54a85767c7116573907364"),  1240); // 1
    c[mp(2,"11")]->AddFile (h("09fd3486d370013d859651eb164ec71a3a09f5cb"), 87541); // 2
    c[mp(2,"11")]->AddFile (h("1a9ef17ae3597bf61d8229dc2bf6ec12ebb42d44"),  9865); // 2

    //
    // # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    // # REVISION 3
    // # This revision does not delete any files available in revision 2 but
    // # adds a couple of more (new) files.
    // #
    //

    c[mp(3,"00")] = CreateAndRegisterCatalog("",        3,  NULL,          c[mp(2,"00")]);
    c[mp(3,"10")] = CreateAndRegisterCatalog("/00/10",  3,  c[mp(3,"00")], c[mp(2,"10")]);
    c[mp(3,"11")] = CreateAndRegisterCatalog("/00/11",  3,  c[mp(3,"00")], c[mp(2,"11")]);

    c[mp(3,"00")]->AddFile (h("c05b6c2319608d2dd03c0d19dba586682772b953"),  1337); // 1
    c[mp(3,"00")]->AddFile (h("2d8f9f90d6914eb52fed7a0548dd1fbcbea281f1"),    42); // 1*
    c[mp(3,"00")]->AddFile (h("d2068490d25c1bd4ef2f3d3a0568a76046466860"),   123); // 3
    c[mp(3,"00")]->AddFile (h("283144632474a0e553e3b61c1f272257942e7a61"),  3457); // 3
    c[mp(3,"00")]->AddFile (h("2e87adef242bc67cb66fcd61238ad808a7b44aab"),  8761); // 3*

    c[mp(3,"10")]->AddFile (h("213bec88ed6729219d94fc9281893ba93fca2a02"), 13424); // 1
    c[mp(3,"10")]->AddFile (h("09fd3486d370013d859651eb164ec71a3a09f5cb"), 87541); // 2
    c[mp(3,"10")]->AddFile (h("380fe86b4cc68164afd5578eb21a32ab397e6d13"),    96); // 2*
    c[mp(3,"10")]->AddFile (h("7d4d0ec225ebe13839d71c0dc0982567cc810402"),   213); // 3
    c[mp(3,"10")]->AddFile (h("3bf4854891899670727fc8e9c6e454f7e4058454"),  1439); // 3*
    c[mp(3,"10")]->AddFile (h("12ea064b069d98cb9da09219568ff2f8dd7d0a7e"),     2); // 3*
    c[mp(3,"10")]->AddFile (h("bb5a7bbe8410f0268a9b12285b6f1fd26e038023"),   415); // 3
    c[mp(3,"10")]->AddChunk(h("8d02b1f7ca8e6f925e308994da4248b6309293ba"),  3462); // 1*
    c[mp(3,"10")]->AddChunk(h("6eebfa4eb98dfa5657afeb0e15361f31288ad339"),  3462); // 1*

    c[mp(3,"11")]->AddFile (h("59b63e8478fb7fc02c54a85767c7116573907364"),  1240); // 1
    c[mp(3,"11")]->AddFile (h("09fd3486d370013d859651eb164ec71a3a09f5cb"), 87541); // 2
    c[mp(3,"11")]->AddFile (h("1a9ef17ae3597bf61d8229dc2bf6ec12ebb42d44"),  9865); // 2*
    c[mp(3,"11")]->AddFile (h("e0862f1d936037eb0c2be7ccf289f5dbf469244b"),   152); // 3

    //
    // # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    // # REVISION 4
    // # We are again removing some old files and (marked with an asterisk in
    // # revision 3). Additionally there is a file from revision 1 re-appearing.
    // # Futhermore this revision adds one additional nested catalog.
    // #
    //

    c[mp(4,"00")] = CreateAndRegisterCatalog("",          4,  NULL,          c[mp(3,"00")]);
    c[mp(4,"10")] = CreateAndRegisterCatalog("/00/10",    4,  c[mp(4,"00")], c[mp(3,"10")]);
    c[mp(4,"11")] = CreateAndRegisterCatalog("/00/11",    4,  c[mp(4,"00")], c[mp(3,"11")]);
    c[mp(4,"20")] = CreateAndRegisterCatalog("/00/10/20", 4,  c[mp(4,"10")], NULL);

    c[mp(4,"00")]->AddFile (h("c05b6c2319608d2dd03c0d19dba586682772b953"),  1337); // 1
    c[mp(4,"00")]->AddFile (h("d2068490d25c1bd4ef2f3d3a0568a76046466860"),   123); // 3
    c[mp(4,"00")]->AddFile (h("283144632474a0e553e3b61c1f272257942e7a61"),  3457); // 3

    c[mp(4,"10")]->AddFile (h("213bec88ed6729219d94fc9281893ba93fca2a02"), 13424); // 1
    c[mp(4,"10")]->AddFile (h("09fd3486d370013d859651eb164ec71a3a09f5cb"), 87541); // 2
    c[mp(4,"10")]->AddFile (h("7d4d0ec225ebe13839d71c0dc0982567cc810402"),   213); // 3
    c[mp(4,"10")]->AddFile (h("bb5a7bbe8410f0268a9b12285b6f1fd26e038023"),   415); // 3

    c[mp(4,"11")]->AddFile (h("59b63e8478fb7fc02c54a85767c7116573907364"),  1240); // 1
    c[mp(4,"11")]->AddFile (h("09fd3486d370013d859651eb164ec71a3a09f5cb"), 87541); // 2
    c[mp(4,"11")]->AddFile (h("e0862f1d936037eb0c2be7ccf289f5dbf469244b"),   152); // 3
    c[mp(4,"11")]->AddChunk(h("defae1853b929bbbdbc7c6d4e75531273f1ae4cb"),  9999); // 4
    c[mp(4,"11")]->AddChunk(h("24bf4276fcdbe57e648b82af4e8fece5bd3581c7"),  9991); // 4
    c[mp(4,"11")]->AddChunk(h("acc4c10cf875861ec8d6744a9ab81cb2abe433b4"),  9992); // 4
    c[mp(4,"11")]->AddChunk(h("654be8b6938b3fb30be3e9476f3ed26db74e0a9e"),  9993); // 4
    c[mp(4,"11")]->AddChunk(h("1a17be523120c7d3a7be745ada1658cc74e8507b"),  9994); // 4

    c[mp(4,"20")]->AddFile (h("8031b9ad81b52cd772db9b1b12d38994fdd9dbe4"), 89765); // 1+
    c[mp(4,"20")]->AddFile (h("18588c597700a7e2d3b4ce91bdf5a947a4ad13fc"), 13254); // 4
    c[mp(4,"20")]->AddFile (h("fea3b5156ebbeddb89c85bc14c8e9caa185c10c7"),  4112); // 4
    c[mp(4,"20")]->AddFile (h("0aceb47a362df1522a69217736617493bef07d5a"),  1422); // 4

    //
    // # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    // # REVISION 5
    // # In the final revision we replace everything by a set of new files. One
    // # file hash is twice in the list (marked by an asterisk).
    // #
    //

    c[mp(5,"00")] = CreateAndRegisterCatalog("",          5,  NULL,          c[mp(4,"00")], MockCatalog::root_hash);
    c[mp(5,"10")] = CreateAndRegisterCatalog("/00/10",    5,  c[mp(5,"00")], c[mp(4,"10")]);
    c[mp(5,"11")] = CreateAndRegisterCatalog("/00/11",    5,  c[mp(5,"00")], c[mp(4,"11")]);
    c[mp(5,"20")] = CreateAndRegisterCatalog("/00/10/20", 5,  c[mp(5,"10")], c[mp(4,"20")]);

    c[mp(5,"00")]->AddFile (h("b52945d780f8cc16711d4e670d82499dad99032d"),  1331); // 5
    c[mp(5,"00")]->AddFile (h("d650d325d59ea9ca754f9b37293cd08d0b12584c"),   513); // 5

    c[mp(5,"10")]->AddFile (h("4083d30ba1f72e1dfad4cdbfc60ea3c38bfa600d"),  5123); // 5
    c[mp(5,"10")]->AddFile (h("c308c87d518c86130d9b9d34723b2a7d4e232ce9"),   124); // 5*
    c[mp(5,"10")]->AddFile (h("8967a86ddf51d89aaad5ad0b7f29bdfc7f7aef2a"),  1453); // 5
    c[mp(5,"10")]->AddChunk(h("372e393bb9f5c33440f842b47b8f6aa3ed4f2943"),  8813); // 5*

    c[mp(5,"11")]->AddFile (h("50c44954ab4348a6a3772ee5bd30ab7a1494c692"), 76125); // 5
    c[mp(5,"11")]->AddFile (h("c308c87d518c86130d9b9d34723b2a7d4e232ce9"),   124); // 5*

    c[mp(5,"20")]->AddFile (h("2dc2b87b8ac840e4fb1cad25c806395c931f7b31"),  9816); // 5
    c[mp(5,"20")]->AddChunk(h("a727b47d99fba5fe196400a3c7bc1738172dff71"),  8811); // 5
    c[mp(5,"20")]->AddChunk(h("80b59550342b6f5141b42e5b2d58ce453f12d710"),  8812); // 5
    c[mp(5,"20")]->AddChunk(h("372e393bb9f5c33440f842b47b8f6aa3ed4f2943"),  8813); // 5*
  }

  MockCatalog* CreateAndRegisterCatalog(
                  const std::string  &root_path,
                  const unsigned int  revision,
                  MockCatalog        *parent       = NULL,
                  MockCatalog        *previous     = NULL,
                  const shash::Any   &catalog_hash = shash::Any(shash::kSha1)) {
    // produce a random hash if no catalog has was given
    shash::Any effective_clg_hash = catalog_hash;
    if (effective_clg_hash.IsNull()) {
      effective_clg_hash.Randomize(dice_);
    }

    // produce the new catalog with references to it's predecessor and parent
    const bool is_root = (parent == NULL);
    MockCatalog *catalog = new MockCatalog(root_path,
                                           effective_clg_hash,
                                           dice_.Next(10000),
                                           revision,
                                           is_root,
                                           parent,
                                           previous);

    // register the new catalog in the data structures
    MockCatalog::RegisterCatalog(catalog);
    return catalog;
  }

  MockCatalog* GetCatalog(const unsigned int   revision,
                          const std::string   &clg_index) {
    RevisionMap::const_iterator i = catalogs_.find(mp(revision, clg_index));
    assert (i != catalogs_.end());
    return i->second;
  }

  shash::Any h(const std::string &hash) {
    return shash::Any(shash::kSha1, shash::HexPtr(hash));
  }

  std::pair<unsigned int, std::string> mp(const unsigned int   revision,
                                          const std::string   &clg_index) {
    return std::make_pair(revision, clg_index);
  }

 protected:
  RevisionMap  catalogs_;

 private:
  Prng         dice_;
};


class InstrumentedSimpleHashFilter : public SimpleHashFilter {
 public:
  InstrumentedSimpleHashFilter() :
    SimpleHashFilter(),
    fill_calls(0), contains_calls(0), freeze_calls(0), count_calls(0) {}

  void Fill(const shash::Any &hash) {
    ++fill_calls;
    SimpleHashFilter::Fill(hash);
  }

  bool Contains(const shash::Any &hash) const {
    ++contains_calls;
    return SimpleHashFilter::Contains(hash);
  }

  void Freeze() {
    ++freeze_calls;
    SimpleHashFilter::Freeze();
  }

  size_t Count() const {
    ++count_calls;
    return SimpleHashFilter::Count();
  }

 public:
  mutable unsigned int fill_calls;
  mutable unsigned int contains_calls;
  mutable unsigned int freeze_calls;
  mutable unsigned int count_calls;
};


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_GarbageCollector, Initialize) {
  InstrumentedSimpleHashFilter filter;
  GarbageDetector<MockCatalog, InstrumentedSimpleHashFilter> gc(filter);

  EXPECT_EQ (0u, filter.fill_calls);
  EXPECT_EQ (0u, filter.contains_calls);
  EXPECT_EQ (0u, filter.freeze_calls);
  EXPECT_EQ (0u, filter.count_calls);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_GarbageCollector, PreserveLatestRevision) {
  InstrumentedSimpleHashFilter filter;
  GarbageDetector<MockCatalog, InstrumentedSimpleHashFilter> gc(filter);

  gc.Preserve(GetCatalog(5, "00"));
  gc.Preserve(GetCatalog(5, "10"));
  gc.Preserve(GetCatalog(5, "11"));
  gc.Preserve(GetCatalog(5, "20"));

  EXPECT_EQ (19u, filter.fill_calls);    //  12 data objects
                                         // + 3 nested catalogs
                                         // + 3 catalog self references
                                         // + 1 root catalog
  EXPECT_EQ (0u, filter.contains_calls);
  EXPECT_EQ (0u, filter.freeze_calls);

  gc.FreezePreservation();

  EXPECT_EQ (1u, filter.freeze_calls);
  EXPECT_EQ (14u, filter.Count()); //  12 data objects
                                   // - 2 duplicates
                                   // + 3 nested catalogs
                                   // + 1 root catalog

  EXPECT_TRUE (gc.IsCondemned(h("59b63e8478fb7fc02c54a85767c7116573907364")));
  EXPECT_TRUE (gc.IsCondemned(h("09fd3486d370013d859651eb164ec71a3a09f5cb")));
  EXPECT_TRUE (gc.IsCondemned(h("e0862f1d936037eb0c2be7ccf289f5dbf469244b")));
  EXPECT_TRUE (gc.IsCondemned(h("defae1853b929bbbdbc7c6d4e75531273f1ae4cb")));
  EXPECT_TRUE (gc.IsCondemned(h("24bf4276fcdbe57e648b82af4e8fece5bd3581c7")));
  EXPECT_TRUE (gc.IsCondemned(h("acc4c10cf875861ec8d6744a9ab81cb2abe433b4")));
  EXPECT_TRUE (gc.IsCondemned(h("654be8b6938b3fb30be3e9476f3ed26db74e0a9e")));
  EXPECT_TRUE (gc.IsCondemned(h("1a17be523120c7d3a7be745ada1658cc74e8507b")));

  EXPECT_FALSE(gc.IsCondemned(h("b52945d780f8cc16711d4e670d82499dad99032d")));
  EXPECT_FALSE(gc.IsCondemned(h("d650d325d59ea9ca754f9b37293cd08d0b12584c")));
  EXPECT_FALSE(gc.IsCondemned(h("4083d30ba1f72e1dfad4cdbfc60ea3c38bfa600d")));
  EXPECT_FALSE(gc.IsCondemned(h("8967a86ddf51d89aaad5ad0b7f29bdfc7f7aef2a")));
  EXPECT_FALSE(gc.IsCondemned(h("50c44954ab4348a6a3772ee5bd30ab7a1494c692")));
  EXPECT_FALSE(gc.IsCondemned(h("c308c87d518c86130d9b9d34723b2a7d4e232ce9")));
  EXPECT_FALSE(gc.IsCondemned(h("2dc2b87b8ac840e4fb1cad25c806395c931f7b31")));
  EXPECT_FALSE(gc.IsCondemned(h("a727b47d99fba5fe196400a3c7bc1738172dff71")));
  EXPECT_FALSE(gc.IsCondemned(h("80b59550342b6f5141b42e5b2d58ce453f12d710")));
  EXPECT_FALSE(gc.IsCondemned(h("372e393bb9f5c33440f842b47b8f6aa3ed4f2943")));

  EXPECT_FALSE(gc.IsCondemned(MockCatalog::root_hash));

  EXPECT_EQ (19u, filter.contains_calls);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_GarbageCollector, PreserveLatestTwoRevisions) {
  InstrumentedSimpleHashFilter filter;
  GarbageDetector<MockCatalog, InstrumentedSimpleHashFilter> gc(filter);

  gc.Preserve(GetCatalog(5, "00"));
  gc.Preserve(GetCatalog(5, "10"));
  gc.Preserve(GetCatalog(5, "11"));
  gc.Preserve(GetCatalog(5, "20"));
  gc.Preserve(GetCatalog(4, "00"));
  gc.Preserve(GetCatalog(4, "10"));
  gc.Preserve(GetCatalog(4, "11"));
  gc.Preserve(GetCatalog(4, "20"));

  EXPECT_EQ (45u, filter.fill_calls);    //   12 data objects (revision 5)
                                         // + 19 data objects (revision 4)
                                         // +  6 nested catalogs (rev 4 + 5)
                                         // +  6 catalog self references
                                         // +  2 root catalogs
  EXPECT_EQ (0u, filter.contains_calls);
  EXPECT_EQ (0u, filter.freeze_calls);

  gc.FreezePreservation();

  EXPECT_EQ (1u, filter.freeze_calls);
  EXPECT_EQ (36u, filter.Count()); //   12 data objects (revision 5)
                                   // + 19 data objects (revision 4)
                                   // -  2 duplicates
                                   // +  6 nested catalogs
                                   // +  1 root catalog

  RevisionMap &c = catalogs_;
  EXPECT_FALSE(gc.IsCondemned(h("c05b6c2319608d2dd03c0d19dba586682772b953")));
  EXPECT_FALSE(gc.IsCondemned(h("d2068490d25c1bd4ef2f3d3a0568a76046466860")));
  EXPECT_FALSE(gc.IsCondemned(h("283144632474a0e553e3b61c1f272257942e7a61")));
  EXPECT_FALSE(gc.IsCondemned(h("213bec88ed6729219d94fc9281893ba93fca2a02")));
  EXPECT_FALSE(gc.IsCondemned(h("09fd3486d370013d859651eb164ec71a3a09f5cb")));
  EXPECT_FALSE(gc.IsCondemned(h("7d4d0ec225ebe13839d71c0dc0982567cc810402")));
  EXPECT_FALSE(gc.IsCondemned(h("bb5a7bbe8410f0268a9b12285b6f1fd26e038023")));
  EXPECT_FALSE(gc.IsCondemned(h("59b63e8478fb7fc02c54a85767c7116573907364")));
  EXPECT_FALSE(gc.IsCondemned(h("e0862f1d936037eb0c2be7ccf289f5dbf469244b")));
  EXPECT_FALSE(gc.IsCondemned(h("defae1853b929bbbdbc7c6d4e75531273f1ae4cb")));
  EXPECT_FALSE(gc.IsCondemned(h("24bf4276fcdbe57e648b82af4e8fece5bd3581c7")));
  EXPECT_FALSE(gc.IsCondemned(h("acc4c10cf875861ec8d6744a9ab81cb2abe433b4")));
  EXPECT_FALSE(gc.IsCondemned(h("654be8b6938b3fb30be3e9476f3ed26db74e0a9e")));
  EXPECT_FALSE(gc.IsCondemned(h("1a17be523120c7d3a7be745ada1658cc74e8507b")));
  EXPECT_FALSE(gc.IsCondemned(h("8031b9ad81b52cd772db9b1b12d38994fdd9dbe4")));
  EXPECT_FALSE(gc.IsCondemned(h("18588c597700a7e2d3b4ce91bdf5a947a4ad13fc")));
  EXPECT_FALSE(gc.IsCondemned(h("fea3b5156ebbeddb89c85bc14c8e9caa185c10c7")));
  EXPECT_FALSE(gc.IsCondemned(h("0aceb47a362df1522a69217736617493bef07d5a")));
  EXPECT_FALSE(gc.IsCondemned(h("b52945d780f8cc16711d4e670d82499dad99032d")));
  EXPECT_FALSE(gc.IsCondemned(h("d650d325d59ea9ca754f9b37293cd08d0b12584c")));
  EXPECT_FALSE(gc.IsCondemned(h("4083d30ba1f72e1dfad4cdbfc60ea3c38bfa600d")));
  EXPECT_FALSE(gc.IsCondemned(h("c308c87d518c86130d9b9d34723b2a7d4e232ce9")));
  EXPECT_FALSE(gc.IsCondemned(h("8967a86ddf51d89aaad5ad0b7f29bdfc7f7aef2a")));
  EXPECT_FALSE(gc.IsCondemned(h("372e393bb9f5c33440f842b47b8f6aa3ed4f2943")));
  EXPECT_FALSE(gc.IsCondemned(h("50c44954ab4348a6a3772ee5bd30ab7a1494c692")));
  EXPECT_FALSE(gc.IsCondemned(h("2dc2b87b8ac840e4fb1cad25c806395c931f7b31")));
  EXPECT_FALSE(gc.IsCondemned(h("a727b47d99fba5fe196400a3c7bc1738172dff71")));
  EXPECT_FALSE(gc.IsCondemned(h("80b59550342b6f5141b42e5b2d58ce453f12d710")));
  EXPECT_FALSE(gc.IsCondemned(c[mp(4,"00")]->catalog_hash()));
  EXPECT_FALSE(gc.IsCondemned(c[mp(4,"10")]->catalog_hash()));
  EXPECT_FALSE(gc.IsCondemned(c[mp(4,"11")]->catalog_hash()));
  EXPECT_FALSE(gc.IsCondemned(c[mp(4,"20")]->catalog_hash()));
  EXPECT_FALSE(gc.IsCondemned(c[mp(5,"00")]->catalog_hash()));
  EXPECT_FALSE(gc.IsCondemned(c[mp(5,"10")]->catalog_hash()));
  EXPECT_FALSE(gc.IsCondemned(c[mp(5,"11")]->catalog_hash()));
  EXPECT_FALSE(gc.IsCondemned(c[mp(5,"20")]->catalog_hash()));

  EXPECT_EQ (36u, filter.contains_calls);

  EXPECT_TRUE (gc.IsCondemned(h("2d8f9f90d6914eb52fed7a0548dd1fbcbea281f1")));
  EXPECT_TRUE (gc.IsCondemned(h("2e87adef242bc67cb66fcd61238ad808a7b44aab")));
  EXPECT_TRUE (gc.IsCondemned(h("380fe86b4cc68164afd5578eb21a32ab397e6d13")));
  EXPECT_TRUE (gc.IsCondemned(h("3bf4854891899670727fc8e9c6e454f7e4058454")));
  EXPECT_TRUE (gc.IsCondemned(h("12ea064b069d98cb9da09219568ff2f8dd7d0a7e")));
  EXPECT_TRUE (gc.IsCondemned(h("8d02b1f7ca8e6f925e308994da4248b6309293ba")));
  EXPECT_TRUE (gc.IsCondemned(h("6eebfa4eb98dfa5657afeb0e15361f31288ad339")));
  EXPECT_TRUE (gc.IsCondemned(h("1a9ef17ae3597bf61d8229dc2bf6ec12ebb42d44")));
  EXPECT_TRUE (gc.IsCondemned(c[mp(1,"00")]->catalog_hash()));
  EXPECT_TRUE (gc.IsCondemned(c[mp(2,"00")]->catalog_hash()));
  EXPECT_TRUE (gc.IsCondemned(c[mp(3,"00")]->catalog_hash()));

  EXPECT_EQ (47u, filter.contains_calls);
}
