#include <gtest/gtest.h>
#include <string>
#include <map>
#include <cassert>
#include "../../cvmfs/prng.h"

#include "../../cvmfs/catalog_traversal.h"
#include "../../cvmfs/manifest.h"
#include "../../cvmfs/hash.h"

#include "testutil.h"

using namespace swissknife;

typedef CatalogTraversal<MockCatalog, MockObjectFetcher> MockedCatalogTraversal;

class T_GarbageCollector : public ::testing::Test {
 public:
  MockCatalog *dummy_catalog_hierarchy;

 protected:

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

    typedef MockCatalog MC;

    //
    // # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    // # REVISION 1
    // # Adds an initial set of files. Some of those files will directly fade
    // # out of existence in the next revision. They are marked with an asterisk
    // #
    //

    MC *_1_00 = CreateAndRegisterCatalog("",          1,    NULL,     NULL);
    MC *_1_10 = CreateAndRegisterCatalog("/00/10",    1,    _1_00,    NULL);
    MC *_1_11 = CreateAndRegisterCatalog("/00/11",    1,    _1_00,    NULL);

    _1_00->AddFile (h("c05b6c2319608d2dd03c0d19dba586682772b953"),  1337); // 1
    _1_00->AddFile (h("2d8f9f90d6914eb52fed7a0548dd1fbcbea281f1"),    42); // 1
    _1_00->AddFile (h("20c2e6328f943003254693a66434ff01ebba26f0"), 32000); // 1*
    _1_00->AddFile (h("219d1ca4c958bd615822f8c125701e73ce379428"),  1232); // 1*
    _1_00->AddChunk(h("8d02b1f7ca8e6f925e308994da4248b6309293ba"),  3462); // 1
    _1_00->AddChunk(h("6eebfa4eb98dfa5657afeb0e15361f31288ad339"),  3462); // 1

    _1_10->AddFile(h("213bec88ed6729219d94fc9281893ba93fca2a02"), 13424); // 1
    _1_10->AddFile(h("1e94ba5dfe746a7e4e55b62bad21666bc9770ce9"),  6374); // 1*
    _1_10->AddFile(h("8031b9ad81b52cd772db9b1b12d38994fdd9dbe4"), 89765); // 1*

    _1_11->AddFile(h("915614a7871a0ffc50abde2885a35545023a6a64"),    99); // 1*
    _1_11->AddFile(h("59b63e8478fb7fc02c54a85767c7116573907364"),  1240); // 1
    _1_11->AddFile(h("c4cbd93ce625b1829a99eeef415f7237ea5d1f02"),     0); // 1*

    //
    // # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    // # REVISION 2
    // # Some files from revision 1 will be removed (marked with an asterisk in
    // # the listing for revision 1). Additionally there will be some more files
    // # added to the listing.
    // #
    //

    MC *_2_00 = CreateAndRegisterCatalog("",          2,    NULL,     _1_00);
    MC *_2_10 = CreateAndRegisterCatalog("/00/10",    2,    _2_00,    _1_10);
    MC *_2_11 = CreateAndRegisterCatalog("/00/11",    2,    _2_00,    _1_11);

    _2_00->AddFile (h("c05b6c2319608d2dd03c0d19dba586682772b953"),  1337); // 1
    _2_00->AddFile (h("2d8f9f90d6914eb52fed7a0548dd1fbcbea281f1"),    42); // 1
    _2_00->AddChunk(h("8d02b1f7ca8e6f925e308994da4248b6309293ba"),  3462); // 1
    _2_00->AddChunk(h("6eebfa4eb98dfa5657afeb0e15361f31288ad339"),  3462); // 1

    _2_10->AddFile(h("213bec88ed6729219d94fc9281893ba93fca2a02"), 13424); // 1
    _2_10->AddFile(h("09fd3486d370013d859651eb164ec71a3a09f5cb"), 87541); // 2
    _2_10->AddFile(h("380fe86b4cc68164afd5578eb21a32ab397e6d13"),    96); // 2

    _2_11->AddFile(h("59b63e8478fb7fc02c54a85767c7116573907364"),  1240); // 1
    _2_11->AddFile(h("09fd3486d370013d859651eb164ec71a3a09f5cb"), 87541); // 2
    _2_11->AddFile(h("1a9ef17ae3597bf61d8229dc2bf6ec12ebb42d44"),  9865); // 2

    //
    // # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    // # REVISION 3
    // # This revision does not delete any files available in revision 2 but
    // # adds a couple of more (new) files.
    // #
    //

    MC *_3_00 = CreateAndRegisterCatalog("",          3,    NULL,     _2_00);
    MC *_3_10 = CreateAndRegisterCatalog("/00/10",    3,    _3_00,    _2_10);
    MC *_3_11 = CreateAndRegisterCatalog("/00/11",    3,    _3_00,    _2_11);

    _3_00->AddFile(h("c05b6c2319608d2dd03c0d19dba586682772b953"),  1337); // 1
    _3_00->AddFile(h("2d8f9f90d6914eb52fed7a0548dd1fbcbea281f1"),    42); // 1*
    _3_00->AddFile(h("d2068490d25c1bd4ef2f3d3a0568a76046466860"),   123); // 3
    _3_00->AddFile(h("283144632474a0e553e3b61c1f272257942e7a61"),  3457); // 3
    _3_00->AddFile(h("2e87adef242bc67cb66fcd61238ad808a7b44aab"),  8761); // 3*

    _3_10->AddFile (h("213bec88ed6729219d94fc9281893ba93fca2a02"), 13424); // 1
    _3_10->AddFile (h("09fd3486d370013d859651eb164ec71a3a09f5cb"), 87541); // 2
    _3_10->AddFile (h("380fe86b4cc68164afd5578eb21a32ab397e6d13"),    96); // 2*
    _3_10->AddFile (h("7d4d0ec225ebe13839d71c0dc0982567cc810402"),   213); // 3
    _3_10->AddFile (h("3bf4854891899670727fc8e9c6e454f7e4058454"),  1439); // 3*
    _3_10->AddFile (h("12ea064b069d98cb9da09219568ff2f8dd7d0a7e"),     2); // 3*
    _3_10->AddFile (h("bb5a7bbe8410f0268a9b12285b6f1fd26e038023"),   415); // 3
    _3_10->AddChunk(h("8d02b1f7ca8e6f925e308994da4248b6309293ba"),  3462); // 1*
    _3_10->AddChunk(h("6eebfa4eb98dfa5657afeb0e15361f31288ad339"),  3462); // 1*

    _3_11->AddFile(h("59b63e8478fb7fc02c54a85767c7116573907364"),  1240); // 1
    _3_11->AddFile(h("09fd3486d370013d859651eb164ec71a3a09f5cb"), 87541); // 2
    _3_11->AddFile(h("1a9ef17ae3597bf61d8229dc2bf6ec12ebb42d44"),  9865); // 2*
    _3_11->AddFile(h("e0862f1d936037eb0c2be7ccf289f5dbf469244b"),   152); // 3

    //
    // # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    // # REVISION 4
    // # We are again removing some old files and (marked with an asterisk in
    // # revision 3). Additionally there is a file from revision 1 re-appearing.
    // # Futhermore this revision adds one additional nested catalog.
    // #
    //

    MC *_4_00 = CreateAndRegisterCatalog("",          4,    NULL,     _3_00);
    MC *_4_10 = CreateAndRegisterCatalog("/00/10",    4,    _4_00,    _3_10);
    MC *_4_11 = CreateAndRegisterCatalog("/00/11",    4,    _4_00,    _3_11);
    MC *_4_20 = CreateAndRegisterCatalog("/00/10/20", 4,    _4_10,    NULL);

    _4_00->AddFile(h("c05b6c2319608d2dd03c0d19dba586682772b953"),  1337); // 1
    _4_00->AddFile(h("d2068490d25c1bd4ef2f3d3a0568a76046466860"),   123); // 3
    _4_00->AddFile(h("283144632474a0e553e3b61c1f272257942e7a61"),  3457); // 3

    _4_10->AddFile(h("213bec88ed6729219d94fc9281893ba93fca2a02"), 13424); // 1
    _4_10->AddFile(h("09fd3486d370013d859651eb164ec71a3a09f5cb"), 87541); // 2
    _4_10->AddFile(h("7d4d0ec225ebe13839d71c0dc0982567cc810402"),   213); // 3
    _4_10->AddFile(h("bb5a7bbe8410f0268a9b12285b6f1fd26e038023"),   415); // 3

    _4_11->AddFile (h("59b63e8478fb7fc02c54a85767c7116573907364"),  1240); // 1
    _4_11->AddFile (h("09fd3486d370013d859651eb164ec71a3a09f5cb"), 87541); // 2
    _4_11->AddFile (h("e0862f1d936037eb0c2be7ccf289f5dbf469244b"),   152); // 3
    _4_11->AddChunk(h("defae1853b929bbbdbc7c6d4e75531273f1ae4cb"),  9999); // 4
    _4_11->AddChunk(h("24bf4276fcdbe57e648b82af4e8fece5bd3581c7"),  9991); // 4
    _4_11->AddChunk(h("acc4c10cf875861ec8d6744a9ab81cb2abe433b4"),  9992); // 4
    _4_11->AddChunk(h("654be8b6938b3fb30be3e9476f3ed26db74e0a9e"),  9993); // 4
    _4_11->AddChunk(h("1a17be523120c7d3a7be745ada1658cc74e8507b"),  9994); // 4

    _4_20->AddFile(h("8031b9ad81b52cd772db9b1b12d38994fdd9dbe4"), 89765); // 1+
    _4_20->AddFile(h("18588c597700a7e2d3b4ce91bdf5a947a4ad13fc"), 13254); // 4
    _4_20->AddFile(h("fea3b5156ebbeddb89c85bc14c8e9caa185c10c7"),  4112); // 4
    _4_20->AddFile(h("0aceb47a362df1522a69217736617493bef07d5a"),  1422); // 4

    //
    // # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    // # REVISION 5
    // # In the final revision we replace everything by a set of new files. One
    // # file hash is twice in the list (marked by an asterisk).
    // #
    //

    MC *_5_00 = CreateAndRegisterCatalog("",          5,    NULL,     _4_00, MockCatalog::root_hash);
    MC *_5_10 = CreateAndRegisterCatalog("/00/10",    5,    _5_00,    _4_10);
    MC *_5_11 = CreateAndRegisterCatalog("/00/11",    5,    _5_00,    _4_11);
    MC *_5_20 = CreateAndRegisterCatalog("/00/10/20", 5,    _5_10,    _4_20);

    _5_00->AddFile(h("b52945d780f8cc16711d4e670d82499dad99032d"),  1331); // 5
    _5_00->AddFile(h("d650d325d59ea9ca754f9b37293cd08d0b12584c"),   513); // 5

    _5_10->AddFile (h("4083d30ba1f72e1dfad4cdbfc60ea3c38bfa600d"),  5123); // 5
    _5_10->AddFile (h("c308c87d518c86130d9b9d34723b2a7d4e232ce9"),   124); // 5*
    _5_10->AddFile (h("8967a86ddf51d89aaad5ad0b7f29bdfc7f7aef2a"),  1453); // 5
    _5_10->AddChunk(h("372e393bb9f5c33440f842b47b8f6aa3ed4f2943"),  8813); // 5*

    _5_11->AddFile(h("50c44954ab4348a6a3772ee5bd30ab7a1494c692"), 76125); // 5
    _5_11->AddFile(h("c308c87d518c86130d9b9d34723b2a7d4e232ce9"),   124); // 5*

    _5_20->AddFile (h("2dc2b87b8ac840e4fb1cad25c806395c931f7b31"),  9816); // 5
    _5_20->AddChunk(h("a727b47d99fba5fe196400a3c7bc1738172dff71"),  8811); // 5
    _5_20->AddChunk(h("80b59550342b6f5141b42e5b2d58ce453f12d710"),  8812); // 5
    _5_20->AddChunk(h("372e393bb9f5c33440f842b47b8f6aa3ed4f2943"),  8813); // 5*
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

  shash::Any h(const std::string &hash) {
    return shash::Any(shash::kSha1, shash::HexPtr(hash));
  }

 private:
  Prng dice_;
};
