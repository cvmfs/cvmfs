/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <cassert>
#include <map>
#include <string>

#include "../../cvmfs/catalog_traversal.h"
#include "../../cvmfs/hash.h"
#include "../../cvmfs/manifest.h"
#include "../../cvmfs/prng.h"
#include "testutil.h"

using swissknife::CatalogTraversal;

typedef CatalogTraversal<MockObjectFetcher>   MockedCatalogTraversal;
typedef MockedCatalogTraversal::Parameters    TraversalParams;
typedef std::pair<unsigned int, std::string>  CatalogIdentifier;
typedef std::vector<CatalogIdentifier>        CatalogIdentifiers;

class T_CatalogTraversal : public ::testing::Test {
 public:
  static const std::string fqrn;

 public:
  catalog::MockCatalog *dummy_catalog_hierarchy;

 protected:
  typedef std::map<std::string, catalog::MockCatalog*>    CatalogPathMap;
  typedef std::map<unsigned int, CatalogPathMap>          RevisionMap;

  struct RootCatalogInfo {
    RootCatalogInfo() : timestamp(0) {}
    RootCatalogInfo(const shash::Any &hash, const time_t timestamp) :
      catalog_hash(hash),
      timestamp(timestamp) {}

    shash::Any  catalog_hash;
    time_t      timestamp;
  };

  typedef std::map<unsigned int, RootCatalogInfo> RootCatalogMap;

  const unsigned int max_revision;
  const unsigned int initial_catalog_instances;

 public:
  T_CatalogTraversal() :
    dummy_catalog_hierarchy(NULL),
    max_revision(6),
    initial_catalog_instances(42) /* depends on max_revision */ {}

 protected:
  void SetUp() {
    dice_.InitLocaltime();
    SetupDummyCatalogs();
    EXPECT_EQ(initial_catalog_instances, catalog::MockCatalog::instances);
  }

  void TearDown() {
    catalog::MockCatalog::Reset();
    MockHistory::Reset();
    EXPECT_EQ(0u, catalog::MockCatalog::instances);
  }

  TraversalParams GetBasicTraversalParams() {
    TraversalParams params;
    params.object_fetcher = &object_fetcher_;
    return params;
  }

  void CheckVisitedCatalogs(const CatalogIdentifiers &expected,
                            const CatalogIdentifiers &observed,
                            const bool                check_counts = true) {
    if (check_counts) {
      EXPECT_EQ(expected.size(), observed.size());
    }
    typedef CatalogIdentifiers::const_iterator itr;

    itr i    = expected.begin();
    itr iend = expected.end();
    for (; i != iend; ++i) {
      bool found = false;

      itr j    = observed.begin();
      itr jend = observed.end();
      for (; j != jend; ++j) {
        if (*i == *j) {
          found = true;
          break;
        }
      }

      EXPECT_TRUE(found) << "didn't find catalog: " << i->second << " "
                          << "(revision: " << i->first << ")";
    }
  }

  void CheckCatalogSequence(const CatalogIdentifiers &expected,
                            const CatalogIdentifiers &observed) {
    ASSERT_EQ(expected.size(), observed.size());

    for (unsigned int _i = 0; _i < expected.size(); ++_i) {
      EXPECT_EQ(expected[_i], observed[_i])
        << "traversing order changed (idx: " << _i << ")" << std::endl
        << "found:    "
        << observed[_i].first << " " << observed[_i].second << std::endl
        << "expected: "
        << expected[_i].first << " " << expected[_i].second << std::endl;
    }
  }

  catalog::MockCatalog* GetCatalog(const unsigned int  revision,
                          const std::string  &path) {
    RevisionMap::const_iterator rev_itr = revisions_.find(revision);
    if (rev_itr == revisions_.end()) {
      return NULL;
    }

    CatalogPathMap::const_iterator catalog_itr = rev_itr->second.find(path);
    if (catalog_itr == rev_itr->second.end()) {
      return NULL;
    }

    return catalog_itr->second;
  }

  shash::Any GetRootHash(const unsigned int revision) const {
    RootCatalogMap::const_iterator i = root_catalogs_.find(revision);
    assert(i != root_catalogs_.end());
    return i->second.catalog_hash;
  }

  time_t GetRootTimestamp(const unsigned int revision) const {
    RootCatalogMap::const_iterator i = root_catalogs_.find(revision);
    assert(i != root_catalogs_.end());
    return i->second.timestamp;
  }

 private:
  void CheckEmpty(const std::string &str) const {
    ASSERT_FALSE(str.empty());
  }

  CatalogPathMap& GetCatalogTree(const unsigned int revision) {
    RevisionMap::iterator rev_itr = revisions_.find(revision);
    assert(rev_itr != revisions_.end());
    return rev_itr->second;
  }

  catalog::MockCatalog* GetRevisionHead(const unsigned int revision) {
    CatalogPathMap &catalogs = GetCatalogTree(revision);
    CatalogPathMap::iterator catalogs_itr = catalogs.find("");
    assert(catalogs_itr != catalogs.end());
    assert(catalogs_itr->second->revision() == revision);
    assert(catalogs_itr->second->IsRoot());
    return catalogs_itr->second;
  }

  catalog::MockCatalog* GetBranchHead(const std::string   &root_path,
                                      const unsigned int   revision) {
    CatalogPathMap &catalogs = GetCatalogTree(revision);
    CatalogPathMap::iterator catalogs_itr = catalogs.find(root_path);
    assert(catalogs_itr != catalogs.end());
    assert(catalogs_itr->second->revision()  == revision);
    assert(catalogs_itr->second->root_path() == root_path);
    return catalogs_itr->second;
  }

  void SetupDummyCatalogs() {
    /**
     * Dummy catalog hierarchy:
     *
     *  0-0 HEAD
     *   |
     *   +-------------------+---------------+---------------+
     *   |                   |               |               |
     *  1-0                 1-1             1-2             1-3
     *   |                   |               |               |
     *   +-------------+     +----+----+     +----+----+     +----+
     *   |             |     |    |    |     |    |    |     |    |
     *  2-0           2-1   2-2  2-3  2-4   2-5  2-6  2-7   2-8  2-9
     *   |                   |                    |
     *   +----+----+         +-----+              +-----+-----+-----+
     *   |    |    |         |     |              |     |     |     |
     *  3-0  3-1  3-2       3-3   3-4            3-5   3-6   3-7   3-8
     *   |                         |
     *   |                         +-----+-----+
     *   |                         |     |     |
     *  4-0                       4-1   4-2   4-3
     *
     * Parts of the hierarchy are created multiple times in order to get some
     * historic catalogs. To simplify the creation the history looks like so:
     *                                                                # catalogs  timestamp    root catalog hash
     *    Revision 1:   - only the root catalog (0-0)                       1     27.11.1987   d01c7fa072d3957ea5dd323f79fa435b33375c06
     *    Revision 2:   - adds branch 1-0                                   8     24.12.2004   ffee2bf068f3c793efa6ca0fa3bddb066541903b
     *    Revision 3:   - adds branch 1-1                                  17     06.03.2009   c9e011bbf7529d25c958bc0f948eefef79e991cd
     *    Revision 4:   - adds branch 1-2 and branch 1-1 is recreated      25     18.07.2010   eec5694dfe5f2055a358acfb4fda7748c896df24
     *    Revision 5:   - adds branch 1-3                                  28     16.11.2014   3c726334c98537e92c8b92b76852f77e3a425be9
     *    Revision 6:   - removes branch 1-0                               21     17.11.2014   catalog::MockCatalog::root_hash
     *
     */

    RootCatalogMap root_catalogs;
    root_catalogs[1] =
      RootCatalogInfo(h("d01c7fa072d3957ea5dd323f79fa435b33375c06", 'C'),
                      t(27, 11, 1987));
    root_catalogs[2] =
      RootCatalogInfo(h("ffee2bf068f3c793efa6ca0fa3bddb066541903b", 'C'),
                      t(24, 12, 2004));
    root_catalogs[3] =
      RootCatalogInfo(h("c9e011bbf7529d25c958bc0f948eefef79e991cd", 'C'),
                      t(06, 03, 2009));
    root_catalogs[4] =
      RootCatalogInfo(h("eec5694dfe5f2055a358acfb4fda7748c896df24", 'C'),
                      t(18, 07, 2010));
    root_catalogs[5] =
      RootCatalogInfo(h("3c726334c98537e92c8b92b76852f77e3a425be9", 'C'),
                      t(16, 11, 2014));
    root_catalogs[6] =
      RootCatalogInfo(catalog::MockCatalog::root_hash, t(17, 11, 2014));
    root_catalogs_ = root_catalogs;

    for (unsigned int r = 1; r <= max_revision; ++r) {
      MakeRevision(r);
    }

    const bool writable_history = false;  // MockHistory doesn't care!
    MockHistory *history = new MockHistory(writable_history,
                                           T_CatalogTraversal::fqrn);
    MockHistory::RegisterObject(MockHistory::root_hash, history);

    history->BeginTransaction();
    EXPECT_TRUE(history->Insert(history::History::Tag(
      "Revision2", root_catalogs[2].catalog_hash, 1337,
      2, root_catalogs[2].timestamp, history::History::kChannelProd,
      "this is revision 2")));
    EXPECT_TRUE(history->Insert(history::History::Tag(
      "Revision5", root_catalogs[5].catalog_hash, 42,
      5, root_catalogs[5].timestamp, history::History::kChannelProd,
      "this is revision 5")));
    EXPECT_TRUE(history->Insert(history::History::Tag(
      "Revision6", root_catalogs[6].catalog_hash, 7,
      6, root_catalogs[6].timestamp, history::History::kChannelTrunk,
      "this is revision 6 - the newest!")));
    history->CommitTransaction();
  }

  void MakeRevision(const unsigned int revision) {
    // sanity checks
    RevisionMap::const_iterator rev_itr = revisions_.find(revision);
    ASSERT_EQ(revisions_.end(), rev_itr);
    ASSERT_LE(1u, revision);
    ASSERT_GE(max_revision, revision);

    // create map for new catalog tree
    revisions_[revision] = CatalogPathMap();

    // create the root catalog
    catalog::MockCatalog *root_catalog =
      CreateAndRegisterCatalog("", revision, GetRootTimestamp(revision), NULL,
                               GetRootHash(revision));

    // create the catalog hierarchy depending on the revision
    switch (revision) {
      case 1:
        // NOOP
        break;
      case 2:
        MakeBranch("/00/10", revision);
        break;
      case 3:
        MakeBranch("/00/11", revision);
        root_catalog->RegisterNestedCatalog(GetBranchHead("/00/10", 2));
        break;
      case 4:
        MakeBranch("/00/12", revision);
        MakeBranch("/00/11", revision);
        root_catalog->RegisterNestedCatalog(GetBranchHead("/00/10", 2));
        break;
      case 5:
        MakeBranch("/00/13", revision);
        root_catalog->RegisterNestedCatalog(GetBranchHead("/00/10", 2));
        root_catalog->RegisterNestedCatalog(GetBranchHead("/00/11", 4));
        root_catalog->RegisterNestedCatalog(GetBranchHead("/00/12", 4));
        break;
      case 6:
        root_catalog->RegisterNestedCatalog(GetBranchHead("/00/11", 4));
        root_catalog->RegisterNestedCatalog(GetBranchHead("/00/12", 4));
        root_catalog->RegisterNestedCatalog(GetBranchHead("/00/13", 5));
        dummy_catalog_hierarchy = root_catalog;  // sets current repo HEAD
        break;
      default:
        FAIL() << "hit revision: " << revision;
    }
  }

  void MakeBranch(const std::string &branch, const unsigned int revision) {
    catalog::MockCatalog   *revision_root = GetRevisionHead(revision);
    const time_t   ts            = GetRootTimestamp(revision);

    if (branch == "/00/10") {
      catalog::MockCatalog *_10 =
        CreateAndRegisterCatalog("/00/10", revision, ts + 1, revision_root);
      catalog::MockCatalog *_20 =
        CreateAndRegisterCatalog("/00/10/20", revision, ts + 2, _10);
      CreateAndRegisterCatalog("/00/10/21", revision, ts + 3, _10);
      catalog::MockCatalog *_30 =
        CreateAndRegisterCatalog("/00/10/20/30", revision, ts +  4, _20);
      CreateAndRegisterCatalog("/00/10/20/31", revision, ts + 5, _20);
      CreateAndRegisterCatalog("/00/10/20/32", revision, ts + 6, _20);
      CreateAndRegisterCatalog("/00/10/20/30/40", revision, ts + 7, _30);
    } else if (branch == "/00/11") {
      catalog::MockCatalog *_11 =
        CreateAndRegisterCatalog("/00/11", revision, ts + 8, revision_root);
      catalog::MockCatalog *_22 =
        CreateAndRegisterCatalog("/00/11/22", revision, ts + 9, _11);
      CreateAndRegisterCatalog("/00/11/23", revision, ts + 10, _11);
      CreateAndRegisterCatalog("/00/11/24", revision, ts + 11, _11);
      CreateAndRegisterCatalog("/00/11/22/33", revision, ts + 12, _22);
      catalog::MockCatalog *_34 =
        CreateAndRegisterCatalog("/00/11/22/34", revision, ts + 13, _22);
      CreateAndRegisterCatalog("/00/11/22/34/41", revision, ts + 14, _34);
      CreateAndRegisterCatalog("/00/11/22/34/42", revision, ts + 15, _34);
      CreateAndRegisterCatalog("/00/11/22/34/43", revision, ts + 16, _34);
    } else if (branch == "/00/12") {
      catalog::MockCatalog *_12 =
        CreateAndRegisterCatalog("/00/12", revision, ts + 17, revision_root);
      CreateAndRegisterCatalog("/00/12/25", revision, ts + 28, _12);
      catalog::MockCatalog *_26 =
        CreateAndRegisterCatalog("/00/12/26", revision, ts + 19, _12);
      CreateAndRegisterCatalog("/00/12/27", revision, ts + 20, _12);
      CreateAndRegisterCatalog("/00/12/26/35", revision, ts + 21, _26);
      CreateAndRegisterCatalog("/00/12/26/36", revision, ts + 22, _26);
      CreateAndRegisterCatalog("/00/12/26/37", revision, ts + 23, _26);
      CreateAndRegisterCatalog("/00/12/26/38", revision, ts + 24, _26);
    } else if (branch == "/00/13") {
      catalog::MockCatalog *_13 =
        CreateAndRegisterCatalog("/00/13", revision, ts + 25, revision_root);
      CreateAndRegisterCatalog("/00/13/28", revision, ts + 26, _13);
      CreateAndRegisterCatalog("/00/13/29", revision, ts + 27, _13);
    } else {
      FAIL();
    }
  }

  catalog::MockCatalog* CreateAndRegisterCatalog(
                  const                        std::string  &root_path,
                  const unsigned int           revision,
                  const time_t                 timestamp,
                  catalog::MockCatalog        *parent       = NULL,
                  const shash::Any   &catalog_hash = shash::Any(shash::kSha1)) {
    // produce a random hash if no catalog has was given
    shash::Any effective_clg_hash = catalog_hash;
    effective_clg_hash.set_suffix(shash::kSuffixCatalog);
    if (effective_clg_hash.IsNull()) {
      effective_clg_hash.Randomize(&dice_);
    }

    // get catalog tree for current revision
    CatalogPathMap &catalogs = GetCatalogTree(revision);

    // find previous catalog from the RevisionsMaps (if there is one)
    catalog::MockCatalog *previous_catalog = NULL;
    if (revision > 1) {
      RevisionMap::iterator prev_rev_itr = revisions_.find(revision - 1);
      assert(prev_rev_itr != revisions_.end());
      CatalogPathMap::iterator prev_clg_itr =
        prev_rev_itr->second.find(root_path);
      if (prev_clg_itr != prev_rev_itr->second.end()) {
        previous_catalog = prev_clg_itr->second;
      }
    }

    // produce the new catalog with references to it's predecessor and parent
    const bool is_root = (parent == NULL);
    catalog::MockCatalog *catalog = new catalog::MockCatalog(root_path,
                                                    effective_clg_hash,
                                                    dice_.Next(10000),
                                                    revision,
                                                    timestamp,
                                                    is_root,
                                                    parent,
                                                    previous_catalog);

    // register the new catalog in the data structures
    catalog::MockCatalog::RegisterObject(catalog->hash(), catalog);
    catalogs[root_path] = catalog;
    return catalog;
  }

 private:
  Prng              dice_;
  RootCatalogMap    root_catalogs_;
  RevisionMap       revisions_;

  MockObjectFetcher object_fetcher_;
};

const std::string T_CatalogTraversal::fqrn    = "test.cern.ch";


//------------------------------------------------------------------------------


TEST_F(T_CatalogTraversal, Initialize) {
  TraversalParams params = GetBasicTraversalParams();
  MockedCatalogTraversal traverse(params);
}


//------------------------------------------------------------------------------

CatalogIdentifiers SimpleTraversal_visited_catalogs;
void SimpleTraversalCallback(const MockedCatalogTraversal::CallbackDataTN &data)
{
  SimpleTraversal_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                  data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, SimpleTraversal) {
  SimpleTraversal_visited_catalogs.clear();
  EXPECT_EQ(0u, SimpleTraversal_visited_catalogs.size());

  TraversalParams params = GetBasicTraversalParams();
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&SimpleTraversalCallback);
  const bool t1 = traverse.Traverse();
  EXPECT_TRUE(t1);

  CatalogIdentifiers catalogs;
  catalogs.push_back(std::make_pair(6, ""));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));

  CheckVisitedCatalogs(catalogs, SimpleTraversal_visited_catalogs);
  CheckCatalogSequence(catalogs, SimpleTraversal_visited_catalogs);
}


//------------------------------------------------------------------------------


std::vector<catalog::MockCatalog*> SimpleTraversalNoCloseCallback_visited_catalogs;
void SimpleTraversalNoCloseCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  SimpleTraversalNoCloseCallback_visited_catalogs.push_back(
    const_cast<catalog::MockCatalog*>(data.catalog));
}

TEST_F(T_CatalogTraversal, SimpleTraversalNoClose) {
  SimpleTraversalNoCloseCallback_visited_catalogs.clear();
  EXPECT_EQ(0u, SimpleTraversalNoCloseCallback_visited_catalogs.size());

  TraversalParams params = GetBasicTraversalParams();
  params.no_close = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&SimpleTraversalNoCloseCallback);
  bool t1 = traverse.Traverse();
  EXPECT_TRUE(t1);

  EXPECT_EQ(21u + initial_catalog_instances, catalog::MockCatalog::instances);

  std::vector<catalog::MockCatalog*>::const_iterator i, iend;
  for (i    = SimpleTraversalNoCloseCallback_visited_catalogs.begin(),
       iend = SimpleTraversalNoCloseCallback_visited_catalogs.end();
       i != iend; ++i) {
    delete *i;
  }
  SimpleTraversalNoCloseCallback_visited_catalogs.clear();
}



//------------------------------------------------------------------------------


CatalogIdentifiers ZeroLevelHistoryTraversal_visited_catalogs;
void ZeroLevelHistoryTraversalCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  ZeroLevelHistoryTraversal_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, ZeroLevelHistoryTraversal) {
  ZeroLevelHistoryTraversal_visited_catalogs.clear();
  EXPECT_EQ(0u, ZeroLevelHistoryTraversal_visited_catalogs.size());

  TraversalParams params = GetBasicTraversalParams();
  params.history = 0;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&ZeroLevelHistoryTraversalCallback);
  const bool t1 = traverse.Traverse();
  EXPECT_TRUE(t1);

  CatalogIdentifiers catalogs;
  catalogs.push_back(std::make_pair(6, ""));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));

  CheckVisitedCatalogs(catalogs, ZeroLevelHistoryTraversal_visited_catalogs);
  CheckCatalogSequence(catalogs, ZeroLevelHistoryTraversal_visited_catalogs);
}


//------------------------------------------------------------------------------


CatalogIdentifiers FirstLevelHistoryTraversal_visited_catalogs;
void FirstLevelHistoryTraversalCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  FirstLevelHistoryTraversal_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, FirstLevelHistoryTraversal) {
  FirstLevelHistoryTraversal_visited_catalogs.clear();
  EXPECT_EQ(0u, FirstLevelHistoryTraversal_visited_catalogs.size());

  TraversalParams params = GetBasicTraversalParams();
  params.history = 1;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&FirstLevelHistoryTraversalCallback);
  const bool t1 = traverse.Traverse();
  EXPECT_TRUE(t1);

  CatalogIdentifiers catalogs;
  catalogs.push_back(std::make_pair(6, ""));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(5, ""));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));

  CheckVisitedCatalogs(catalogs, FirstLevelHistoryTraversal_visited_catalogs);
  CheckCatalogSequence(catalogs, FirstLevelHistoryTraversal_visited_catalogs);
}


//------------------------------------------------------------------------------


std::vector<catalog::MockCatalog*> FirstLevelHistoryTraversalNoClose_visited_catalogs;
void FirstLevelHistoryTraversalNoCloseCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  FirstLevelHistoryTraversalNoClose_visited_catalogs.push_back(
    const_cast<catalog::MockCatalog*>(data.catalog));
}

TEST_F(T_CatalogTraversal, FirstLevelHistoryTraversalNoClose) {
  FirstLevelHistoryTraversalNoClose_visited_catalogs.clear();
  EXPECT_EQ(0u, FirstLevelHistoryTraversalNoClose_visited_catalogs.size());

  TraversalParams params = GetBasicTraversalParams();
  params.history  = 1;
  params.no_close = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&FirstLevelHistoryTraversalNoCloseCallback);
  const bool t1 = traverse.Traverse();
  EXPECT_TRUE(t1);

  EXPECT_EQ(49u + initial_catalog_instances, catalog::MockCatalog::instances);

  std::vector<catalog::MockCatalog*>::const_iterator i, iend;
  for (i    = FirstLevelHistoryTraversalNoClose_visited_catalogs.begin(),
       iend = FirstLevelHistoryTraversalNoClose_visited_catalogs.end();
       i != iend; ++i) {
    delete *i;
  }
  FirstLevelHistoryTraversalNoClose_visited_catalogs.clear();
}


//------------------------------------------------------------------------------


CatalogIdentifiers SecondLevelHistoryTraversal_visited_catalogs;
void SecondLevelHistoryTraversalCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  SecondLevelHistoryTraversal_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, SecondLevelHistoryTraversal) {
  SecondLevelHistoryTraversal_visited_catalogs.clear();
  EXPECT_EQ(0u, SecondLevelHistoryTraversal_visited_catalogs.size());

  TraversalParams params = GetBasicTraversalParams();
  params.history = 2;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&SecondLevelHistoryTraversalCallback);
  const bool t1 = traverse.Traverse();
  EXPECT_TRUE(t1);

  CatalogIdentifiers catalogs;
  catalogs.push_back(std::make_pair(6, ""));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(5, ""));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(4, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));

  CheckVisitedCatalogs(catalogs, SecondLevelHistoryTraversal_visited_catalogs);
  CheckCatalogSequence(catalogs, SecondLevelHistoryTraversal_visited_catalogs);
}


//------------------------------------------------------------------------------


CatalogIdentifiers FullHistoryTraversal_visited_catalogs;
void FullHistoryTraversalCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  FullHistoryTraversal_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, FullHistoryTraversal) {
  FullHistoryTraversal_visited_catalogs.clear();
  EXPECT_EQ(0u, FullHistoryTraversal_visited_catalogs.size());

  TraversalParams params = GetBasicTraversalParams();
  params.history = TraversalParams::kFullHistory;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&FullHistoryTraversalCallback);
  const bool t1 = traverse.Traverse();
  EXPECT_TRUE(t1);

  CatalogIdentifiers catalogs;
  catalogs.push_back(std::make_pair(6, ""));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(5, ""));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(4, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(3, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(3, "/00/11"));
  catalogs.push_back(std::make_pair(3, "/00/11/24"));
  catalogs.push_back(std::make_pair(3, "/00/11/23"));
  catalogs.push_back(std::make_pair(3, "/00/11/22"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(2, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(1, ""));

  CheckVisitedCatalogs(catalogs, FullHistoryTraversal_visited_catalogs);
  CheckCatalogSequence(catalogs, FullHistoryTraversal_visited_catalogs);
}


//------------------------------------------------------------------------------


CatalogIdentifiers SecondLevelHistoryTraversalNoRepeat_visited_catalogs;
void SecondLevelHistoryTraversalNoRepeatCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  SecondLevelHistoryTraversalNoRepeat_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, SecondLevelHistoryTraversalNoRepeat) {
  SecondLevelHistoryTraversalNoRepeat_visited_catalogs.clear();
  EXPECT_EQ(0u, SecondLevelHistoryTraversalNoRepeat_visited_catalogs.size());

  TraversalParams params = GetBasicTraversalParams();
  params.history           = 2;
  params.no_repeat_history = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&SecondLevelHistoryTraversalNoRepeatCallback);
  const bool t1 = traverse.Traverse();
  EXPECT_TRUE(t1);

  CatalogIdentifiers catalogs;
  catalogs.push_back(std::make_pair(6, ""));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(5, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(4, ""));

  CheckVisitedCatalogs(
    catalogs, SecondLevelHistoryTraversalNoRepeat_visited_catalogs);
  CheckCatalogSequence(
    catalogs, SecondLevelHistoryTraversalNoRepeat_visited_catalogs);
}


//------------------------------------------------------------------------------


CatalogIdentifiers FullHistoryTraversalNoRepeat_visited_catalogs;
void FullHistoryTraversalNoRepeatCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  FullHistoryTraversalNoRepeat_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, FullHistoryTraversalNoRepeat) {
  FullHistoryTraversalNoRepeat_visited_catalogs.clear();
  EXPECT_EQ(0u, FullHistoryTraversalNoRepeat_visited_catalogs.size());

  TraversalParams params = GetBasicTraversalParams();
  params.history           = TraversalParams::kFullHistory;
  params.no_repeat_history = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&FullHistoryTraversalNoRepeatCallback);
  const bool t1 = traverse.Traverse();
  EXPECT_TRUE(t1);

  CatalogIdentifiers catalogs;
  catalogs.push_back(std::make_pair(6, ""));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(5, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(4, ""));
  catalogs.push_back(std::make_pair(3, ""));
  catalogs.push_back(std::make_pair(3, "/00/11"));
  catalogs.push_back(std::make_pair(3, "/00/11/24"));
  catalogs.push_back(std::make_pair(3, "/00/11/23"));
  catalogs.push_back(std::make_pair(3, "/00/11/22"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(2, ""));
  catalogs.push_back(std::make_pair(1, ""));

  EXPECT_EQ(initial_catalog_instances,
    FullHistoryTraversalNoRepeat_visited_catalogs.size());

  CheckVisitedCatalogs(
    catalogs, FullHistoryTraversalNoRepeat_visited_catalogs);
  CheckCatalogSequence(
    catalogs, FullHistoryTraversalNoRepeat_visited_catalogs);
}


//------------------------------------------------------------------------------


CatalogIdentifiers MultiTraversal_visited_catalogs;
void MultiTraversalCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  MultiTraversal_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, MultiTraversal) {
  MultiTraversal_visited_catalogs.clear();
  EXPECT_EQ(0u, MultiTraversal_visited_catalogs.size());

  CatalogIdentifiers catalogs;

  TraversalParams params = GetBasicTraversalParams();
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&MultiTraversalCallback);

  const bool t1 = traverse.Traverse(GetRootHash(6));
  EXPECT_TRUE(t1);

  catalogs.push_back(std::make_pair(6, ""));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  CheckVisitedCatalogs(catalogs, MultiTraversal_visited_catalogs);
  CheckCatalogSequence(catalogs, MultiTraversal_visited_catalogs);

  const bool t2 = traverse.Traverse(GetRootHash(4));
  EXPECT_TRUE(t2);

  catalogs.push_back(std::make_pair(4, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  CheckVisitedCatalogs(catalogs, MultiTraversal_visited_catalogs);
  CheckCatalogSequence(catalogs, MultiTraversal_visited_catalogs);

  const bool t3 = traverse.Traverse(GetRootHash(2));
  EXPECT_TRUE(t3);

  catalogs.push_back(std::make_pair(2, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  CheckVisitedCatalogs(catalogs, MultiTraversal_visited_catalogs);
  CheckCatalogSequence(catalogs, MultiTraversal_visited_catalogs);
}


//------------------------------------------------------------------------------


CatalogIdentifiers MultiTraversalNoRepeat_visited_catalogs;
void MultiTraversalNoRepeatCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  MultiTraversalNoRepeat_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, MultiTraversalNoRepeat) {
  MultiTraversalNoRepeat_visited_catalogs.clear();
  EXPECT_EQ(0u, MultiTraversalNoRepeat_visited_catalogs.size());

  CatalogIdentifiers catalogs;

  TraversalParams params = GetBasicTraversalParams();
  params.no_repeat_history = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&MultiTraversalNoRepeatCallback);

  const bool t1 = traverse.Traverse(GetRootHash(6));
  EXPECT_TRUE(t1);

  catalogs.push_back(std::make_pair(6, ""));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  CheckVisitedCatalogs(catalogs, MultiTraversalNoRepeat_visited_catalogs);
  CheckCatalogSequence(catalogs, MultiTraversalNoRepeat_visited_catalogs);

  const bool t2 = traverse.Traverse(GetRootHash(4));
  EXPECT_TRUE(t2);

  catalogs.push_back(std::make_pair(4, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  CheckVisitedCatalogs(catalogs, MultiTraversalNoRepeat_visited_catalogs);
  CheckCatalogSequence(catalogs, MultiTraversalNoRepeat_visited_catalogs);

  const bool t3 = traverse.Traverse(GetRootHash(2));
  EXPECT_TRUE(t3);

  catalogs.push_back(std::make_pair(2, ""));
  CheckVisitedCatalogs(catalogs, MultiTraversalNoRepeat_visited_catalogs);
  CheckCatalogSequence(catalogs, MultiTraversalNoRepeat_visited_catalogs);
}


//------------------------------------------------------------------------------


CatalogIdentifiers MultiTraversalFirstLevelHistory_visited_catalogs;
void MultiTraversalFirstLevelHistoryCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  MultiTraversalFirstLevelHistory_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, MultiTraversalFirstLevelHistory) {
  MultiTraversalFirstLevelHistory_visited_catalogs.clear();
  EXPECT_EQ(0u, MultiTraversalFirstLevelHistory_visited_catalogs.size());

  CatalogIdentifiers catalogs;

  TraversalParams params = GetBasicTraversalParams();
  params.history = 1;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&MultiTraversalFirstLevelHistoryCallback);

  const bool t1 = traverse.Traverse(GetRootHash(6));
  EXPECT_TRUE(t1);

  catalogs.push_back(std::make_pair(6, ""));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(5, ""));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  CheckVisitedCatalogs(catalogs,
                       MultiTraversalFirstLevelHistory_visited_catalogs);
  CheckCatalogSequence(catalogs,
                       MultiTraversalFirstLevelHistory_visited_catalogs);

  const bool t2 = traverse.Traverse(GetRootHash(4));
  EXPECT_TRUE(t2);

  catalogs.push_back(std::make_pair(4, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(3, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(3, "/00/11"));
  catalogs.push_back(std::make_pair(3, "/00/11/24"));
  catalogs.push_back(std::make_pair(3, "/00/11/23"));
  catalogs.push_back(std::make_pair(3, "/00/11/22"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/33"));
  CheckVisitedCatalogs(catalogs,
                       MultiTraversalFirstLevelHistory_visited_catalogs);
  CheckCatalogSequence(catalogs,
                       MultiTraversalFirstLevelHistory_visited_catalogs);

  const bool t3 = traverse.Traverse(GetRootHash(2));
  EXPECT_TRUE(t3);

  catalogs.push_back(std::make_pair(2, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(1, ""));
  CheckVisitedCatalogs(catalogs,
                       MultiTraversalFirstLevelHistory_visited_catalogs);
  CheckCatalogSequence(catalogs,
                       MultiTraversalFirstLevelHistory_visited_catalogs);
}


//------------------------------------------------------------------------------


CatalogIdentifiers MultiTraversalFirstLevelHistoryNoRepeat_visited_catalogs;
void MultiTraversalFirstLevelHistoryNoRepeatCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  MultiTraversalFirstLevelHistoryNoRepeat_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, MultiTraversalFirstLevelHistoryNoRepeat) {
  MultiTraversalFirstLevelHistoryNoRepeat_visited_catalogs.clear();
  EXPECT_EQ(
    0u, MultiTraversalFirstLevelHistoryNoRepeat_visited_catalogs.size());

  CatalogIdentifiers catalogs;

  TraversalParams params = GetBasicTraversalParams();
  params.history           = 1;
  params.no_repeat_history = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&MultiTraversalFirstLevelHistoryNoRepeatCallback);

  const bool t1 = traverse.Traverse(GetRootHash(6));
  EXPECT_TRUE(t1);

  catalogs.push_back(std::make_pair(6, ""));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(5, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  CheckVisitedCatalogs(
    catalogs, MultiTraversalFirstLevelHistoryNoRepeat_visited_catalogs);
  CheckCatalogSequence(
    catalogs, MultiTraversalFirstLevelHistoryNoRepeat_visited_catalogs);

  const bool t2 = traverse.Traverse(GetRootHash(4));
  EXPECT_TRUE(t2);

  catalogs.push_back(std::make_pair(4, ""));
  catalogs.push_back(std::make_pair(3, ""));
  catalogs.push_back(std::make_pair(3, "/00/11"));
  catalogs.push_back(std::make_pair(3, "/00/11/24"));
  catalogs.push_back(std::make_pair(3, "/00/11/23"));
  catalogs.push_back(std::make_pair(3, "/00/11/22"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/33"));
  CheckVisitedCatalogs(
    catalogs, MultiTraversalFirstLevelHistoryNoRepeat_visited_catalogs);
  CheckCatalogSequence(
    catalogs, MultiTraversalFirstLevelHistoryNoRepeat_visited_catalogs);

  const bool t3 = traverse.Traverse(GetRootHash(2));
  EXPECT_TRUE(t3);

  catalogs.push_back(std::make_pair(2, ""));
  catalogs.push_back(std::make_pair(1, ""));
  CheckVisitedCatalogs(
    catalogs, MultiTraversalFirstLevelHistoryNoRepeat_visited_catalogs);
  CheckCatalogSequence(
    catalogs, MultiTraversalFirstLevelHistoryNoRepeat_visited_catalogs);
}


//------------------------------------------------------------------------------


CatalogIdentifiers EmptyTraversePruned_visited_catalogs;
void EmptyTraversePrunedCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  EmptyTraversePruned_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, EmptyTraversePruned) {
  EmptyTraversePruned_visited_catalogs.clear();
  EXPECT_EQ(0u, EmptyTraversePruned_visited_catalogs.size());

  CatalogIdentifiers catalogs;

  TraversalParams params = GetBasicTraversalParams();
  params.history           = 0;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&EmptyTraversePrunedCallback);
  const bool t1 = traverse.TraversePruned();

  EXPECT_FALSE(t1);
  EXPECT_EQ(0u, traverse.pruned_revision_count());
  CheckVisitedCatalogs(catalogs, EmptyTraversePruned_visited_catalogs);
}


//------------------------------------------------------------------------------


CatalogIdentifiers TraversePrunedAfterSimpleTraversal_visited_catalogs;
void TraversePrunedAfterSimpleTraversalCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  TraversePrunedAfterSimpleTraversal_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, TraversePrunedAfterSimpleTraversal) {
  TraversePrunedAfterSimpleTraversal_visited_catalogs.clear();
  EXPECT_EQ(0u, TraversePrunedAfterSimpleTraversal_visited_catalogs.size());

  CatalogIdentifiers catalogs;

  TraversalParams params = GetBasicTraversalParams();
  params.history = 0;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&TraversePrunedAfterSimpleTraversalCallback);

  const bool t1 = traverse.Traverse();
  EXPECT_TRUE(t1);

  catalogs.push_back(std::make_pair(6, ""));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));

  CheckVisitedCatalogs(
    catalogs, TraversePrunedAfterSimpleTraversal_visited_catalogs);
  CheckCatalogSequence(
    catalogs, TraversePrunedAfterSimpleTraversal_visited_catalogs);
  EXPECT_EQ(1u, traverse.pruned_revision_count());

  const bool t2 = traverse.TraversePruned();
  EXPECT_TRUE(t2);

  catalogs.push_back(std::make_pair(5, ""));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(4, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(3, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(3, "/00/11"));
  catalogs.push_back(std::make_pair(3, "/00/11/24"));
  catalogs.push_back(std::make_pair(3, "/00/11/23"));
  catalogs.push_back(std::make_pair(3, "/00/11/22"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(2, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(1, ""));

  CheckVisitedCatalogs(
    catalogs, TraversePrunedAfterSimpleTraversal_visited_catalogs);
  CheckCatalogSequence(
    catalogs, TraversePrunedAfterSimpleTraversal_visited_catalogs);
  EXPECT_EQ(0u, traverse.pruned_revision_count());
}


//------------------------------------------------------------------------------


CatalogIdentifiers TraversePrunedAfterSimpleTraversalNoRepeat_visited_catalogs;
void TraversePrunedAfterSimpleTraversalNoRepeatCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  TraversePrunedAfterSimpleTraversalNoRepeat_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, TraversePrunedAfterSimpleTraversalNoRepeat) {
  TraversePrunedAfterSimpleTraversalNoRepeat_visited_catalogs.clear();
  EXPECT_EQ(
    0u, TraversePrunedAfterSimpleTraversalNoRepeat_visited_catalogs.size());

  CatalogIdentifiers catalogs;

  TraversalParams params = GetBasicTraversalParams();
  params.history           = 0;
  params.no_repeat_history = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(
    &TraversePrunedAfterSimpleTraversalNoRepeatCallback);

  const bool t1 = traverse.Traverse();
  EXPECT_TRUE(t1);

  catalogs.push_back(std::make_pair(6, ""));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));

  CheckVisitedCatalogs(
    catalogs, TraversePrunedAfterSimpleTraversalNoRepeat_visited_catalogs);
  CheckCatalogSequence(
    catalogs, TraversePrunedAfterSimpleTraversalNoRepeat_visited_catalogs);
  EXPECT_EQ(1u, traverse.pruned_revision_count());

  const bool t2 = traverse.TraversePruned();
  EXPECT_TRUE(t2);

  catalogs.push_back(std::make_pair(5, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(4, ""));
  catalogs.push_back(std::make_pair(3, ""));
  catalogs.push_back(std::make_pair(3, "/00/11"));
  catalogs.push_back(std::make_pair(3, "/00/11/24"));
  catalogs.push_back(std::make_pair(3, "/00/11/23"));
  catalogs.push_back(std::make_pair(3, "/00/11/22"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(2, ""));
  catalogs.push_back(std::make_pair(1, ""));

  CheckVisitedCatalogs(
    catalogs, TraversePrunedAfterSimpleTraversalNoRepeat_visited_catalogs);
  CheckCatalogSequence(
    catalogs, TraversePrunedAfterSimpleTraversalNoRepeat_visited_catalogs);
  EXPECT_EQ(0u, traverse.pruned_revision_count());
}


//------------------------------------------------------------------------------


CatalogIdentifiers
  TraversePrunedAfterSecondLevelHistoryTraversalNoRepeat_visited_catalogs;
void TraversePrunedAfterSecondLevelHistoryTraversalNoRepeatCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  TraversePrunedAfterSecondLevelHistoryTraversalNoRepeat_visited_catalogs.
    push_back(std::make_pair(data.catalog->GetRevision(),
              data.catalog->path().ToString()));
}

TEST_F(
  T_CatalogTraversal,
  TraversePrunedAfterSecondLevelHistoryTraversalNoRepeat
) {
  TraversePrunedAfterSecondLevelHistoryTraversalNoRepeat_visited_catalogs.
    clear();
  EXPECT_EQ(0u,
    TraversePrunedAfterSecondLevelHistoryTraversalNoRepeat_visited_catalogs.
      size());

  CatalogIdentifiers catalogs;

  TraversalParams params = GetBasicTraversalParams();
  params.history           = 2;
  params.no_repeat_history = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(
    &TraversePrunedAfterSecondLevelHistoryTraversalNoRepeatCallback);

  const bool t1 = traverse.Traverse();
  EXPECT_TRUE(t1);

  catalogs.push_back(std::make_pair(6, ""));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(5, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(4, ""));

  CheckVisitedCatalogs(catalogs,
    TraversePrunedAfterSecondLevelHistoryTraversalNoRepeat_visited_catalogs);
  CheckCatalogSequence(catalogs,
    TraversePrunedAfterSecondLevelHistoryTraversalNoRepeat_visited_catalogs);
  EXPECT_EQ(1u, traverse.pruned_revision_count());

  const bool t2 = traverse.TraversePruned();
  EXPECT_TRUE(t2);

  catalogs.push_back(std::make_pair(3, ""));
  catalogs.push_back(std::make_pair(3, "/00/11"));
  catalogs.push_back(std::make_pair(3, "/00/11/24"));
  catalogs.push_back(std::make_pair(3, "/00/11/23"));
  catalogs.push_back(std::make_pair(3, "/00/11/22"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(2, ""));
  catalogs.push_back(std::make_pair(1, ""));

  CheckVisitedCatalogs(catalogs,
    TraversePrunedAfterSecondLevelHistoryTraversalNoRepeat_visited_catalogs);
  CheckCatalogSequence(catalogs,
    TraversePrunedAfterSecondLevelHistoryTraversalNoRepeat_visited_catalogs);
  EXPECT_EQ(0u, traverse.pruned_revision_count());
}


//------------------------------------------------------------------------------


CatalogIdentifiers TraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs;
void TraversePrunedAfterMultiTraversalNoRepeatCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  TraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, TraversePrunedAfterMultiTraversalNoRepeat) {
  TraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs.clear();
  EXPECT_EQ(
    0u, TraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs.size());

  CatalogIdentifiers catalogs;

  TraversalParams params = GetBasicTraversalParams();
  params.history           = 0;
  params.no_repeat_history = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&TraversePrunedAfterMultiTraversalNoRepeatCallback);

  const bool t1 = traverse.Traverse(GetRootHash(6));
  EXPECT_TRUE(t1);

  catalogs.push_back(std::make_pair(6, ""));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  CheckVisitedCatalogs(
    catalogs, TraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs);
  CheckCatalogSequence(
    catalogs, TraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs);

  const bool t2 = traverse.Traverse(GetRootHash(4));
  EXPECT_TRUE(t2);

  catalogs.push_back(std::make_pair(4, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  CheckVisitedCatalogs(
    catalogs, TraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs);
  CheckCatalogSequence(
    catalogs, TraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs);

  const bool t3 = traverse.Traverse(GetRootHash(2));
  EXPECT_TRUE(t3);

  catalogs.push_back(std::make_pair(2, ""));
  CheckVisitedCatalogs(
    catalogs, TraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs);
  CheckCatalogSequence(
    catalogs, TraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs);

  EXPECT_EQ(3u, traverse.pruned_revision_count());

  const bool t4 = traverse.TraversePruned();
  EXPECT_TRUE(t4);

  catalogs.push_back(std::make_pair(1, ""));
  catalogs.push_back(std::make_pair(3, ""));
  catalogs.push_back(std::make_pair(3, "/00/11"));
  catalogs.push_back(std::make_pair(3, "/00/11/24"));
  catalogs.push_back(std::make_pair(3, "/00/11/23"));
  catalogs.push_back(std::make_pair(3, "/00/11/22"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(5, ""));

  EXPECT_EQ(0u, traverse.pruned_revision_count());
  EXPECT_EQ(initial_catalog_instances,
            TraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs.size());
  CheckVisitedCatalogs(
    catalogs, TraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs);
  CheckCatalogSequence(
    catalogs, TraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs);
}


//------------------------------------------------------------------------------


CatalogIdentifiers TraverseRepositoryTagList_visited_catalogs;
void TraverseRepositoryTagListCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  TraverseRepositoryTagList_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, TraverseRepositoryTagList) {
  TraverseRepositoryTagList_visited_catalogs.clear();
  EXPECT_EQ(0u, TraverseRepositoryTagList_visited_catalogs.size());

  CatalogIdentifiers catalogs;

  TraversalParams params = GetBasicTraversalParams();
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&TraverseRepositoryTagListCallback);

  const bool t1 = traverse.TraverseNamedSnapshots();
  EXPECT_TRUE(t1);

  catalogs.push_back(std::make_pair(2, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(5, ""));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(6, ""));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));

  CheckVisitedCatalogs(catalogs, TraverseRepositoryTagList_visited_catalogs);
  CheckCatalogSequence(catalogs, TraverseRepositoryTagList_visited_catalogs);
}


//------------------------------------------------------------------------------


CatalogIdentifiers TraverseRepositoryTagListSecondHistoryLevel_visited_catalogs;
void TraverseRepositoryTagListSecondHistoryLevelCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  TraverseRepositoryTagListSecondHistoryLevel_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, TraverseRepositoryTagListSecondHistoryLevel) {
  TraverseRepositoryTagListSecondHistoryLevel_visited_catalogs.clear();
  EXPECT_EQ(
    0u, TraverseRepositoryTagListSecondHistoryLevel_visited_catalogs.size());

  CatalogIdentifiers catalogs;

  TraversalParams params = GetBasicTraversalParams();
  params.history = 2;  // doesn't have any effect on TraverseNamedSnapshot()
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(
    &TraverseRepositoryTagListSecondHistoryLevelCallback);

  const bool t1 = traverse.TraverseNamedSnapshots();
  EXPECT_TRUE(t1);

  catalogs.push_back(std::make_pair(2, ""));                // Revision 2
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(5, ""));                // Revision 5
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(6, ""));                // Revision 6
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));

  CheckVisitedCatalogs(
    catalogs, TraverseRepositoryTagListSecondHistoryLevel_visited_catalogs);
  CheckCatalogSequence(
    catalogs, TraverseRepositoryTagListSecondHistoryLevel_visited_catalogs);
  EXPECT_EQ(3u, traverse.pruned_revision_count());
}


//------------------------------------------------------------------------------


CatalogIdentifiers
  TraverseRepositoryTagListSecondHistoryLevelNoRepeat_visited_catalogs;
void TraverseRepositoryTagListSecondHistoryLevelNoRepeatCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  TraverseRepositoryTagListSecondHistoryLevelNoRepeat_visited_catalogs.
    push_back(std::make_pair(data.catalog->GetRevision(),
                             data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, TraverseRepositoryTagListSecondHistoryLevelNoRepeat)
{
  TraverseRepositoryTagListSecondHistoryLevelNoRepeat_visited_catalogs.clear();
  EXPECT_EQ(0u,
  TraverseRepositoryTagListSecondHistoryLevelNoRepeat_visited_catalogs.size());

  CatalogIdentifiers catalogs;

  TraversalParams params = GetBasicTraversalParams();
  // doesn't have any effect on TraverseNamedSnapshot()
  params.history           = 2;
  params.no_repeat_history = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(
    &TraverseRepositoryTagListSecondHistoryLevelNoRepeatCallback);

  const bool t1 = traverse.TraverseNamedSnapshots();
  EXPECT_TRUE(t1);

  catalogs.push_back(std::make_pair(2, ""));                // Revision 2
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(5, ""));                // Revision 5
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(6, ""));                // Revision 6

  CheckVisitedCatalogs(catalogs,
    TraverseRepositoryTagListSecondHistoryLevelNoRepeat_visited_catalogs);
  CheckCatalogSequence(catalogs,
    TraverseRepositoryTagListSecondHistoryLevelNoRepeat_visited_catalogs);
  EXPECT_EQ(3u, traverse.pruned_revision_count());
}


//------------------------------------------------------------------------------


CatalogIdentifiers
  TraverseRepositoryTagListFirstLevelHistoryTraversePrunedNoRepeat_visited_catalogs;  // NOLINT(whitespace/line_length)
void TraverseRepositoryTagListFirstLevelHistoryTraversePrunedNoRepeatCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  TraverseRepositoryTagListFirstLevelHistoryTraversePrunedNoRepeat_visited_catalogs.  // NOLINT(whitespace/line_length)
    push_back(std::make_pair(data.catalog->GetRevision(),
                             data.catalog->path().ToString()));
}

TEST_F(
  T_CatalogTraversal,
  TraverseRepositoryTagListFirstLevelHistoryTraversePrunedNoRepeat)
{
  TraverseRepositoryTagListFirstLevelHistoryTraversePrunedNoRepeat_visited_catalogs.clear();  // NOLINT(whitespace/line_length)
  EXPECT_EQ(0u, TraverseRepositoryTagListFirstLevelHistoryTraversePrunedNoRepeat_visited_catalogs.size());  // NOLINT(whitespace/line_length)

  CatalogIdentifiers catalogs;

  TraversalParams params = GetBasicTraversalParams();
  // doesn't have any effect on TraverseNamedSnapshot()
  params.history           = 1;
  params.no_repeat_history = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(
    &TraverseRepositoryTagListFirstLevelHistoryTraversePrunedNoRepeatCallback);

  const bool t1 = traverse.TraverseNamedSnapshots();
  EXPECT_TRUE(t1);

  catalogs.push_back(std::make_pair(2, ""));                // Revision 2
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(5, ""));                // Revision 5
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(6, ""));                // Revision 6

  CheckVisitedCatalogs(catalogs,
    TraverseRepositoryTagListFirstLevelHistoryTraversePrunedNoRepeat_visited_catalogs);  // NOLINT(whitespace/line_length)
  CheckCatalogSequence(catalogs,
    TraverseRepositoryTagListFirstLevelHistoryTraversePrunedNoRepeat_visited_catalogs);  // NOLINT(whitespace/line_length)
  EXPECT_EQ(3u, traverse.pruned_revision_count());

  const bool t2 = traverse.TraversePruned();
  EXPECT_TRUE(t2);

  catalogs.push_back(std::make_pair(4, ""));
  catalogs.push_back(std::make_pair(3, ""));
  catalogs.push_back(std::make_pair(3, "/00/11"));
  catalogs.push_back(std::make_pair(3, "/00/11/24"));
  catalogs.push_back(std::make_pair(3, "/00/11/23"));
  catalogs.push_back(std::make_pair(3, "/00/11/22"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(1, ""));

  CheckVisitedCatalogs(catalogs,
    TraverseRepositoryTagListFirstLevelHistoryTraversePrunedNoRepeat_visited_catalogs);  // NOLINT(whitespace/line_length)
  CheckCatalogSequence(catalogs,
    TraverseRepositoryTagListFirstLevelHistoryTraversePrunedNoRepeat_visited_catalogs);  // NOLINT(whitespace/line_length)
  EXPECT_EQ(0u, traverse.pruned_revision_count());
}


//------------------------------------------------------------------------------


CatalogIdentifiers TraverseUntilUnavailableRevisionNoRepeat_visited_catalogs;
void TraverseUntilUnavailableRevisionNoRepeatCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  TraverseUntilUnavailableRevisionNoRepeat_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, TraverseUntilUnavailableRevisionNoRepeat) {
  TraverseUntilUnavailableRevisionNoRepeat_visited_catalogs.clear();
  EXPECT_EQ(
    0u, TraverseUntilUnavailableRevisionNoRepeat_visited_catalogs.size());

  std::set<shash::Any> deleted_catalogs;
  deleted_catalogs.insert(GetRootHash(1));
  deleted_catalogs.insert(GetRootHash(2));
  deleted_catalogs.insert(GetRootHash(3));
  deleted_catalogs.insert(GetRootHash(4));
  catalog::MockCatalog::s_deleted_objects = &deleted_catalogs;

  CatalogIdentifiers catalogs;

  TraversalParams params = GetBasicTraversalParams();
  params.history             = 4;
  params.no_repeat_history   = true;
  params.ignore_load_failure = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&TraverseUntilUnavailableRevisionNoRepeatCallback);

  const bool t1 = traverse.Traverse();
  EXPECT_TRUE(t1);

  catalogs.push_back(std::make_pair(6, ""));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(5, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));

  CheckVisitedCatalogs(
    catalogs, TraverseUntilUnavailableRevisionNoRepeat_visited_catalogs);
}


//------------------------------------------------------------------------------


CatalogIdentifiers UnavailableNestedNoRepeat_visited_catalogs;
void UnavailableNestedNoRepeatCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  UnavailableNestedNoRepeat_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, UnavailableNestedNoRepeat) {
  UnavailableNestedNoRepeat_visited_catalogs.clear();
  EXPECT_EQ(0u, UnavailableNestedNoRepeat_visited_catalogs.size());

  catalog::MockCatalog* doomed_nested_catalog = GetCatalog(2, "/00/10/20");
  ASSERT_NE(static_cast<catalog::MockCatalog*>(NULL), doomed_nested_catalog);

  std::set<shash::Any> deleted_catalogs;
  deleted_catalogs.insert(doomed_nested_catalog->hash());
  catalog::MockCatalog::s_deleted_objects = &deleted_catalogs;

  CatalogIdentifiers catalogs;

  TraversalParams params = GetBasicTraversalParams();
  params.history             = 4;
  params.quiet               = true;
  params.no_repeat_history   = true;
  params.ignore_load_failure = false;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&UnavailableNestedNoRepeatCallback);

  const bool t1 = traverse.Traverse();
  EXPECT_FALSE(t1);

  // the doomed catalog is part of revision 5, thus, all catalog of revision 6
  // should still be hit (+ a couple more from revision 5)
  catalogs.push_back(std::make_pair(6, ""));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(5, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  // --> here the missing catalog (and its descendents should have been)
  //     since the catalog traversal aborted, the overall traversed tree is
  //     truncated (see `IgnoreUnavailableNestedNoRepeat`)
  // catalogs.push_back(std::make_pair(2, "/00/10/20"));
  // catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  // catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  // ...
  // catalogs.push_back(std::make_pair(4, ""));
  // ...

  CheckVisitedCatalogs(catalogs, UnavailableNestedNoRepeat_visited_catalogs);
  CheckCatalogSequence(catalogs, UnavailableNestedNoRepeat_visited_catalogs);
}


//------------------------------------------------------------------------------


CatalogIdentifiers IgnoreUnavailableNestedNoRepeat_visited_catalogs;
void IgnoreUnavailableNestedNoRepeatCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  IgnoreUnavailableNestedNoRepeat_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, IgnoreUnavailableNestedNoRepeat) {
  IgnoreUnavailableNestedNoRepeat_visited_catalogs.clear();
  EXPECT_EQ(0u, IgnoreUnavailableNestedNoRepeat_visited_catalogs.size());

  catalog::MockCatalog* doomed_nested_catalog = GetCatalog(2, "/00/10/20");
  ASSERT_NE(static_cast<catalog::MockCatalog*>(NULL), doomed_nested_catalog);

  std::set<shash::Any> deleted_catalogs;
  deleted_catalogs.insert(doomed_nested_catalog->hash());
  catalog::MockCatalog::s_deleted_objects = &deleted_catalogs;

  CatalogIdentifiers catalogs;

  TraversalParams params = GetBasicTraversalParams();
  params.history             = 4;
  params.quiet               = true;
  params.no_repeat_history   = true;
  params.ignore_load_failure = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&IgnoreUnavailableNestedNoRepeatCallback);

  const bool t1 = traverse.Traverse();
  EXPECT_TRUE(t1);

  catalogs.push_back(std::make_pair(6, ""));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(5, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  // --> here the missing catalog (and its descendents should have been)
  // catalogs.push_back(std::make_pair(2, "/00/10/20"));
  // catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  // catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  // catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  // catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(4, ""));
  catalogs.push_back(std::make_pair(3, ""));
  catalogs.push_back(std::make_pair(3, "/00/11"));
  catalogs.push_back(std::make_pair(3, "/00/11/24"));
  catalogs.push_back(std::make_pair(3, "/00/11/23"));
  catalogs.push_back(std::make_pair(3, "/00/11/22"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(2, ""));

  CheckVisitedCatalogs(
    catalogs, IgnoreUnavailableNestedNoRepeat_visited_catalogs);
  CheckCatalogSequence(
    catalogs, IgnoreUnavailableNestedNoRepeat_visited_catalogs);
}


//------------------------------------------------------------------------------


CatalogIdentifiers
  DepthFirstSearchFullHistoryTraversalNoRepeat_visited_catalogs;
void DepthFirstSearchFullHistoryTraversalNoRepeatCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  DepthFirstSearchFullHistoryTraversalNoRepeat_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, DepthFirstSearchFullHistoryTraversalNoRepeat) {
  DepthFirstSearchFullHistoryTraversalNoRepeat_visited_catalogs.clear();
  EXPECT_EQ(0u,
    DepthFirstSearchFullHistoryTraversalNoRepeat_visited_catalogs.size());

  TraversalParams params = GetBasicTraversalParams();
  params.history           = TraversalParams::kFullHistory;
  params.no_repeat_history = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(
    &DepthFirstSearchFullHistoryTraversalNoRepeatCallback);
  const bool t1 =
    traverse.Traverse(MockedCatalogTraversal::kDepthFirstTraversal);
  EXPECT_TRUE(t1);

  CatalogIdentifiers catalogs;

  catalogs.push_back(std::make_pair(1, ""));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, ""));
  catalogs.push_back(std::make_pair(3, "/00/11/24"));
  catalogs.push_back(std::make_pair(3, "/00/11/23"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(3, "/00/11/22"));
  catalogs.push_back(std::make_pair(3, "/00/11"));
  catalogs.push_back(std::make_pair(3, ""));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, ""));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, ""));
  catalogs.push_back(std::make_pair(6, ""));

  EXPECT_EQ(initial_catalog_instances,
    DepthFirstSearchFullHistoryTraversalNoRepeat_visited_catalogs.size());

  CheckVisitedCatalogs(catalogs,
    DepthFirstSearchFullHistoryTraversalNoRepeat_visited_catalogs);
  CheckCatalogSequence(catalogs,
    DepthFirstSearchFullHistoryTraversalNoRepeat_visited_catalogs);
}


//------------------------------------------------------------------------------


CatalogIdentifiers FullHistoryDepthFirstTraversal_visited_catalogs;
void FullHistoryDepthFirstTraversalCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  FullHistoryDepthFirstTraversal_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, FullHistoryDepthFirstTraversal) {
  FullHistoryDepthFirstTraversal_visited_catalogs.clear();
  EXPECT_EQ(0u, FullHistoryDepthFirstTraversal_visited_catalogs.size());

  TraversalParams params = GetBasicTraversalParams();
  params.history = TraversalParams::kFullHistory;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&FullHistoryDepthFirstTraversalCallback);
  const bool t1 =
    traverse.Traverse(MockedCatalogTraversal::kDepthFirstTraversal);
  EXPECT_TRUE(t1);

  CatalogIdentifiers catalogs;

  catalogs.push_back(std::make_pair(1, ""));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, ""));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(3, "/00/11/24"));
  catalogs.push_back(std::make_pair(3, "/00/11/23"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(3, "/00/11/22"));
  catalogs.push_back(std::make_pair(3, "/00/11"));
  catalogs.push_back(std::make_pair(3, ""));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, ""));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, ""));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(6, ""));

  CheckVisitedCatalogs(
    catalogs, FullHistoryDepthFirstTraversal_visited_catalogs);
  CheckCatalogSequence(
    catalogs, FullHistoryDepthFirstTraversal_visited_catalogs);
}


//------------------------------------------------------------------------------


CatalogIdentifiers
  DepthFirstTraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs;
void DepthFirstTraversePrunedAfterMultiTraversalNoRepeatCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  DepthFirstTraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs.
    push_back(std::make_pair(data.catalog->GetRevision(),
                             data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, DepthFirstTraversePrunedAfterMultiTraversalNoRepeat)
{
  DepthFirstTraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs.clear();
  EXPECT_EQ(0u,
  DepthFirstTraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs.size());

  CatalogIdentifiers catalogs;

  TraversalParams params = GetBasicTraversalParams();
  params.history           = 0;
  params.no_repeat_history = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(
    &DepthFirstTraversePrunedAfterMultiTraversalNoRepeatCallback);

  const bool t1 = traverse.Traverse(GetRootHash(6));
  EXPECT_TRUE(t1);

  catalogs.push_back(std::make_pair(6, ""));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  CheckVisitedCatalogs(catalogs,
  DepthFirstTraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs);

  const bool t2 = traverse.Traverse(GetRootHash(4));
  EXPECT_TRUE(t2);

  catalogs.push_back(std::make_pair(4, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  CheckVisitedCatalogs(catalogs,
  DepthFirstTraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs);

  const bool t3 = traverse.Traverse(GetRootHash(2));
  EXPECT_TRUE(t3);

  catalogs.push_back(std::make_pair(2, ""));
  CheckVisitedCatalogs(catalogs,
  DepthFirstTraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs);

  EXPECT_EQ(3u, traverse.pruned_revision_count());

  const bool t4 = traverse.TraversePruned(
    MockedCatalogTraversal::kDepthFirstTraversal);
  EXPECT_TRUE(t4);

  catalogs.push_back(std::make_pair(5, ""));
  catalogs.push_back(std::make_pair(3, ""));
  catalogs.push_back(std::make_pair(3, "/00/11"));
  catalogs.push_back(std::make_pair(3, "/00/11/24"));
  catalogs.push_back(std::make_pair(3, "/00/11/23"));
  catalogs.push_back(std::make_pair(3, "/00/11/22"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(1, ""));

  EXPECT_EQ(0u, traverse.pruned_revision_count());
  EXPECT_EQ(initial_catalog_instances,
    DepthFirstTraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs.
      size());
  CheckVisitedCatalogs(catalogs,
    DepthFirstTraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs);
}


//------------------------------------------------------------------------------


CatalogIdentifiers DepthFirstTraversalSequence_visited_catalogs;
void DepthFirstTraversalSequenceCallback(
  const MockedCatalogTraversal::CallbackDataTN &data) {
  DepthFirstTraversalSequence_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, DepthFirstTraversalSequence) {
  DepthFirstTraversalSequence_visited_catalogs.clear();
  EXPECT_EQ(0u, DepthFirstTraversalSequence_visited_catalogs.size());

  CatalogIdentifiers catalogs;

  TraversalParams params = GetBasicTraversalParams();
  params.history           = 0;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&DepthFirstTraversalSequenceCallback);

  const bool t1 = traverse.Traverse(
    GetRootHash(2), MockedCatalogTraversal::kDepthFirstTraversal);
  EXPECT_TRUE(t1);

  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, ""));

  EXPECT_EQ(1u, traverse.pruned_revision_count());
  CheckVisitedCatalogs(catalogs, DepthFirstTraversalSequence_visited_catalogs);
  CheckCatalogSequence(catalogs, DepthFirstTraversalSequence_visited_catalogs);
}


//------------------------------------------------------------------------------


CatalogIdentifiers
  FullHistoryDepthFirstTraversalUnavailableAncestor_visited_catalogs;
void FullHistoryDepthFirstTraversalUnavailableAncestorCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  FullHistoryDepthFirstTraversalUnavailableAncestor_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, FullHistoryDepthFirstTraversalUnavailableAncestor) {
  FullHistoryDepthFirstTraversalUnavailableAncestor_visited_catalogs.clear();
  EXPECT_EQ(0u,
    FullHistoryDepthFirstTraversalUnavailableAncestor_visited_catalogs.size());

  std::set<shash::Any> deleted_catalogs;
  deleted_catalogs.insert(GetRootHash(2));
  catalog::MockCatalog::s_deleted_objects = &deleted_catalogs;

  TraversalParams params = GetBasicTraversalParams();
  params.history             = TraversalParams::kFullHistory;
  params.ignore_load_failure = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(
    &FullHistoryDepthFirstTraversalUnavailableAncestorCallback);
  const bool t1 =
    traverse.Traverse(MockedCatalogTraversal::kDepthFirstTraversal);
  EXPECT_TRUE(t1);

  CatalogIdentifiers catalogs;
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(3, "/00/11/24"));
  catalogs.push_back(std::make_pair(3, "/00/11/23"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(3, "/00/11/22"));
  catalogs.push_back(std::make_pair(3, "/00/11"));
  catalogs.push_back(std::make_pair(3, ""));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, ""));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, ""));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(6, ""));

  CheckVisitedCatalogs(catalogs,
    FullHistoryDepthFirstTraversalUnavailableAncestor_visited_catalogs);
  CheckCatalogSequence(catalogs,
    FullHistoryDepthFirstTraversalUnavailableAncestor_visited_catalogs);
}


//------------------------------------------------------------------------------


void FullTraversalRootCatalogDetectionCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  const bool should_be_root = (data.catalog->path().ToString() == "" ||
                               data.tree_level                 == 0);
  EXPECT_EQ(should_be_root, data.catalog->IsRoot());
}

TEST_F(T_CatalogTraversal, FullTraversalRootCatalogDetection) {
  TraversalParams params = GetBasicTraversalParams();
  params.history = TraversalParams::kFullHistory;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&FullTraversalRootCatalogDetectionCallback);

  const bool t1 = traverse.Traverse();
  EXPECT_TRUE(t1);
}


//------------------------------------------------------------------------------


CatalogIdentifiers TimestampThreshold_visited_catalogs;
void TimestampThresholdCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  TimestampThreshold_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, TimestampThreshold) {
  TimestampThreshold_visited_catalogs.clear();
  EXPECT_EQ(0u, TimestampThreshold_visited_catalogs.size());

  TraversalParams params = GetBasicTraversalParams();
  params.history             = TraversalParams::kFullHistory;
  params.timestamp           = t(16, 11, 2014) + 1;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&TimestampThresholdCallback);
  const bool t1 =
    traverse.Traverse(MockedCatalogTraversal::kBreadthFirstTraversal);
  EXPECT_TRUE(t1);

  CatalogIdentifiers catalogs;

  catalogs.push_back(std::make_pair(6, ""));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(5, ""));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));

  CheckVisitedCatalogs(catalogs, TimestampThreshold_visited_catalogs);
  CheckCatalogSequence(catalogs, TimestampThreshold_visited_catalogs);
}


//------------------------------------------------------------------------------


CatalogIdentifiers FutureTimestampThreshold_visited_catalogs;
void FutureTimestampThresholdCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  FutureTimestampThreshold_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, FutureTimestampThreshold) {
  // Note: future in a sense of: younger than newest mocked revision!
  FutureTimestampThreshold_visited_catalogs.clear();
  EXPECT_EQ(0u, FutureTimestampThreshold_visited_catalogs.size());

  TraversalParams params = GetBasicTraversalParams();
  params.history             = TraversalParams::kFullHistory;
  params.timestamp           = t(31, 12, 2014);
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&FutureTimestampThresholdCallback);
  const bool t1 =
    traverse.Traverse(MockedCatalogTraversal::kBreadthFirstTraversal);
  EXPECT_TRUE(t1);

  CatalogIdentifiers catalogs;
  catalogs.push_back(std::make_pair(6, ""));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));

  CheckVisitedCatalogs(catalogs, FutureTimestampThreshold_visited_catalogs);
  CheckCatalogSequence(catalogs, FutureTimestampThreshold_visited_catalogs);
}


//------------------------------------------------------------------------------


CatalogIdentifiers TimestampThresholdAndNamedSnapshots_visited_catalogs;
void TimestampThresholdAndNamedSnapshotsCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  TimestampThresholdAndNamedSnapshots_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, TimestampThresholdAndNamedSnapshots) {
  // Note: future in a sense of: younger than newest mocked revision!
  TimestampThresholdAndNamedSnapshots_visited_catalogs.clear();
  EXPECT_EQ(0u, TimestampThresholdAndNamedSnapshots_visited_catalogs.size());

  TraversalParams params = GetBasicTraversalParams();
  params.timestamp = t(6, 6, 2010);  // no effect on NamedSnapshotTraversal()
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&TimestampThresholdAndNamedSnapshotsCallback);
  const bool t1 = traverse.TraverseNamedSnapshots(
    MockedCatalogTraversal::kBreadthFirstTraversal);
  EXPECT_TRUE(t1);

  CatalogIdentifiers catalogs;

  catalogs.push_back(std::make_pair(2, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(5, ""));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(6, ""));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));

  CheckVisitedCatalogs(
    catalogs, TimestampThresholdAndNamedSnapshots_visited_catalogs);
  CheckCatalogSequence(
    catalogs, TimestampThresholdAndNamedSnapshots_visited_catalogs);
  EXPECT_EQ(3u, traverse.pruned_revision_count());
}


//------------------------------------------------------------------------------


CatalogIdentifiers
  TraversePrunedAfterTimestampThresholdHistoryDepthAndNamedSnapshotsAndAfterHeadTraversalNoRepeat_visited_catalogs;  // NOLINT(whitespace/line_length)
void TraversePrunedAfterTimestampThresholdHistoryDepthAndNamedSnapshotsAndAfterHeadTraversalNoRepeatCallback(  // NOLINT(whitespace/line_length)
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  TraversePrunedAfterTimestampThresholdHistoryDepthAndNamedSnapshotsAndAfterHeadTraversalNoRepeat_visited_catalogs.  // NOLINT(whitespace/line_length)
    push_back(std::make_pair(data.catalog->GetRevision(),
                             data.catalog->path().ToString()));
}

TEST_F(
  T_CatalogTraversal,
  TraversePrunedAfterTimestampThresholdHistoryDepthAndNamedSnapshotsAndAfterHeadTraversalNoRepeat  // NOLINT(whitespace/line_length)
) {
  // Note: future in a sense of: younger than newest mocked revision!
  TraversePrunedAfterTimestampThresholdHistoryDepthAndNamedSnapshotsAndAfterHeadTraversalNoRepeat_visited_catalogs.clear();  // NOLINT(whitespace/line_length)
  EXPECT_EQ(0u,
    TraversePrunedAfterTimestampThresholdHistoryDepthAndNamedSnapshotsAndAfterHeadTraversalNoRepeat_visited_catalogs.size());  // NOLINT(whitespace/line_length)

  TraversalParams params = GetBasicTraversalParams();
  // no effect on NamedSnapshotTraversal()
  params.timestamp         = t(6, 6, 2008);
  // no effect on NamedSnapshotTraversal()
  params.history           = 2;
  params.no_repeat_history = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(
    &TraversePrunedAfterTimestampThresholdHistoryDepthAndNamedSnapshotsAndAfterHeadTraversalNoRepeatCallback);  // NOLINT(whitespace/line_length)

  const bool t1 =
    traverse.Traverse(MockedCatalogTraversal::kDepthFirstTraversal);
  EXPECT_TRUE(t1);

  CatalogIdentifiers catalogs;
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, ""));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, ""));
  catalogs.push_back(std::make_pair(6, ""));

  CheckVisitedCatalogs(catalogs,
    TraversePrunedAfterTimestampThresholdHistoryDepthAndNamedSnapshotsAndAfterHeadTraversalNoRepeat_visited_catalogs);  // NOLINT(whitespace/line_length)
  CheckCatalogSequence(catalogs,
    TraversePrunedAfterTimestampThresholdHistoryDepthAndNamedSnapshotsAndAfterHeadTraversalNoRepeat_visited_catalogs);  // NOLINT(whitespace/line_length)
  EXPECT_EQ(1u, traverse.pruned_revision_count());

  const bool t2 = traverse.TraverseNamedSnapshots();
  EXPECT_TRUE(t2);

  catalogs.push_back(std::make_pair(2, ""));

  CheckVisitedCatalogs(catalogs,
    TraversePrunedAfterTimestampThresholdHistoryDepthAndNamedSnapshotsAndAfterHeadTraversalNoRepeat_visited_catalogs);  // NOLINT(whitespace/line_length)
  CheckCatalogSequence(catalogs,
    TraversePrunedAfterTimestampThresholdHistoryDepthAndNamedSnapshotsAndAfterHeadTraversalNoRepeat_visited_catalogs);  // NOLINT(whitespace/line_length)
  EXPECT_EQ(2u, traverse.pruned_revision_count());

  const bool t3 =
    traverse.TraversePruned(MockedCatalogTraversal::kDepthFirstTraversal);
  EXPECT_TRUE(t3);

  catalogs.push_back(std::make_pair(1, ""));
  catalogs.push_back(std::make_pair(3, "/00/11/24"));
  catalogs.push_back(std::make_pair(3, "/00/11/23"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(3, "/00/11/22"));
  catalogs.push_back(std::make_pair(3, "/00/11"));
  catalogs.push_back(std::make_pair(3, ""));

  CheckVisitedCatalogs(catalogs,
    TraversePrunedAfterTimestampThresholdHistoryDepthAndNamedSnapshotsAndAfterHeadTraversalNoRepeat_visited_catalogs);  // NOLINT(whitespace/line_length)
  CheckCatalogSequence(catalogs,
    TraversePrunedAfterTimestampThresholdHistoryDepthAndNamedSnapshotsAndAfterHeadTraversalNoRepeat_visited_catalogs);  // NOLINT(whitespace/line_length)
  EXPECT_EQ(0u, traverse.pruned_revision_count());
}


//------------------------------------------------------------------------------


CatalogIdentifiers TimestampThresholdDepthFirst_visited_catalogs;
void TimestampThresholdDepthFirstCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  TimestampThresholdDepthFirst_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, TimestampThresholdDepthFirst) {
  TimestampThresholdDepthFirst_visited_catalogs.clear();
  EXPECT_EQ(0u, TimestampThresholdDepthFirst_visited_catalogs.size());

  TraversalParams params = GetBasicTraversalParams();
  params.history             = TraversalParams::kFullHistory;
  params.timestamp           = t(16, 11, 2014) + 1;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&TimestampThresholdDepthFirstCallback);
  const bool t1 =
    traverse.Traverse(MockedCatalogTraversal::kDepthFirstTraversal);
  EXPECT_TRUE(t1);

  CatalogIdentifiers catalogs;
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, ""));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(6, ""));

  CheckVisitedCatalogs(catalogs, TimestampThresholdDepthFirst_visited_catalogs);
  CheckCatalogSequence(catalogs, TimestampThresholdDepthFirst_visited_catalogs);
}


//------------------------------------------------------------------------------


CatalogIdentifiers
  TimestampThresholdDepthFirstAndTraversePruned_visited_catalogs;
void TimestampThresholdDepthFirstAndTraversePrunedCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  TimestampThresholdDepthFirstAndTraversePruned_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, TimestampThresholdDepthFirstAndTraversePruned) {
  TimestampThresholdDepthFirstAndTraversePruned_visited_catalogs.clear();
  EXPECT_EQ(0u,
    TimestampThresholdDepthFirstAndTraversePruned_visited_catalogs.size());

  TraversalParams params = GetBasicTraversalParams();
  params.history             = TraversalParams::kFullHistory;
  params.timestamp           = t(16, 11, 2014) + 1;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(
    &TimestampThresholdDepthFirstAndTraversePrunedCallback);
  const bool t1 = traverse.Traverse(
    MockedCatalogTraversal::kDepthFirstTraversal);
  EXPECT_TRUE(t1);

  CatalogIdentifiers catalogs;
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, ""));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(6, ""));

  CheckVisitedCatalogs(catalogs,
    TimestampThresholdDepthFirstAndTraversePruned_visited_catalogs);
  CheckCatalogSequence(catalogs,
    TimestampThresholdDepthFirstAndTraversePruned_visited_catalogs);
  EXPECT_EQ(1u, traverse.pruned_revision_count());

  const bool t2 =
    traverse.TraversePruned(MockedCatalogTraversal::kBreadthFirstTraversal);
  EXPECT_TRUE(t2);

  catalogs.push_back(std::make_pair(4, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(3, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(3, "/00/11"));
  catalogs.push_back(std::make_pair(3, "/00/11/24"));
  catalogs.push_back(std::make_pair(3, "/00/11/23"));
  catalogs.push_back(std::make_pair(3, "/00/11/22"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(3, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(2, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(1, ""));

  CheckVisitedCatalogs(catalogs,
    TimestampThresholdDepthFirstAndTraversePruned_visited_catalogs);
  CheckCatalogSequence(catalogs,
    TimestampThresholdDepthFirstAndTraversePruned_visited_catalogs);
  EXPECT_EQ(0u, traverse.pruned_revision_count());
}


//------------------------------------------------------------------------------


CatalogIdentifiers
  TimestampThresholdHistoryDepthAndNamedSnapshotsDepthFirstNoRepeat_visited_catalogs;  // NOLINT(whitespace/line_length)
void TimestampThresholdHistoryDepthAndNamedSnapshotsDepthFirstNoRepeatCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  TimestampThresholdHistoryDepthAndNamedSnapshotsDepthFirstNoRepeat_visited_catalogs.  // NOLINT(whitespace/line_length)
    push_back(std::make_pair(data.catalog->GetRevision(),
              data.catalog->path().ToString()));
}

TEST_F(
  T_CatalogTraversal,
  TimestampThresholdHistoryDepthDepthFirstAndNamedSnapshotsNoRepeat
) {
  // Note: future in a sense of: younger than newest mocked revision!
  TimestampThresholdHistoryDepthAndNamedSnapshotsDepthFirstNoRepeat_visited_catalogs.clear();  // NOLINT(whitespace/line_length)
  EXPECT_EQ(0u,
    TimestampThresholdHistoryDepthAndNamedSnapshotsDepthFirstNoRepeat_visited_catalogs.size());  // NOLINT(whitespace/line_length)

  TraversalParams params = GetBasicTraversalParams();
  // no effect on TraverseNamedSnapshots()
  params.timestamp         = t(6, 6, 2003);
  params.history           = 1;
  // no effect on TraverseNamedSnapshots()
  params.no_repeat_history = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(
    &TimestampThresholdHistoryDepthAndNamedSnapshotsDepthFirstNoRepeatCallback);
  const bool t1 = traverse.TraverseNamedSnapshots(
    MockedCatalogTraversal::kDepthFirstTraversal);
  EXPECT_TRUE(t1);

  CatalogIdentifiers catalogs;
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, ""));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, ""));
  catalogs.push_back(std::make_pair(6, ""));

  CheckVisitedCatalogs(catalogs,
    TimestampThresholdHistoryDepthAndNamedSnapshotsDepthFirstNoRepeat_visited_catalogs);  // NOLINT(whitespace/line_length)
  CheckCatalogSequence(catalogs,
    TimestampThresholdHistoryDepthAndNamedSnapshotsDepthFirstNoRepeat_visited_catalogs);  // NOLINT(whitespace/line_length)
  EXPECT_EQ(3u, traverse.pruned_revision_count());
}


//------------------------------------------------------------------------------


CatalogIdentifiers
  TimestampThresholdHistoryDepthNamedSnapshotsDeletedRevisionDepthFirstNoRepeat_visited_catalogs;  // NOLINT(whitespace/line_length)
void TimestampThresholdHistoryDepthNamedSnapshotsDeletedRevisionDepthFirstNoRepeatCallback(  // NOLINT(whitespace/line_length)
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  TimestampThresholdHistoryDepthNamedSnapshotsDeletedRevisionDepthFirstNoRepeat_visited_catalogs.  // NOLINT(whitespace/line_length)
    push_back(std::make_pair(data.catalog->GetRevision(),
              data.catalog->path().ToString()));
}

TEST_F(
  T_CatalogTraversal,
  TimestampThresholdHistoryDepthNamedSnapshotsDeletedRevisionDepthFirstNoRepeat)
{
  // Note: future in a sense of: younger than newest mocked revision!
  TimestampThresholdHistoryDepthNamedSnapshotsDeletedRevisionDepthFirstNoRepeat_visited_catalogs.clear();  // NOLINT(whitespace/line_length)
  EXPECT_EQ(0u,
    TimestampThresholdHistoryDepthNamedSnapshotsDeletedRevisionDepthFirstNoRepeat_visited_catalogs.size());  // NOLINT(whitespace/line_length)

  std::set<shash::Any> deleted_catalogs;
  deleted_catalogs.insert(GetRootHash(4));
  catalog::MockCatalog::s_deleted_objects = &deleted_catalogs;

  TraversalParams params = GetBasicTraversalParams();
  params.timestamp           = t(6, 6, 2003);
  params.history             = 1;
  params.no_repeat_history   = true;
  params.ignore_load_failure = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(
    &TimestampThresholdHistoryDepthNamedSnapshotsDeletedRevisionDepthFirstNoRepeatCallback);  // NOLINT(whitespace/line_length)
  const bool t1 = traverse.TraverseNamedSnapshots(
    MockedCatalogTraversal::kDepthFirstTraversal);
  EXPECT_TRUE(t1);

  CatalogIdentifiers catalogs;
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, ""));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, ""));
  catalogs.push_back(std::make_pair(6, ""));

  CheckVisitedCatalogs(catalogs,
    TimestampThresholdHistoryDepthNamedSnapshotsDeletedRevisionDepthFirstNoRepeat_visited_catalogs);  // NOLINT(whitespace/line_length)
  CheckCatalogSequence(catalogs,
    TimestampThresholdHistoryDepthNamedSnapshotsDeletedRevisionDepthFirstNoRepeat_visited_catalogs);  // NOLINT(whitespace/line_length)
}


//------------------------------------------------------------------------------


CatalogIdentifiers
  TimestampThresholdHistoryDepthNamedSnapshotsDeletedRevisionDepthFirstNoRepeatTraversePruned_visited_catalogs;  // NOLINT(whitespace/line_length)
void TimestampThresholdHistoryDepthNamedSnapshotsDeletedRevisionDepthFirstNoRepeatTraversePrunedCallback(  // NOLINT(whitespace/line_length)
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  TimestampThresholdHistoryDepthNamedSnapshotsDeletedRevisionDepthFirstNoRepeatTraversePruned_visited_catalogs.  // NOLINT(whitespace/line_length)
    push_back(std::make_pair(data.catalog->GetRevision(),
              data.catalog->path().ToString()));
}

TEST_F(
  T_CatalogTraversal,
  TimestampThresholdHistoryDepthNamedSnapshotsDeletedRevisionDepthFirstNoRepeatTraversePruned  // NOLINT(whitespace/line_length)
) {
  // Note: future in a sense of: younger than newest mocked revision!
  TimestampThresholdHistoryDepthNamedSnapshotsDeletedRevisionDepthFirstNoRepeatTraversePruned_visited_catalogs.clear();  // NOLINT(whitespace/line_length)
  EXPECT_EQ(0u,
    TimestampThresholdHistoryDepthNamedSnapshotsDeletedRevisionDepthFirstNoRepeatTraversePruned_visited_catalogs.size());  // NOLINT(whitespace/line_length)

  std::set<shash::Any> deleted_catalogs;
  deleted_catalogs.insert(GetRootHash(4));
  catalog::MockCatalog::s_deleted_objects = &deleted_catalogs;

  TraversalParams params = GetBasicTraversalParams();
  params.timestamp           = t(6, 6, 2003);
  params.history             = 1;
  params.no_repeat_history   = true;
  params.ignore_load_failure = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(
    &TimestampThresholdHistoryDepthNamedSnapshotsDeletedRevisionDepthFirstNoRepeatTraversePrunedCallback);  // // NOLINT(whitespace/line_length)
  const bool t1 = traverse.TraverseNamedSnapshots(
    MockedCatalogTraversal::kDepthFirstTraversal);
  EXPECT_TRUE(t1);

  CatalogIdentifiers catalogs;
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, ""));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, ""));
  catalogs.push_back(std::make_pair(6, ""));

  EXPECT_EQ(3u, traverse.pruned_revision_count());
  CheckVisitedCatalogs(catalogs,
    TimestampThresholdHistoryDepthNamedSnapshotsDeletedRevisionDepthFirstNoRepeatTraversePruned_visited_catalogs);  // NOLINT(whitespace/line_length)
  CheckCatalogSequence(catalogs,
    TimestampThresholdHistoryDepthNamedSnapshotsDeletedRevisionDepthFirstNoRepeatTraversePruned_visited_catalogs);  // NOLINT(whitespace/line_length)

  const bool t2 =
    traverse.TraversePruned(MockedCatalogTraversal::kDepthFirstTraversal);
  EXPECT_TRUE(t2);

  catalogs.push_back(std::make_pair(1, ""));
  // revision 3 is unreachable as revision 4 is not available...

  EXPECT_EQ(0u, traverse.pruned_revision_count());
  CheckVisitedCatalogs(catalogs,
    TimestampThresholdHistoryDepthNamedSnapshotsDeletedRevisionDepthFirstNoRepeatTraversePruned_visited_catalogs);  // NOLINT(whitespace/line_length)
  CheckCatalogSequence(catalogs,
    TimestampThresholdHistoryDepthNamedSnapshotsDeletedRevisionDepthFirstNoRepeatTraversePruned_visited_catalogs);  // NOLINT(whitespace/line_length)
}


//------------------------------------------------------------------------------


CatalogIdentifiers
  NamedSnapshotTraversalWithTimestampThresholdNoRepeat_visited_catalogs;
void NamedSnapshotTraversalWithTimestampThresholdNoRepeatCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  NamedSnapshotTraversalWithTimestampThresholdNoRepeat_visited_catalogs.
    push_back(std::make_pair(data.catalog->GetRevision(),
                             data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, NamedSnapshotTraversalWithTimestampThresholdNoRepeat)
{
  // Note: future in a sense of: younger than newest mocked revision!
  NamedSnapshotTraversalWithTimestampThresholdNoRepeat_visited_catalogs.clear();
  EXPECT_EQ(0u,
    NamedSnapshotTraversalWithTimestampThresholdNoRepeat_visited_catalogs.
      size());

  std::set<shash::Any> deleted_catalogs;
  deleted_catalogs.insert(GetRootHash(4));
  catalog::MockCatalog::s_deleted_objects = &deleted_catalogs;

  TraversalParams params = GetBasicTraversalParams();
  // excludes all revisions but HEAD
  params.timestamp           = t(17, 11, 2014) - 10;
  params.no_repeat_history   = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(
    &NamedSnapshotTraversalWithTimestampThresholdNoRepeatCallback);
  const bool t1 = traverse.TraverseNamedSnapshots();
  EXPECT_TRUE(t1);

  CatalogIdentifiers catalogs;

  catalogs.push_back(std::make_pair(2, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(5, ""));
  catalogs.push_back(std::make_pair(4, "/00/12"));
  catalogs.push_back(std::make_pair(4, "/00/12/27"));
  catalogs.push_back(std::make_pair(4, "/00/12/26"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/38"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/37"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/36"));
  catalogs.push_back(std::make_pair(4, "/00/12/26/35"));
  catalogs.push_back(std::make_pair(4, "/00/12/25"));
  catalogs.push_back(std::make_pair(4, "/00/11"));
  catalogs.push_back(std::make_pair(4, "/00/11/24"));
  catalogs.push_back(std::make_pair(4, "/00/11/23"));
  catalogs.push_back(std::make_pair(4, "/00/11/22"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/43"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/42"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/34/41"));
  catalogs.push_back(std::make_pair(4, "/00/11/22/33"));
  catalogs.push_back(std::make_pair(5, "/00/13"));
  catalogs.push_back(std::make_pair(5, "/00/13/29"));
  catalogs.push_back(std::make_pair(5, "/00/13/28"));
  catalogs.push_back(std::make_pair(6, ""));

  EXPECT_EQ(3u, traverse.pruned_revision_count());

  CheckVisitedCatalogs(catalogs,
    NamedSnapshotTraversalWithTimestampThresholdNoRepeat_visited_catalogs);
  CheckCatalogSequence(catalogs,
    NamedSnapshotTraversalWithTimestampThresholdNoRepeat_visited_catalogs);
}


//------------------------------------------------------------------------------


CatalogIdentifiers TraverseNamedSnapshotsWithoutHistory_visited_catalogs;
void TraverseNamedSnapshotsWithoutHistoryCallback(
  const MockedCatalogTraversal::CallbackDataTN &data)
{
  TraverseNamedSnapshotsWithoutHistory_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(),
                   data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, TraverseNamedSnapshotsWithoutHistory) {
  TraverseNamedSnapshotsWithoutHistory_visited_catalogs.clear();
  EXPECT_EQ(0u, TraverseNamedSnapshotsWithoutHistory_visited_catalogs.size());

  std::set<shash::Any> deleted_history;
  deleted_history.insert(MockHistory::root_hash);
  MockHistory::s_deleted_objects = &deleted_history;

  TraversalParams params = GetBasicTraversalParams();
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&TraverseNamedSnapshotsWithoutHistoryCallback);
  const bool t1 = traverse.TraverseNamedSnapshots();
  EXPECT_TRUE(t1);

  CatalogIdentifiers catalogs;
  // nothing to be traversed

  EXPECT_EQ(0u, traverse.pruned_revision_count());
  CheckVisitedCatalogs(
    catalogs, TraverseNamedSnapshotsWithoutHistory_visited_catalogs);
  CheckCatalogSequence(
    catalogs, TraverseNamedSnapshotsWithoutHistory_visited_catalogs);
}
