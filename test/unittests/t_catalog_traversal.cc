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
typedef std::pair<unsigned int, std::string>             CatalogIdentifier;
typedef std::vector<CatalogIdentifier>                   CatalogIdentifiers;

class T_CatalogTraversal : public ::testing::Test {
 public:
  static const std::string sandbox;
  static const std::string fqrn;

 public:
  MockCatalog *dummy_catalog_hierarchy;

 protected:
  typedef std::map<std::string, MockCatalog*>    CatalogPathMap;
  typedef std::map<unsigned int, CatalogPathMap> RevisionMap;

  const unsigned int max_revision;
  const unsigned int initial_catalog_instances;

 public:
  T_CatalogTraversal() :
    max_revision(6),
    initial_catalog_instances(42) /* depends on max_revision */ {}

 protected:
  void SetUp() {
    // create a dummy history file
    const bool retval = MkdirDeep(sandbox, 0700);
    ASSERT_TRUE (retval) << "failed to create sandbox";
    const std::string history_db = GetHistoryFilename();
    named_snapshots_ = history::History::Create(history_db,
                                                T_CatalogTraversal::fqrn);
    ASSERT_TRUE (named_snapshots_.IsValid());
    MockObjectFetcher::s_history = &named_snapshots_; // a pointer to a UniquePtr, I should burn
                                                      // in hell for that. (If the testee code is
                                                      // actually requesting this, the UniquePtr
                                                      // will be released - thus not freed in
                                                      // MockObjectFetcher::FetchHistory)

    dice_.InitLocaltime();
    MockCatalog::Reset();
    SetupDummyCatalogs();
    EXPECT_EQ (initial_catalog_instances, MockCatalog::instances);
    MockObjectFetcher::s_history = &named_snapshots_;
  }

  void TearDown() {
    MockCatalog::UnregisterCatalogs();
    EXPECT_EQ (0u, MockCatalog::instances);
    MockObjectFetcher::s_history = NULL; // TODO: potential memory leak!

    const bool retval = RemoveTree(sandbox);
    ASSERT_TRUE (retval) << "failed to remove sandbox";
  }

  std::string GetHistoryFilename() const {
    const std::string path = CreateTempPath(sandbox + "/history", 0600);
    CheckEmpty(path);
    return path;
  }

  void CheckVisitedCatalogs(const CatalogIdentifiers expected,
                            const CatalogIdentifiers observed,
                            const bool               check_counts = true) {
    if (check_counts) {
      EXPECT_EQ (expected.size(), observed.size());
    }
    typedef CatalogIdentifiers::const_iterator itr;

    unsigned int _i = 0;
    itr i    = expected.begin();
    itr iend = expected.end();
    for (; i != iend; ++i) {
      bool found = false;

      unsigned int _j = 0;
      itr j    = observed.begin();
      itr jend = observed.end();
      for (; j != jend; ++j) {
        if (*i == *j) {
          found = true;
          EXPECT_EQ (_i, _j) << "traversing order changed";
          break;
        }
      }

      EXPECT_TRUE (found) << "didn't find catalog: " << i->second << " "
                          << "(revision: " << i->first << ")";
    }
  }

  MockCatalog* GetCatalog(const unsigned int  revision,
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
    assert (i != root_catalogs_.end());
    return i->second;
  }

 private:
  void CheckEmpty(const std::string &str) const {
    ASSERT_FALSE (str.empty());
  }

  CatalogPathMap& GetCatalogTree(const unsigned int revision) {
    RevisionMap::iterator rev_itr = revisions_.find(revision);
    assert (rev_itr != revisions_.end());
    return rev_itr->second;
  }

  MockCatalog* GetRevisionHead(const unsigned int revision) {
    CatalogPathMap &catalogs = GetCatalogTree(revision);
    CatalogPathMap::iterator catalogs_itr = catalogs.find("");
    assert (catalogs_itr != catalogs.end());
    assert (catalogs_itr->second->revision() == revision);
    assert (catalogs_itr->second->IsRoot());
    return catalogs_itr->second;
  }

  MockCatalog* GetBranchHead(const std::string   &root_path,
                             const unsigned int   revision) {
    CatalogPathMap &catalogs = GetCatalogTree(revision);
    CatalogPathMap::iterator catalogs_itr = catalogs.find(root_path);
    assert (catalogs_itr != catalogs.end());
    assert (catalogs_itr->second->revision()  == revision);
    assert (catalogs_itr->second->root_path() == root_path);
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
     *                                                                # catalogs   root catalog hash
     *    Revision 1:   - only the root catalog (0-0)                       1      d01c7fa072d3957ea5dd323f79fa435b33375c06
     *    Revision 2:   - adds branch 1-0                                   8      ffee2bf068f3c793efa6ca0fa3bddb066541903b
     *    Revision 3:   - adds branch 1-1                                  17      c9e011bbf7529d25c958bc0f948eefef79e991cd
     *    Revision 4:   - adds branch 1-2 and branch 1-1 is recreated      25      eec5694dfe5f2055a358acfb4fda7748c896df24
     *    Revision 5:   - adds branch 1-3                                  28      3c726334c98537e92c8b92b76852f77e3a425be9
     *    Revision 6:   - removes branch 1-0                               21      MockCatalog::root_hash
     *
     */

    RootCatalogMap root_catalogs;
    root_catalogs[1] = h("d01c7fa072d3957ea5dd323f79fa435b33375c06", 'C');
    root_catalogs[2] = h("ffee2bf068f3c793efa6ca0fa3bddb066541903b", 'C');
    root_catalogs[3] = h("c9e011bbf7529d25c958bc0f948eefef79e991cd", 'C');
    root_catalogs[4] = h("eec5694dfe5f2055a358acfb4fda7748c896df24", 'C');
    root_catalogs[5] = h("3c726334c98537e92c8b92b76852f77e3a425be9", 'C');
    root_catalogs[6] = MockCatalog::root_hash;
    root_catalogs_ = root_catalogs;

    for (unsigned int r = 1; r <= max_revision; ++r) {
      MakeRevision(r);
    }

    named_snapshots_->BeginTransaction();
    EXPECT_TRUE (named_snapshots_->Insert(history::History::Tag(
                                          "Revision2", root_catalogs[2], 1337,
                                          2, t(27,11,1987), history::History::kChannelProd,
                                          "this is revision 2")));
    EXPECT_TRUE (named_snapshots_->Insert(history::History::Tag(
                                          "Revision5", root_catalogs[5], 42,
                                          5, t(11, 9,2001), history::History::kChannelProd,
                                          "this is revision 5")));
    EXPECT_TRUE (named_snapshots_->Insert(history::History::Tag(
                                          "Revision6", root_catalogs[6], 7,
                                          6, t(10, 7,2014), history::History::kChannelTrunk,
                                          "this is revision 6 - the newest!")));
    named_snapshots_->CommitTransaction();
  }

  void MakeRevision(const unsigned int revision) {
    // sanity checks
    RevisionMap::const_iterator rev_itr = revisions_.find(revision);
    ASSERT_EQ (revisions_.end(), rev_itr);
    ASSERT_LE (1u, revision);
    ASSERT_GE (max_revision, revision);

    // create map for new catalog tree
    revisions_[revision] = CatalogPathMap();

    // create the root catalog
    MockCatalog *root_catalog = CreateAndRegisterCatalog("",
                                                         revision,
                                                         NULL,
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
        root_catalog->RegisterChild(GetBranchHead("/00/10", 2));
        break;
      case 4:
        MakeBranch("/00/12", revision);
        MakeBranch("/00/11", revision);
        root_catalog->RegisterChild(GetBranchHead("/00/10", 2));
        break;
      case 5:
        MakeBranch("/00/13", revision);
        root_catalog->RegisterChild(GetBranchHead("/00/10", 2));
        root_catalog->RegisterChild(GetBranchHead("/00/11", 4));
        root_catalog->RegisterChild(GetBranchHead("/00/12", 4));
        break;
      case 6:
        root_catalog->RegisterChild(GetBranchHead("/00/11", 4));
        root_catalog->RegisterChild(GetBranchHead("/00/12", 4));
        root_catalog->RegisterChild(GetBranchHead("/00/13", 5));
        dummy_catalog_hierarchy = root_catalog; // sets current repo HEAD
        break;
      default:
        FAIL() << "hit revision: " << revision;
    }
  }

  void MakeBranch(const std::string &branch, const unsigned int revision) {
    MockCatalog *revision_root = GetRevisionHead(revision);

    if (branch == "/00/10") {
      MockCatalog *_10 = CreateAndRegisterCatalog("/00/10",          revision, revision_root);
      MockCatalog *_20 = CreateAndRegisterCatalog("/00/10/20",       revision,           _10);
                         CreateAndRegisterCatalog("/00/10/21",       revision,           _10);
      MockCatalog *_30 = CreateAndRegisterCatalog("/00/10/20/30",    revision,           _20);
                         CreateAndRegisterCatalog("/00/10/20/31",    revision,           _20);
                         CreateAndRegisterCatalog("/00/10/20/32",    revision,           _20);
                         CreateAndRegisterCatalog("/00/10/20/30/40", revision,           _30);

    } else if (branch == "/00/11") {
      MockCatalog *_11 = CreateAndRegisterCatalog("/00/11",          revision, revision_root);
      MockCatalog *_22 = CreateAndRegisterCatalog("/00/11/22",       revision,           _11);
                         CreateAndRegisterCatalog("/00/11/23",       revision,           _11);
                         CreateAndRegisterCatalog("/00/11/24",       revision,           _11);
                         CreateAndRegisterCatalog("/00/11/22/33",    revision,           _22);
      MockCatalog *_34 = CreateAndRegisterCatalog("/00/11/22/34",    revision,           _22);
                         CreateAndRegisterCatalog("/00/11/22/34/41", revision,           _34);
                         CreateAndRegisterCatalog("/00/11/22/34/42", revision,           _34);
                         CreateAndRegisterCatalog("/00/11/22/34/43", revision,           _34);

    } else if (branch == "/00/12") {
      MockCatalog *_12 = CreateAndRegisterCatalog("/00/12",          revision, revision_root);
                         CreateAndRegisterCatalog("/00/12/25",       revision,           _12);
      MockCatalog *_26 = CreateAndRegisterCatalog("/00/12/26",       revision,           _12);
                         CreateAndRegisterCatalog("/00/12/27",       revision,           _12);
                         CreateAndRegisterCatalog("/00/12/26/35",    revision,           _26);
                         CreateAndRegisterCatalog("/00/12/26/36",    revision,           _26);
                         CreateAndRegisterCatalog("/00/12/26/37",    revision,           _26);
                         CreateAndRegisterCatalog("/00/12/26/38",    revision,           _26);

    } else if (branch == "/00/13") {
      MockCatalog *_13 = CreateAndRegisterCatalog("/00/13",          revision, revision_root);
                         CreateAndRegisterCatalog("/00/13/28",       revision,          _13);
                         CreateAndRegisterCatalog("/00/13/29",       revision,          _13);

    } else {
      FAIL();
    }
  }

  MockCatalog* CreateAndRegisterCatalog(
                  const std::string  &root_path,
                  const unsigned int  revision,
                  MockCatalog        *parent       = NULL,
                  const shash::Any   &catalog_hash = shash::Any(shash::kSha1)) {
    // produce a random hash if no catalog has was given
    shash::Any effective_clg_hash = catalog_hash;
    effective_clg_hash.set_suffix(shash::kSuffixCatalog);
    if (effective_clg_hash.IsNull()) {
      effective_clg_hash.Randomize(dice_);
    }

    // get catalog tree for current revision
    CatalogPathMap &catalogs = GetCatalogTree(revision);

    // find previous catalog from the RevisionsMaps (if there is one)
    MockCatalog *previous_catalog = NULL;
    if (revision > 1) {
      RevisionMap::iterator prev_rev_itr = revisions_.find(revision - 1);
      assert (prev_rev_itr != revisions_.end());
      CatalogPathMap::iterator prev_clg_itr =
        prev_rev_itr->second.find(root_path);
      if (prev_clg_itr != prev_rev_itr->second.end()) {
        previous_catalog = prev_clg_itr->second;
      }
    }

    // produce the new catalog with references to it's predecessor and parent
    const bool is_root = (parent == NULL);
    MockCatalog *catalog = new MockCatalog(root_path,
                                           effective_clg_hash,
                                           dice_.Next(10000),
                                           revision,
                                           is_root,
                                           parent,
                                           previous_catalog);

    // register the new catalog in the data structures
    MockCatalog::RegisterCatalog(catalog);
    catalogs[root_path] = catalog;
    return catalog;
  }

  shash::Any h(const std::string &hash, const char suffix = 0) {
    return shash::Any(shash::kSha1, shash::HexPtr(hash), suffix);
  }

  time_t t(const int day, const int month, const int year) const {
    struct tm time_descriptor;

    time_descriptor.tm_hour = 0;
    time_descriptor.tm_min  = 0;
    time_descriptor.tm_sec  = 0;
    time_descriptor.tm_mday = day;
    time_descriptor.tm_mon  = month;
    time_descriptor.tm_year = year;

    return mktime(&time_descriptor);
  }

 protected:
  typedef std::map<unsigned int, shash::Any> RootCatalogMap;

 private:
  Prng                         dice_;
  RootCatalogMap               root_catalogs_;
  RevisionMap                  revisions_;
  UniquePtr<history::History>  named_snapshots_;
};

const std::string T_CatalogTraversal::sandbox = "/tmp/cvmfs_ut_catalog_traversal";
const std::string T_CatalogTraversal::fqrn    = "test.cern.ch";


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_CatalogTraversal, Initialize) {
  CatalogTraversalParams params;
  MockedCatalogTraversal traverse(params);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


CatalogIdentifiers SimpleTraversal_visited_catalogs;
void SimpleTraversalCallback(const MockedCatalogTraversal::CallbackData &data) {
  SimpleTraversal_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(), data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, SimpleTraversal) {
  SimpleTraversal_visited_catalogs.clear();
  EXPECT_EQ (0u, SimpleTraversal_visited_catalogs.size());

  CatalogTraversalParams params;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&SimpleTraversalCallback);
  const bool t1 = traverse.Traverse();
  EXPECT_TRUE (t1);

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
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


std::vector<MockCatalog*> SimpleTraversalNoCloseCallback_visited_catalogs;
void SimpleTraversalNoCloseCallback(
                             const MockedCatalogTraversal::CallbackData &data) {
  SimpleTraversalNoCloseCallback_visited_catalogs.push_back(
    const_cast<MockCatalog*>(data.catalog));
}

TEST_F(T_CatalogTraversal, SimpleTraversalNoClose) {
  SimpleTraversalNoCloseCallback_visited_catalogs.clear();
  EXPECT_EQ (0u, SimpleTraversalNoCloseCallback_visited_catalogs.size());

  CatalogTraversalParams params;
  params.no_close = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&SimpleTraversalNoCloseCallback);
  bool t1 = traverse.Traverse();
  EXPECT_TRUE (t1);

  EXPECT_EQ (21u + initial_catalog_instances, MockCatalog::instances);

  std::vector<MockCatalog*>::const_iterator i, iend;
  for (i    = SimpleTraversalNoCloseCallback_visited_catalogs.begin(),
       iend = SimpleTraversalNoCloseCallback_visited_catalogs.end();
       i != iend; ++i) {
    delete *i;
  }
  SimpleTraversalNoCloseCallback_visited_catalogs.clear();
}



//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


CatalogIdentifiers ZeroLevelHistoryTraversal_visited_catalogs;
void ZeroLevelHistoryTraversalCallback(
                             const MockedCatalogTraversal::CallbackData &data) {
  ZeroLevelHistoryTraversal_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(), data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, ZeroLevelHistoryTraversal) {
  ZeroLevelHistoryTraversal_visited_catalogs.clear();
  EXPECT_EQ (0u, ZeroLevelHistoryTraversal_visited_catalogs.size());

  CatalogTraversalParams params;
  params.history = 0;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&ZeroLevelHistoryTraversalCallback);
  const bool t1 = traverse.Traverse();
  EXPECT_TRUE (t1);

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
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


CatalogIdentifiers FirstLevelHistoryTraversal_visited_catalogs;
void FirstLevelHistoryTraversalCallback(
                             const MockedCatalogTraversal::CallbackData &data) {
  FirstLevelHistoryTraversal_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(), data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, FirstLevelHistoryTraversal) {
  FirstLevelHistoryTraversal_visited_catalogs.clear();
  EXPECT_EQ (0u, FirstLevelHistoryTraversal_visited_catalogs.size());

  CatalogTraversalParams params;
  params.history = 1;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&FirstLevelHistoryTraversalCallback);
  const bool t1 = traverse.Traverse();
  EXPECT_TRUE (t1);

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
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


std::vector<MockCatalog*> FirstLevelHistoryTraversalNoClose_visited_catalogs;
void FirstLevelHistoryTraversalNoCloseCallback(
                             const MockedCatalogTraversal::CallbackData &data) {
  FirstLevelHistoryTraversalNoClose_visited_catalogs.push_back(
                                        const_cast<MockCatalog*>(data.catalog));
}

TEST_F(T_CatalogTraversal, FirstLevelHistoryTraversalNoClose) {
  FirstLevelHistoryTraversalNoClose_visited_catalogs.clear();
  EXPECT_EQ (0u, FirstLevelHistoryTraversalNoClose_visited_catalogs.size());

  CatalogTraversalParams params;
  params.history  = 1;
  params.no_close = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&FirstLevelHistoryTraversalNoCloseCallback);
  const bool t1 = traverse.Traverse();
  EXPECT_TRUE (t1);

  EXPECT_EQ (49u + initial_catalog_instances, MockCatalog::instances);

  std::vector<MockCatalog*>::const_iterator i, iend;
  for (i    = FirstLevelHistoryTraversalNoClose_visited_catalogs.begin(),
       iend = FirstLevelHistoryTraversalNoClose_visited_catalogs.end();
       i != iend; ++i) {
    delete *i;
  }
  FirstLevelHistoryTraversalNoClose_visited_catalogs.clear();
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


CatalogIdentifiers SecondLevelHistoryTraversal_visited_catalogs;
void SecondLevelHistoryTraversalCallback(
                             const MockedCatalogTraversal::CallbackData &data) {
  SecondLevelHistoryTraversal_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(), data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, SecondLevelHistoryTraversal) {
  SecondLevelHistoryTraversal_visited_catalogs.clear();
  EXPECT_EQ (0u, SecondLevelHistoryTraversal_visited_catalogs.size());

  CatalogTraversalParams params;
  params.history = 2;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&SecondLevelHistoryTraversalCallback);
  const bool t1 = traverse.Traverse();
  EXPECT_TRUE (t1);

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

  CheckVisitedCatalogs(catalogs, SecondLevelHistoryTraversal_visited_catalogs);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


CatalogIdentifiers FullHistoryTraversal_visited_catalogs;
void FullHistoryTraversalCallback(
                             const MockedCatalogTraversal::CallbackData &data) {
  FullHistoryTraversal_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(), data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, FullHistoryTraversal) {
  FullHistoryTraversal_visited_catalogs.clear();
  EXPECT_EQ (0u, FullHistoryTraversal_visited_catalogs.size());

  CatalogTraversalParams params;
  params.history = CatalogTraversalParams::kFullHistory;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&FullHistoryTraversalCallback);
  const bool t1 = traverse.Traverse();
  EXPECT_TRUE (t1);

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
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
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
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


CatalogIdentifiers SecondLevelHistoryTraversalNoRepeat_visited_catalogs;
void SecondLevelHistoryTraversalNoRepeatCallback(
                             const MockedCatalogTraversal::CallbackData &data) {
  SecondLevelHistoryTraversalNoRepeat_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(), data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, SecondLevelHistoryTraversalNoRepeat) {
  SecondLevelHistoryTraversalNoRepeat_visited_catalogs.clear();
  EXPECT_EQ (0u, SecondLevelHistoryTraversalNoRepeat_visited_catalogs.size());

  CatalogTraversalParams params;
  params.history           = 2;
  params.no_repeat_history = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&SecondLevelHistoryTraversalNoRepeatCallback);
  const bool t1 = traverse.Traverse();
  EXPECT_TRUE (t1);

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

  CheckVisitedCatalogs(catalogs, SecondLevelHistoryTraversalNoRepeat_visited_catalogs);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


CatalogIdentifiers FullHistoryTraversalNoRepeat_visited_catalogs;
void FullHistoryTraversalNoRepeatCallback(
                             const MockedCatalogTraversal::CallbackData &data) {
  FullHistoryTraversalNoRepeat_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(), data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, FullHistoryTraversalNoRepeat) {
  FullHistoryTraversalNoRepeat_visited_catalogs.clear();
  EXPECT_EQ (0u, FullHistoryTraversalNoRepeat_visited_catalogs.size());

  CatalogTraversalParams params;
  params.history           = CatalogTraversalParams::kFullHistory;
  params.no_repeat_history = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&FullHistoryTraversalNoRepeatCallback);
  const bool t1 = traverse.Traverse();
  EXPECT_TRUE (t1);

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

  EXPECT_EQ (initial_catalog_instances, FullHistoryTraversalNoRepeat_visited_catalogs.size());

  CheckVisitedCatalogs(catalogs, FullHistoryTraversalNoRepeat_visited_catalogs);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


CatalogIdentifiers MultiTraversal_visited_catalogs;
void MultiTraversalCallback(
                             const MockedCatalogTraversal::CallbackData &data) {
  MultiTraversal_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(), data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, MultiTraversal) {
  MultiTraversal_visited_catalogs.clear();
  EXPECT_EQ (0u, MultiTraversal_visited_catalogs.size());

  CatalogIdentifiers catalogs;

  CatalogTraversalParams params;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&MultiTraversalCallback);

  const bool t1 = traverse.Traverse(GetRootHash(6));
  EXPECT_TRUE (t1);

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

  const bool t2 = traverse.Traverse(GetRootHash(4));
  EXPECT_TRUE (t2);

  catalogs.push_back(std::make_pair(4, ""));
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
  CheckVisitedCatalogs(catalogs, MultiTraversal_visited_catalogs);

  const bool t3 = traverse.Traverse(GetRootHash(2));
  EXPECT_TRUE (t3);

  catalogs.push_back(std::make_pair(2, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  CheckVisitedCatalogs(catalogs, MultiTraversal_visited_catalogs);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


CatalogIdentifiers MultiTraversalNoRepeat_visited_catalogs;
void MultiTraversalNoRepeatCallback(
                             const MockedCatalogTraversal::CallbackData &data) {
  MultiTraversalNoRepeat_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(), data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, MultiTraversalNoRepeat) {
  MultiTraversalNoRepeat_visited_catalogs.clear();
  EXPECT_EQ (0u, MultiTraversalNoRepeat_visited_catalogs.size());

  CatalogIdentifiers catalogs;

  CatalogTraversalParams params;
  params.no_repeat_history = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&MultiTraversalNoRepeatCallback);

  const bool t1 = traverse.Traverse(GetRootHash(6));
  EXPECT_TRUE (t1);

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

  const bool t2 = traverse.Traverse(GetRootHash(4));
  EXPECT_TRUE (t2);

  catalogs.push_back(std::make_pair(4, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  CheckVisitedCatalogs(catalogs, MultiTraversalNoRepeat_visited_catalogs);

  const bool t3 = traverse.Traverse(GetRootHash(2));
  EXPECT_TRUE (t3);

  catalogs.push_back(std::make_pair(2, ""));
  CheckVisitedCatalogs(catalogs, MultiTraversalNoRepeat_visited_catalogs);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


CatalogIdentifiers MultiTraversalFirstLevelHistory_visited_catalogs;
void MultiTraversalFirstLevelHistoryCallback(
                             const MockedCatalogTraversal::CallbackData &data) {
  MultiTraversalFirstLevelHistory_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(), data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, MultiTraversalFirstLevelHistory) {
  MultiTraversalFirstLevelHistory_visited_catalogs.clear();
  EXPECT_EQ (0u, MultiTraversalFirstLevelHistory_visited_catalogs.size());

  CatalogIdentifiers catalogs;

  CatalogTraversalParams params;
  params.history = 1;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&MultiTraversalFirstLevelHistoryCallback);

  const bool t1 = traverse.Traverse(GetRootHash(6));
  EXPECT_TRUE (t1);

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
  CheckVisitedCatalogs(catalogs, MultiTraversalFirstLevelHistory_visited_catalogs);

  const bool t2 = traverse.Traverse(GetRootHash(4));
  EXPECT_TRUE (t2);

  catalogs.push_back(std::make_pair(4, ""));
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
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  CheckVisitedCatalogs(catalogs, MultiTraversalFirstLevelHistory_visited_catalogs);

  const bool t3 = traverse.Traverse(GetRootHash(2));
  EXPECT_TRUE (t3);

  catalogs.push_back(std::make_pair(2, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(1, ""));
  CheckVisitedCatalogs(catalogs, MultiTraversalFirstLevelHistory_visited_catalogs);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


CatalogIdentifiers MultiTraversalFirstLevelHistoryNoRepeat_visited_catalogs;
void MultiTraversalFirstLevelHistoryNoRepeatCallback(
                             const MockedCatalogTraversal::CallbackData &data) {
  MultiTraversalFirstLevelHistoryNoRepeat_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(), data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, MultiTraversalFirstLevelHistoryNoRepeat) {
  MultiTraversalFirstLevelHistoryNoRepeat_visited_catalogs.clear();
  EXPECT_EQ (0u, MultiTraversalFirstLevelHistoryNoRepeat_visited_catalogs.size());

  CatalogIdentifiers catalogs;

  CatalogTraversalParams params;
  params.history           = 1;
  params.no_repeat_history = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&MultiTraversalFirstLevelHistoryNoRepeatCallback);

  const bool t1 = traverse.Traverse(GetRootHash(6));
  EXPECT_TRUE (t1);

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
  CheckVisitedCatalogs(catalogs, MultiTraversalFirstLevelHistoryNoRepeat_visited_catalogs);

  const bool t2 = traverse.Traverse(GetRootHash(4));
  EXPECT_TRUE (t2);

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
  CheckVisitedCatalogs(catalogs, MultiTraversalFirstLevelHistoryNoRepeat_visited_catalogs);

  const bool t3 = traverse.Traverse(GetRootHash(2));
  EXPECT_TRUE (t3);

  catalogs.push_back(std::make_pair(2, ""));
  catalogs.push_back(std::make_pair(1, ""));
  CheckVisitedCatalogs(catalogs, MultiTraversalFirstLevelHistoryNoRepeat_visited_catalogs);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


CatalogIdentifiers EmptyTraversePruned_visited_catalogs;
void EmptyTraversePrunedCallback(
                             const MockedCatalogTraversal::CallbackData &data) {
  EmptyTraversePruned_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(), data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, EmptyTraversePruned) {
  EmptyTraversePruned_visited_catalogs.clear();
  EXPECT_EQ (0u, EmptyTraversePruned_visited_catalogs.size());

  CatalogIdentifiers catalogs;

  CatalogTraversalParams params;
  params.history           = 0;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&EmptyTraversePrunedCallback);
  const bool t1 = traverse.TraversePruned();

  EXPECT_FALSE (t1);
  EXPECT_EQ (0u, traverse.pruned_revision_count());
  CheckVisitedCatalogs(catalogs, EmptyTraversePruned_visited_catalogs);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


CatalogIdentifiers TraversePrunedAfterSimpleTraversal_visited_catalogs;
void TraversePrunedAfterSimpleTraversalCallback(
                             const MockedCatalogTraversal::CallbackData &data) {
  TraversePrunedAfterSimpleTraversal_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(), data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, TraversePrunedAfterSimpleTraversal) {
  TraversePrunedAfterSimpleTraversal_visited_catalogs.clear();
  EXPECT_EQ (0u, TraversePrunedAfterSimpleTraversal_visited_catalogs.size());

  CatalogIdentifiers catalogs;

  CatalogTraversalParams params;
  params.history = 0;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&TraversePrunedAfterSimpleTraversalCallback);

  const bool t1 = traverse.Traverse();
  EXPECT_TRUE (t1);

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

  CheckVisitedCatalogs(catalogs, TraversePrunedAfterSimpleTraversal_visited_catalogs);
  EXPECT_EQ (1u, traverse.pruned_revision_count());

  const bool t2 = traverse.TraversePruned();
  EXPECT_TRUE (t2);

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
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(2, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(1, ""));

  CheckVisitedCatalogs(catalogs, TraversePrunedAfterSimpleTraversal_visited_catalogs);
  EXPECT_EQ (0u, traverse.pruned_revision_count());
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


CatalogIdentifiers TraversePrunedAfterSimpleTraversalNoRepeat_visited_catalogs;
void TraversePrunedAfterSimpleTraversalNoRepeatCallback(
                             const MockedCatalogTraversal::CallbackData &data) {
  TraversePrunedAfterSimpleTraversalNoRepeat_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(), data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, TraversePrunedAfterSimpleTraversalNoRepeat) {
  TraversePrunedAfterSimpleTraversalNoRepeat_visited_catalogs.clear();
  EXPECT_EQ (0u, TraversePrunedAfterSimpleTraversalNoRepeat_visited_catalogs.size());

  CatalogIdentifiers catalogs;

  CatalogTraversalParams params;
  params.history           = 0;
  params.no_repeat_history = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&TraversePrunedAfterSimpleTraversalNoRepeatCallback);

  const bool t1 = traverse.Traverse();
  EXPECT_TRUE (t1);

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

  CheckVisitedCatalogs(catalogs, TraversePrunedAfterSimpleTraversalNoRepeat_visited_catalogs);
  EXPECT_EQ (1u, traverse.pruned_revision_count());

  const bool t2 = traverse.TraversePruned();
  EXPECT_TRUE (t2);

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

  CheckVisitedCatalogs(catalogs, TraversePrunedAfterSimpleTraversalNoRepeat_visited_catalogs);
  EXPECT_EQ (0u, traverse.pruned_revision_count());
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


CatalogIdentifiers TraversePrunedAfterSecondLevelHistoryTraversalNoRepeat_visited_catalogs;
void TraversePrunedAfterSecondLevelHistoryTraversalNoRepeatCallback(
                             const MockedCatalogTraversal::CallbackData &data) {
  TraversePrunedAfterSecondLevelHistoryTraversalNoRepeat_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(), data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, TraversePrunedAfterSecondLevelHistoryTraversalNoRepeat) {
  TraversePrunedAfterSecondLevelHistoryTraversalNoRepeat_visited_catalogs.clear();
  EXPECT_EQ (0u, TraversePrunedAfterSecondLevelHistoryTraversalNoRepeat_visited_catalogs.size());

  CatalogIdentifiers catalogs;

  CatalogTraversalParams params;
  params.history           = 2;
  params.no_repeat_history = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&TraversePrunedAfterSecondLevelHistoryTraversalNoRepeatCallback);

  const bool t1 = traverse.Traverse();
  EXPECT_TRUE (t1);

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

  CheckVisitedCatalogs(catalogs, TraversePrunedAfterSecondLevelHistoryTraversalNoRepeat_visited_catalogs);
  EXPECT_EQ (1u, traverse.pruned_revision_count());

  const bool t2 = traverse.TraversePruned();
  EXPECT_TRUE (t2);

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

  CheckVisitedCatalogs(catalogs, TraversePrunedAfterSecondLevelHistoryTraversalNoRepeat_visited_catalogs);
  EXPECT_EQ (0u, traverse.pruned_revision_count());
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


CatalogIdentifiers TraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs;
void TraversePrunedAfterMultiTraversalNoRepeatCallback(
                             const MockedCatalogTraversal::CallbackData &data) {
  TraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(), data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, TraversePrunedAfterMultiTraversalNoRepeat) {
  TraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs.clear();
  EXPECT_EQ (0u, TraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs.size());

  CatalogIdentifiers catalogs;

  CatalogTraversalParams params;
  params.history           = 0;
  params.no_repeat_history = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&TraversePrunedAfterMultiTraversalNoRepeatCallback);

  const bool t1 = traverse.Traverse(GetRootHash(6));
  EXPECT_TRUE (t1);

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
  CheckVisitedCatalogs(catalogs, TraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs);

  const bool t2 = traverse.Traverse(GetRootHash(4));
  EXPECT_TRUE (t2);

  catalogs.push_back(std::make_pair(4, ""));
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  CheckVisitedCatalogs(catalogs, TraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs);

  const bool t3 = traverse.Traverse(GetRootHash(2));
  EXPECT_TRUE (t3);

  catalogs.push_back(std::make_pair(2, ""));
  CheckVisitedCatalogs(catalogs, TraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs);

  EXPECT_EQ (3u, traverse.pruned_revision_count());

  const bool t4 = traverse.TraversePruned();
  EXPECT_TRUE (t4);

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

  EXPECT_EQ (0u, traverse.pruned_revision_count());
  EXPECT_EQ (initial_catalog_instances, TraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs.size());
  CheckVisitedCatalogs(catalogs, TraversePrunedAfterMultiTraversalNoRepeat_visited_catalogs);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


CatalogIdentifiers TraverseRepositoryTagList_visited_catalogs;
void TraverseRepositoryTagListCallback(
                             const MockedCatalogTraversal::CallbackData &data) {
  TraverseRepositoryTagList_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(), data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, TraverseRepositoryTagList) {
  TraverseRepositoryTagList_visited_catalogs.clear();
  EXPECT_EQ (0u, TraverseRepositoryTagList_visited_catalogs.size());

  CatalogIdentifiers catalogs;

  CatalogTraversalParams params;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&TraverseRepositoryTagListCallback);

  const bool t1 = traverse.TraverseNamedSnapshots();
  EXPECT_TRUE (t1);

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
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


CatalogIdentifiers TraverseRepositoryTagListSecondHistoryLevel_visited_catalogs;
void TraverseRepositoryTagListSecondHistoryLevelCallback(
                             const MockedCatalogTraversal::CallbackData &data) {
  TraverseRepositoryTagListSecondHistoryLevel_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(), data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, TraverseRepositoryTagListSecondHistoryLevel) {
  TraverseRepositoryTagListSecondHistoryLevel_visited_catalogs.clear();
  EXPECT_EQ (0u, TraverseRepositoryTagListSecondHistoryLevel_visited_catalogs.size());

  CatalogIdentifiers catalogs;

  CatalogTraversalParams params;
  params.history = 2;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&TraverseRepositoryTagListSecondHistoryLevelCallback);

  const bool t1 = traverse.TraverseNamedSnapshots();
  EXPECT_TRUE (t1);

  catalogs.push_back(std::make_pair(2, ""));                // Revision 2 ... 1
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(1, ""));
  catalogs.push_back(std::make_pair(5, ""));                // Revision 5 ... 3
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
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(6, ""));                // Revision 6 ... 4
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

  CheckVisitedCatalogs(catalogs, TraverseRepositoryTagListSecondHistoryLevel_visited_catalogs);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


CatalogIdentifiers TraverseRepositoryTagListSecondHistoryLevelNoRepeat_visited_catalogs;
void TraverseRepositoryTagListSecondHistoryLevelNoRepeatCallback(
                             const MockedCatalogTraversal::CallbackData &data) {
  TraverseRepositoryTagListSecondHistoryLevelNoRepeat_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(), data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, TraverseRepositoryTagListSecondHistoryLevelNoRepeat) {
  TraverseRepositoryTagListSecondHistoryLevelNoRepeat_visited_catalogs.clear();
  EXPECT_EQ (0u, TraverseRepositoryTagListSecondHistoryLevelNoRepeat_visited_catalogs.size());

  CatalogIdentifiers catalogs;

  CatalogTraversalParams params;
  params.history           = 2;
  params.no_repeat_history = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&TraverseRepositoryTagListSecondHistoryLevelNoRepeatCallback);

  const bool t1 = traverse.TraverseNamedSnapshots();
  EXPECT_TRUE (t1);

  catalogs.push_back(std::make_pair(2, ""));                // Revision 2 ... 1
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(1, ""));
  catalogs.push_back(std::make_pair(5, ""));                // Revision 5 ... 3
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
  catalogs.push_back(std::make_pair(6, ""));                // Revision 6 ... 4

  CheckVisitedCatalogs(catalogs, TraverseRepositoryTagListSecondHistoryLevelNoRepeat_visited_catalogs);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


CatalogIdentifiers TraverseRepositoryTagListFirstLevelHistoryTraversePrunedNoRepeat_visited_catalogs;
void TraverseRepositoryTagListFirstLevelHistoryTraversePrunedNoRepeatCallback(
                             const MockedCatalogTraversal::CallbackData &data) {
  TraverseRepositoryTagListFirstLevelHistoryTraversePrunedNoRepeat_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(), data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, TraverseRepositoryTagListFirstLevelHistoryTraversePrunedNoRepeat) {
  TraverseRepositoryTagListFirstLevelHistoryTraversePrunedNoRepeat_visited_catalogs.clear();
  EXPECT_EQ (0u, TraverseRepositoryTagListFirstLevelHistoryTraversePrunedNoRepeat_visited_catalogs.size());

  CatalogIdentifiers catalogs;

  CatalogTraversalParams params;
  params.history           = 1;
  params.no_repeat_history = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&TraverseRepositoryTagListFirstLevelHistoryTraversePrunedNoRepeatCallback);

  const bool t1 = traverse.TraverseNamedSnapshots();
  EXPECT_TRUE (t1);

  catalogs.push_back(std::make_pair(2, ""));                // Revision 2 ... 1
  catalogs.push_back(std::make_pair(2, "/00/10"));
  catalogs.push_back(std::make_pair(2, "/00/10/21"));
  catalogs.push_back(std::make_pair(2, "/00/10/20"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/32"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/31"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30"));
  catalogs.push_back(std::make_pair(2, "/00/10/20/30/40"));
  catalogs.push_back(std::make_pair(1, ""));
  catalogs.push_back(std::make_pair(5, ""));                // Revision 5 ... 4
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
  catalogs.push_back(std::make_pair(4, ""));
  catalogs.push_back(std::make_pair(6, ""));                // Revision 6 ... 5

  CheckVisitedCatalogs(catalogs, TraverseRepositoryTagListFirstLevelHistoryTraversePrunedNoRepeat_visited_catalogs);

  const bool t2 = traverse.TraversePruned();
  EXPECT_TRUE (t2);

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

  CheckVisitedCatalogs(catalogs, TraverseRepositoryTagListFirstLevelHistoryTraversePrunedNoRepeat_visited_catalogs);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


CatalogIdentifiers TraverseUntilUnavailableRevisionNoRepeat_visited_catalogs;
void TraverseUntilUnavailableRevisionNoRepeatCallback(
                             const MockedCatalogTraversal::CallbackData &data) {
  TraverseUntilUnavailableRevisionNoRepeat_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(), data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, TraverseUntilUnavailableRevisionNoRepeat) {
  TraverseUntilUnavailableRevisionNoRepeat_visited_catalogs.clear();
  EXPECT_EQ (0u, TraverseUntilUnavailableRevisionNoRepeat_visited_catalogs.size());

  std::set<shash::Any> deleted_catalogs;
  deleted_catalogs.insert(GetRootHash(1));
  deleted_catalogs.insert(GetRootHash(2));
  deleted_catalogs.insert(GetRootHash(3));
  deleted_catalogs.insert(GetRootHash(4));
  MockObjectFetcher::deleted_catalogs = &deleted_catalogs;

  CatalogIdentifiers catalogs;

  CatalogTraversalParams params;
  params.history             = 4;
  params.no_repeat_history   = true;
  params.ignore_load_failure = true;
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&TraverseUntilUnavailableRevisionNoRepeatCallback);

  const bool t1 = traverse.Traverse();
  EXPECT_TRUE (t1);

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

  CheckVisitedCatalogs(catalogs, TraverseUntilUnavailableRevisionNoRepeat_visited_catalogs);

  MockObjectFetcher::deleted_catalogs = NULL;
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


CatalogIdentifiers TraverseWithUnavailableNestedNoRepeat_visited_catalogs;
void TraverseWithUnavailableNestedNoRepeatCallback(
                             const MockedCatalogTraversal::CallbackData &data) {
  TraverseWithUnavailableNestedNoRepeat_visited_catalogs.push_back(
    std::make_pair(data.catalog->GetRevision(), data.catalog->path().ToString()));
}

TEST_F(T_CatalogTraversal, TraverseWithUnavailableNestedNoRepeat) {
  TraverseWithUnavailableNestedNoRepeat_visited_catalogs.clear();
  EXPECT_EQ (0u, TraverseWithUnavailableNestedNoRepeat_visited_catalogs.size());

  MockCatalog* doomed_nested_catalog = GetCatalog(2, "/00/10/20");
  ASSERT_NE (static_cast<MockCatalog*>(NULL), doomed_nested_catalog);

  std::set<shash::Any> deleted_catalogs;
  deleted_catalogs.insert(doomed_nested_catalog->catalog_hash());
  MockObjectFetcher::deleted_catalogs = &deleted_catalogs;

  CatalogIdentifiers catalogs;

  CatalogTraversalParams params;
  params.history             = 4;
  params.quiet               = true;
  params.no_repeat_history   = true;
  params.ignore_load_failure = true; // even though load failures should be
                                     // ignored, traversal is supposed to fail
                                     // because a missing nested catalog is con-
                                     // sidered an error!
  MockedCatalogTraversal traverse(params);
  traverse.RegisterListener(&TraverseWithUnavailableNestedNoRepeatCallback);

  const bool t1 = traverse.Traverse();
  EXPECT_FALSE (t1);

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

  const bool dont_check_catalog_count = false;
  CheckVisitedCatalogs(catalogs, TraverseWithUnavailableNestedNoRepeat_visited_catalogs,
                       dont_check_catalog_count);

  MockObjectFetcher::deleted_catalogs = NULL;
}
