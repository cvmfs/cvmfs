#include <gtest/gtest.h>
#include <string>
#include <map>
#include <iostream> // TODO: remove me!

#include "../../cvmfs/catalog_traversal.h"
#include "../../cvmfs/manifest.h"
#include "../../cvmfs/hash.h"

using namespace swissknife;

static const std::string rhs       = "f9d87ae2cc46be52b324335ff05fae4c1a7c4dd4";
static const shash::Any  root_hash = shash::Any(shash::kSha1, shash::HexPtr(rhs));

/**
 * This is a mock of an ObjectFetcher that does essentially nothing.
 */
class MockObjectFetcher {
 public:
  MockObjectFetcher(const CatalogTraversalParams &params) {}
 public:
  manifest::Manifest* FetchManifest() {
    return new manifest::Manifest(root_hash, 0, "");
  }
  inline bool Fetch(const shash::Any  &catalog_hash,
                    std::string       *catalog_file) {
    return true;
  }
  inline bool Exists(const std::string &file) {
    return false;
  }
};


/**
 * This is a minimal mock of a Catalog class.
 */
class MockCatalog {
 public:
  typedef std::map<shash::Any, MockCatalog*> AvailableCatalogs;
  static AvailableCatalogs available_catalogs;

  static void RegisterCatalog(MockCatalog *catalog) {
    ASSERT_EQ (MockCatalog::available_catalogs.end(),
               MockCatalog::available_catalogs.find(catalog->catalog_hash()));
    MockCatalog::available_catalogs[catalog->catalog_hash()] = catalog;
  }

  static void UnregisterCatalogs() {
    MockCatalog::AvailableCatalogs::const_iterator i, iend;
    for (i    = MockCatalog::available_catalogs.begin(),
         iend = MockCatalog::available_catalogs.end();
         i != iend; ++i)
    {
      delete i->second;
    }
    MockCatalog::available_catalogs.clear();
  }

  static MockCatalog* GetCatalog(const shash::Any &catalog_hash) {
    AvailableCatalogs::const_iterator clg_itr =
      MockCatalog::available_catalogs.find(catalog_hash);
    return (MockCatalog::available_catalogs.end() != clg_itr)
      ? clg_itr->second
      : NULL;
  }

 public:
  struct NestedCatalog {
    PathString   path;
    shash::Any   hash;
    MockCatalog *child;
    uint64_t     size;
  };
  typedef std::vector<NestedCatalog> NestedCatalogList;

 public:
  MockCatalog(const std::string &root_path,
              const shash::Any  &catalog_hash,
              const uint64_t     catalog_size,
              MockCatalog *parent = NULL,
              MockCatalog *previous = NULL) :
    parent_(parent), previous_(previous), root_path_(root_path),
    catalog_hash_(catalog_hash), catalog_size_(catalog_size)
  {
    if (parent != NULL) {
      parent->RegisterChild(this);
    }
  }

 public: /* API in this 'public block' is used by CatalogTraversal
          * (see catalog.h - catalog::Catalog for details)
          */
  static MockCatalog* AttachFreely(const std::string  &root_path,
                                   const std::string  &file,
                                   const shash::Any   &catalog_hash,
                                         MockCatalog  *parent = NULL) {
    const MockCatalog *catalog = MockCatalog::GetCatalog(catalog_hash);
    if (catalog == NULL) {
      return NULL;
    } else {
      return catalog->Clone();
    }
  }

  bool IsRoot() const {
    return (parent_ == NULL);
  }

  NestedCatalogList *ListNestedCatalogs() const {
    return const_cast<NestedCatalogList*>(&children_);
  }

  shash::Any GetPreviousRevision() const {
    return (previous_ != NULL) ? previous_->catalog_hash() : shash::Any();
  }

 public:
  const std::string& root_path()    const { return root_path_;    }
  const shash::Any&  catalog_hash() const { return catalog_hash_; }
  uint64_t           catalog_size() const { return catalog_size_; }

 protected:
  void RegisterChild(MockCatalog *child) {
    NestedCatalog nested;
    nested.path  = PathString(child->root_path());
    nested.hash  = child->catalog_hash();
    nested.child = child;
    nested.size  = child->catalog_size();
    children_.push_back(nested);
  }

  MockCatalog* Clone() const {
    MockCatalog *new_catalog = new MockCatalog(root_path_, catalog_hash_,
                                               catalog_size_, parent_,
                                               previous_);
    new_catalog->children_ = children_;
    return new_catalog;
  }

 private:
  MockCatalog        *parent_;
  MockCatalog        *previous_;
  const std::string   root_path_;
  const shash::Any    catalog_hash_;
  const uint64_t      catalog_size_;

  NestedCatalogList   children_;
};

MockCatalog::AvailableCatalogs MockCatalog::available_catalogs;


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//

typedef CatalogTraversal<MockCatalog, MockObjectFetcher> MockedCatalogTraversal;

class T_CatalogTraversal : public ::testing::Test {
 public:
  MockCatalog *dummy_catalog_hierarchy;

 protected:
  void SetUp() {
    SetupDummyCatalogs();
  }

  void TearDown() {
    MockCatalog::UnregisterCatalogs();
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
     *    Revision 1:   - only the root catalog (0-0)
     *    Revision 2:   - adds branch 1-0
     *    Revision 3:   - adds branch 1-1
     *    Revision 4:   - adds branch 1-2 and branch 1-1 is recreated
     *    Revision 5:   - adds branch 1-3
     *
     */
    MockCatalog *head = CreateAndRegisterCatalog("", root_hash, 42);

    Make_10_Branch(head);
    Make_11_Branch(head);
    Make_12_Branch(head);
    Make_13_Branch(head);
  }

  void Make_10_Branch(MockCatalog *parent) {
    MockCatalog *_10 = CreateAndRegisterCatalog("/00/10",          GetRandomHash(), 13124, parent);
    MockCatalog *_20 = CreateAndRegisterCatalog("/00/10/20",       GetRandomHash(), 23524, _10);
    MockCatalog *_21 = CreateAndRegisterCatalog("/00/10/21",       GetRandomHash(), 74546, _10);
    MockCatalog *_30 = CreateAndRegisterCatalog("/00/10/20/30",    GetRandomHash(), 66234, _20);
    MockCatalog *_31 = CreateAndRegisterCatalog("/00/10/20/31",    GetRandomHash(), 87365, _20);
    MockCatalog *_32 = CreateAndRegisterCatalog("/00/10/20/32",    GetRandomHash(), 93405, _20);
    MockCatalog *_40 = CreateAndRegisterCatalog("/00/10/20/30/40", GetRandomHash(), 85617, _30);
  }

  void Make_11_Branch(MockCatalog *parent) {
    MockCatalog *_11 = CreateAndRegisterCatalog("/00/11",          GetRandomHash(), 87648, parent);
    MockCatalog *_22 = CreateAndRegisterCatalog("/00/11/22",       GetRandomHash(), 86546, _11);
    MockCatalog *_23 = CreateAndRegisterCatalog("/00/11/23",       GetRandomHash(), 98565, _11);
    MockCatalog *_24 = CreateAndRegisterCatalog("/00/11/24",       GetRandomHash(), 45271, _11);
    MockCatalog *_33 = CreateAndRegisterCatalog("/00/11/22/33",    GetRandomHash(), 17412, _22);
    MockCatalog *_34 = CreateAndRegisterCatalog("/00/11/22/34",    GetRandomHash(), 89127, _22);
    MockCatalog *_41 = CreateAndRegisterCatalog("/00/11/22/34/41", GetRandomHash(), 10987, _34);
    MockCatalog *_42 = CreateAndRegisterCatalog("/00/11/22/34/42", GetRandomHash(), 40987, _34);
    MockCatalog *_43 = CreateAndRegisterCatalog("/00/11/22/34/43", GetRandomHash(), 12234, _34);
  }

  void Make_12_Branch(MockCatalog *parent) {
    MockCatalog *_12 = CreateAndRegisterCatalog("/00/12",       GetRandomHash(), 39272, parent);
    MockCatalog *_25 = CreateAndRegisterCatalog("/00/12/25",    GetRandomHash(), 91999, _12);
    MockCatalog *_26 = CreateAndRegisterCatalog("/00/12/26",    GetRandomHash(), 11111, _12);
    MockCatalog *_27 = CreateAndRegisterCatalog("/00/12/27",    GetRandomHash(), 12344, _12);
    MockCatalog *_35 = CreateAndRegisterCatalog("/00/12/26/35", GetRandomHash(), 99992, _26);
    MockCatalog *_36 = CreateAndRegisterCatalog("/00/12/26/36", GetRandomHash(), 12333, _26);
    MockCatalog *_37 = CreateAndRegisterCatalog("/00/12/26/37", GetRandomHash(), 23442, _26);
    MockCatalog *_38 = CreateAndRegisterCatalog("/00/12/26/38", GetRandomHash(), 14112, _26);
  }

  void Make_13_Branch(MockCatalog *parent) {
    MockCatalog *_13 = CreateAndRegisterCatalog("/00/13",    GetRandomHash(), 14120, parent);
    MockCatalog *_28 = CreateAndRegisterCatalog("/00/13/28", GetRandomHash(), 92370, _13);
    MockCatalog *_29 = CreateAndRegisterCatalog("/00/13/29", GetRandomHash(), 14122, _13);
  }

 private:
  MockCatalog* CreateAndRegisterCatalog(const std::string &root_path,
                                        const shash::Any  &catalog_hash,
                                        const uint64_t     catalog_size,
                                        MockCatalog       *parent   = NULL,
                                        MockCatalog       *previous = NULL) {
    MockCatalog *catalog = new MockCatalog(root_path, catalog_hash, catalog_size,
                                           parent, previous);
    MockCatalog::RegisterCatalog(catalog);
    return catalog;
  }

  shash::Any GetRandomHash() const {
    shash::Any hash(shash::kSha1);
    hash.Randomize();
    return hash;
  }
};


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


  CatalogTraversal<MockCatalog, MockObjectFetcher> traverse(params);
  traverse.Traverse();
}
