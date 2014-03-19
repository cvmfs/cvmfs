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
    MockCatalog *head_root = CreateAndRegisterCatalog("", root_hash, 42);
    shash::Any other;
    other.Randomize();
    MockCatalog *minus1_root = CreateAndRegisterCatalog("", other, 1337, head_root);
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
