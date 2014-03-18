#include <gtest/gtest.h>

#include "../../cvmfs/catalog_traversal.h"

using namespace swissknife;


/**
 * This is a mock of an ObjectFetcher that does essentially nothing.
 */
class MockObjectFetcher {
 public:
  MockObjectFetcher(const CatalogTraversalParams &params) {}
 public:
  manifest::Manifest* FetchManifest() {
    return NULL;
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
  struct NestedCatalog {
    PathString path;
    shash::Any hash;
    uint64_t size;
  };
  typedef std::vector<NestedCatalog> NestedCatalogList;

 public:
  static MockCatalog* AttachFreely(const std::string  &root_path,
                                   const std::string  &file,
                                   const shash::Any   &catalog_hash,
                                         MockCatalog  *parent = NULL) {
    return NULL;
  }

  bool IsRoot() const {
    return true;
  }

  NestedCatalogList *ListNestedCatalogs() const {

    return NULL;
  }

  shash::Any GetPreviousRevision() const {
    return shash::Any();
  }
};



TEST(T_CatalogTraversal, Initialize) {
  CatalogTraversalParams params;

  CatalogTraversal<MockCatalog, MockObjectFetcher> traverse(params);
  traverse.Traverse();
}
