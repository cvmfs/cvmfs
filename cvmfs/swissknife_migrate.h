/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_MIGRATE_H_
#define CVMFS_SWISSKNIFE_MIGRATE_H_

#include "swissknife.h"

#include "hash.h"
#include "util_concurrency.h"
#include "catalog.h"

namespace catalog {
  class WritableCatalog;
}

namespace swissknife {

class CommandMigrate : public Command {
 protected:
  typedef
    std::vector<Future<catalog::Catalog::NestedCatalog>* >
    FutureNestedCatalogList;

  class MigrationWorker : public ConcurrentWorker<MigrationWorker> {
   public:
    struct expected_data {
      expected_data(
        const catalog::Catalog                         *catalog,
              Future<catalog::Catalog::NestedCatalog>  *new_catalog,
        const FutureNestedCatalogList                  &future_nested_catalogs) :
        catalog(catalog),
        new_catalog(new_catalog),
        future_nested_catalogs(future_nested_catalogs) {}
      expected_data() :
        catalog(NULL),
        new_catalog(NULL) {}

      const catalog::Catalog                         *catalog;
            Future<catalog::Catalog::NestedCatalog>  *new_catalog;
      const FutureNestedCatalogList                   future_nested_catalogs;
    };

    struct returned_data {

    };

    struct worker_context {
      worker_context(const std::string temporary_directory) :
        temporary_directory(temporary_directory) {}

      const std::string temporary_directory;
    };

   public:
    MigrationWorker(const worker_context *context);
    virtual ~MigrationWorker();

    void operator()(const expected_data &data);

   protected:
    catalog::WritableCatalog* CreateNewEmptyCatalog(
                                            const std::string &root_path) const;
    bool MigrateFileMetadata(const catalog::Catalog    *catalog,
                             catalog::WritableCatalog  *writable_catalog) const;

   private:
    const std::string temporary_directory_;
  };


 public:
  CommandMigrate();
  ~CommandMigrate() { };
  std::string GetName() { return "migrate"; };
  std::string GetDescription() {
    return "CernVM-FS catalog repository migration \n"
      "This command migrates the whole catalog structure of a given repository";
  };
  ParameterList GetParams();

  int Main(const ArgumentList &args);

  void MigrationCallback(const MigrationWorker::returned_data &data);

 protected:
  void CatalogCallback(const catalog::Catalog* catalog,
                       const hash::Any&        catalog_hash,
                       const unsigned          tree_level);

  void ConvertCatalogsRecursively(
              const catalog::Catalog                         *catalog,
                    Future<catalog::Catalog::NestedCatalog>  *new_catalog);
  void ConvertCatalog(
        const catalog::Catalog                         *catalog,
              Future<catalog::Catalog::NestedCatalog>  *new_catalog,
        const FutureNestedCatalogList                  &future_nested_catalogs);

 private:
  bool              print_tree_;
  bool              print_hash_;

  catalog::Catalog const*                        root_catalog_;
  UniquePtr<ConcurrentWorkers<MigrationWorker> > concurrent_migration;
};

}

#endif  // CVMFS_SWISSKNIFE_MIGRATE_H_
