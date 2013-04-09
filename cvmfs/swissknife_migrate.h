/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_MIGRATE_H_
#define CVMFS_SWISSKNIFE_MIGRATE_H_

#include "swissknife.h"

#include "hash.h"
#include "util_concurrency.h"
#include "catalog.h"
#include "upload.h"

#include <map>

namespace catalog {
  class WritableCatalog;
}

namespace swissknife {

class CommandMigrate : public Command {
 protected:
  struct NestedCatalogReference : public catalog::Catalog::NestedCatalog {
    unsigned int            mountpoint_linkcount;
    catalog::DeltaCounters  nested_statistics;
  };

  typedef Future<NestedCatalogReference> FutureNestedCatalogReference;
  typedef std::vector<FutureNestedCatalogReference* >
          FutureNestedCatalogReferenceList;

  struct PendingCatalog {
    PendingCatalog(const bool                     success           = false,
                   const unsigned int             mntpnt_lnkcnt     = 0,
                   const catalog::DeltaCounters  &nested_statistics =
                                                       catalog::DeltaCounters(),
                   catalog::WritableCatalog      *new_catalog       = NULL,
                   FutureNestedCatalogReference  *new_nested_ref    = NULL) :
      success(success),
      mountpoint_linkcount(mntpnt_lnkcnt),
      nested_statistics(nested_statistics),
      new_catalog(new_catalog),
      new_nested_ref(new_nested_ref) {}

    bool                           success;
    unsigned int                   mountpoint_linkcount;
    catalog::DeltaCounters         nested_statistics;
    catalog::WritableCatalog      *new_catalog;
    FutureNestedCatalogReference  *new_nested_ref;
  };

  class PendingCatalogMap : public std::map<std::string, PendingCatalog>,
                            public Lockable {};

  class MigrationWorker : public ConcurrentWorker<MigrationWorker> {
   public:
    struct expected_data {
      expected_data(
        const catalog::Catalog                  *catalog,
              FutureNestedCatalogReference      *new_nested_ref,
        const FutureNestedCatalogReferenceList  &future_nested_catalogs) :
        catalog(catalog),
        new_nested_ref(new_nested_ref),
        future_nested_catalogs(future_nested_catalogs) {}
      expected_data() :
        catalog(NULL),
        new_nested_ref(NULL) {}

      const catalog::Catalog                  *catalog;
            FutureNestedCatalogReference      *new_nested_ref;
      const FutureNestedCatalogReferenceList   future_nested_catalogs;
    };

    typedef PendingCatalog returned_data;

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
    bool CheckDatabaseSchemaCompatibility(
                                    const catalog::Database &new_catalog,
                                    const catalog::Database &old_catalog) const;
    bool AttachOldCatalogDatabase(const catalog::Database &new_catalog,
                                  const catalog::Database &old_catalog) const;
    bool MigrateFileMetadata(const catalog::Catalog    *catalog,
                             catalog::WritableCatalog  *writable_catalog) const;
    bool MigrateNestedCatalogReferences(
         const catalog::WritableCatalog          *writable_catalog,
         const FutureNestedCatalogReferenceList  &future_nested_catalogs) const;
    bool GenerateCatalogStatistics(
         catalog::WritableCatalog                *writable_catalog,
         const FutureNestedCatalogReferenceList  &future_nested_catalogs,
         catalog::DeltaCounters                  *nested_statistics) const;
    bool FindMountpointLinkcount(
                   const catalog::WritableCatalog  *writable_catalog,
                   unsigned int                    *mountpoint_linkcount) const;

   private:
    void SqlError(const std::string &message,
                  const catalog::Sql &statement) const;

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


 protected:
  void CatalogCallback(const catalog::Catalog* catalog,
                       const hash::Any&        catalog_hash,
                       const unsigned          tree_level);
  void MigrationCallback(const PendingCatalog &data);
  void UploadCallback(const upload::SpoolerResult &result);

  void ConvertCatalogsRecursively(
              const catalog::Catalog              *catalog,
                    FutureNestedCatalogReference  *new_catalog);
  void ConvertCatalog(
        const catalog::Catalog                  *catalog,
              FutureNestedCatalogReference      *new_catalog,
        const FutureNestedCatalogReferenceList  &future_nested_catalogs);

 private:
  bool              print_tree_;
  bool              print_hash_;

  catalog::Catalog const*                        root_catalog_;
  UniquePtr<ConcurrentWorkers<MigrationWorker> > concurrent_migration;
  UniquePtr<upload::Spooler>                     spooler_;
  PendingCatalogMap                              pending_catalogs_;
};

}

#endif  // CVMFS_SWISSKNIFE_MIGRATE_H_
