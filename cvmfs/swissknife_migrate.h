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
  struct PendingCatalog;
  typedef std::vector<PendingCatalog*> PendingCatalogList;
  struct PendingCatalog {
    PendingCatalog(const catalog::Catalog *old_catalog = NULL) :
      success(false),
      old_catalog(old_catalog),
      new_catalog(NULL) {}

    const std::string root_path() const {
      return old_catalog->path().ToString();
    }

    bool                              success;

    const catalog::Catalog           *old_catalog;
    catalog::WritableCatalog         *new_catalog;

    PendingCatalogList                nested_catalogs;
    Future<unsigned int>              mountpoint_linkcount;
    Future<catalog::DeltaCounters>    nested_statistics;

    Future<hash::Any>                 new_catalog_hash;
  };

  class PendingCatalogMap : public std::map<std::string, const PendingCatalog*>,
                            public Lockable {};

  class MigrationWorker : public ConcurrentWorker<MigrationWorker> {
   public:
    typedef CommandMigrate::PendingCatalog* expected_data;
    typedef CommandMigrate::PendingCatalog* returned_data;

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
    bool CreateNewEmptyCatalog            (PendingCatalog *data) const;
    bool CheckDatabaseSchemaCompatibility (PendingCatalog *data) const;
    bool AttachOldCatalogDatabase         (PendingCatalog *data) const;
    bool MigrateFileMetadata              (PendingCatalog *data) const;
    bool MigrateNestedCatalogReferences   (PendingCatalog *data) const;
    bool GenerateCatalogStatistics        (PendingCatalog *data) const;
    bool FindMountpointLinkcount          (PendingCatalog *data) const;
    bool DetachOldCatalogDatabase         (PendingCatalog *data) const;

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
  void MigrationCallback(PendingCatalog *const &data);
  void UploadCallback(const upload::SpoolerResult &result);

  void ConvertCatalogsRecursively(PendingCatalog *catalog);

 private:
  bool              print_tree_;
  bool              print_hash_;

  catalog::Catalog const*                        root_catalog_;
  UniquePtr<ConcurrentWorkers<MigrationWorker> > concurrent_migration_;
  UniquePtr<upload::Spooler>                     spooler_;
  PendingCatalogMap                              pending_catalogs_;
};

}

#endif  // CVMFS_SWISSKNIFE_MIGRATE_H_
