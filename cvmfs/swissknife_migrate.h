/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_MIGRATE_H_
#define CVMFS_SWISSKNIFE_MIGRATE_H_

#include "swissknife.h"

#include <map>
#include <string>
#include <vector>

#include "atomic.h"
#include "catalog.h"
#include "catalog_traversal.h"
#include "hash.h"
#include "uid_map.h"
#include "upload.h"
#include "util.h"
#include "util_concurrency.h"

namespace catalog {
class WritableCatalog;
}

namespace swissknife {

class CommandMigrate : public Command {
 protected:
  struct CatalogStatistics {
    CatalogStatistics()
      : max_row_id(0)
      , entry_count(0)
      , hardlink_group_count(0)
      , aggregated_linkcounts(0)
      , migration_time(0.0) { }
    unsigned int max_row_id;
    unsigned int entry_count;

    unsigned int hardlink_group_count;
    unsigned int aggregated_linkcounts;

    double       migration_time;

    std::string root_path;
  };

  class CatalogStatisticsList : protected std::vector<CatalogStatistics>,
                                public    Lockable {
    friend class CommandMigrate;

   public:
    inline void Insert(const CatalogStatistics &statistics) {
      LockGuard<CatalogStatisticsList> lock(this);
      this->push_back(statistics);
    }
  };

 public:
  struct PendingCatalog;
  typedef std::vector<PendingCatalog *> PendingCatalogList;
  struct PendingCatalog {
    explicit PendingCatalog(const catalog::Catalog *old_catalog = NULL) :
      success(false),
      old_catalog(old_catalog),
      new_catalog(NULL) { }
    virtual ~PendingCatalog();

    inline const std::string root_path() const {
      return old_catalog->path().ToString();
    }
    inline bool IsRoot() const { return old_catalog->IsRoot(); }
    inline bool HasNew() const { return new_catalog != NULL;   }

    inline bool HasChanges() const {
      return (new_catalog != NULL ||
              old_catalog->database().GetModifiedRowCount() > 0);
    }

    inline shash::Any GetOldContentHash() const {
      return old_catalog->hash();
    }

    bool                              success;

    const catalog::Catalog           *old_catalog;
    catalog::WritableCatalog         *new_catalog;

    PendingCatalogList                nested_catalogs;
    Future<catalog::DirectoryEntry>   root_entry;
    Future<catalog::DeltaCounters>    nested_statistics;

    CatalogStatistics                 statistics;

    // Note: As soon as the `was_updated` future is set to 'true', both
    //       `new_catalog_hash` and `new_catalog_size` are assumed to be set
    //       accordingly. If it is set to 'false' they will be ignored.
    Future<bool>                      was_updated;
    shash::Any                        new_catalog_hash;
    size_t                            new_catalog_size;
  };

  class PendingCatalogMap : public std::map<std::string, const PendingCatalog*>,
                            public Lockable {};

  template<class DerivedT>
  class AbstractMigrationWorker : public ConcurrentWorker<DerivedT> {
   public:
    typedef CommandMigrate::PendingCatalog* expected_data;
    typedef CommandMigrate::PendingCatalog* returned_data;

    struct worker_context {
      worker_context(const std::string  &temporary_directory,
                     const bool          collect_catalog_statistics) :
        temporary_directory(temporary_directory),
        collect_catalog_statistics(collect_catalog_statistics) {}
      const std::string  temporary_directory;
      const bool         collect_catalog_statistics;
    };

   public:
    explicit AbstractMigrationWorker(const worker_context *context);
    virtual ~AbstractMigrationWorker();

    void operator()(const expected_data &data);

   protected:
    bool RunMigration(PendingCatalog *data) const { return false; }

    bool UpdateNestedCatalogReferences(PendingCatalog *data) const;
    bool CleanupNestedCatalogs(PendingCatalog *data) const;
    bool CollectAndAggregateStatistics(PendingCatalog *data) const;

    catalog::WritableCatalog*
      GetWritable(const catalog::Catalog *catalog) const;

   protected:
    const std::string  temporary_directory_;
    const bool         collect_catalog_statistics_;

    StopWatch          migration_stopwatch_;
  };

  class MigrationWorker_20x :
    public AbstractMigrationWorker<MigrationWorker_20x>
  {
    friend class AbstractMigrationWorker<MigrationWorker_20x>;
   protected:
    static const float    kSchema;
    static const unsigned kSchemaRevision;

   public:
    struct worker_context :
      AbstractMigrationWorker<MigrationWorker_20x>::worker_context
    {
      worker_context(const std::string  &temporary_directory,
                     const bool          collect_catalog_statistics,
                     const bool          fix_nested_catalog_transitions,
                     const bool          analyze_file_linkcounts,
                     const uid_t         uid,
                     const gid_t         gid)
        : AbstractMigrationWorker<MigrationWorker_20x>::worker_context(
            temporary_directory, collect_catalog_statistics)
        , fix_nested_catalog_transitions(fix_nested_catalog_transitions)
        , analyze_file_linkcounts(analyze_file_linkcounts)
        , uid(uid)
        , gid(gid) { }
      const bool  fix_nested_catalog_transitions;
      const bool  analyze_file_linkcounts;
      const uid_t uid;
      const gid_t gid;
    };

   public:
    explicit MigrationWorker_20x(const worker_context *context);

   protected:
    bool RunMigration(PendingCatalog *data) const;

    bool CreateNewEmptyCatalog(PendingCatalog *data) const;
    bool CheckDatabaseSchemaCompatibility(PendingCatalog *data) const;
    bool AttachOldCatalogDatabase(PendingCatalog *data) const;
    bool StartDatabaseTransaction(PendingCatalog *data) const;
    bool MigrateFileMetadata(PendingCatalog *data) const;
    bool AnalyzeFileLinkcounts(PendingCatalog *data) const;
    bool RemoveDanglingNestedMountpoints(PendingCatalog *data) const;
    bool MigrateNestedCatalogMountPoints(PendingCatalog *data) const;
    bool FixNestedCatalogTransitionPoints(PendingCatalog *data) const;
    bool GenerateCatalogStatistics(PendingCatalog *data) const;
    bool FindRootEntryInformation(PendingCatalog *data) const;
    bool CommitDatabaseTransaction(PendingCatalog *data) const;
    bool DetachOldCatalogDatabase(PendingCatalog *data) const;

   private:
    const bool  fix_nested_catalog_transitions_;
    const bool  analyze_file_linkcounts_;
    const uid_t uid_;
    const gid_t gid_;
  };

  class MigrationWorker_217 :
    public AbstractMigrationWorker<MigrationWorker_217>
  {
    friend class AbstractMigrationWorker<MigrationWorker_217>;

   public:
    explicit MigrationWorker_217(const worker_context *context);

   protected:
    bool RunMigration(PendingCatalog *data) const;

    bool CheckDatabaseSchemaCompatibility(PendingCatalog *data) const;
    bool StartDatabaseTransaction(PendingCatalog *data) const;
    bool GenerateNewStatisticsCounters(PendingCatalog *data) const;
    bool UpdateCatalogSchema(PendingCatalog *data) const;
    bool CommitDatabaseTransaction(PendingCatalog *data) const;
  };

  class ChownMigrationWorker :
    public AbstractMigrationWorker<ChownMigrationWorker>
  {
    friend class AbstractMigrationWorker<ChownMigrationWorker>;
   public:
    struct worker_context :
      AbstractMigrationWorker<ChownMigrationWorker>::worker_context
    {
      worker_context(const std::string  &temporary_directory,
                     const bool          collect_catalog_statistics,
                     const UidMap       &uid_map,
                     const GidMap       &gid_map)
        : AbstractMigrationWorker<ChownMigrationWorker>::worker_context(
            temporary_directory, collect_catalog_statistics)
        , uid_map(uid_map)
        , gid_map(gid_map) { }
      const UidMap &uid_map;
      const GidMap &gid_map;
    };

   public:
    explicit ChownMigrationWorker(const worker_context *context);

   protected:
    bool RunMigration(PendingCatalog *data) const;
    bool ApplyPersonaMappings(PendingCatalog *data) const;

   private:
    template <class MapT>
    std::string GenerateMappingStatement(const MapT         &map,
                                         const std::string  &column) const;

   private:
    const std::string uid_map_statement_;
    const std::string gid_map_statement_;
  };

  class HardlinkRemovalMigrationWorker :
    public AbstractMigrationWorker<HardlinkRemovalMigrationWorker>
  {
    friend class AbstractMigrationWorker<HardlinkRemovalMigrationWorker>;

   public:
    explicit HardlinkRemovalMigrationWorker(const worker_context *context) :
      AbstractMigrationWorker<HardlinkRemovalMigrationWorker>(context) {}

   protected:
    bool RunMigration(PendingCatalog *data) const;

    bool CheckDatabaseSchemaCompatibility(PendingCatalog *data) const;
    bool BreakUpHardlinks(PendingCatalog *data) const;
  };

 public:
  CommandMigrate();
  ~CommandMigrate() { }
  std::string GetName() { return "migrate"; }
  std::string GetDescription() {
    return "CernVM-FS catalog repository migration \n"
      "This command migrates the whole catalog structure of a given repository";
  }
  ParameterList GetParams();

  int Main(const ArgumentList &args);

  static void FixNestedCatalogTransitionPoint(
    const catalog::DirectoryEntry &nested_root,
    catalog::DirectoryEntry *mountpoint);
  static const catalog::DirectoryEntry& GetNestedCatalogMarkerDirent();

 protected:
  template <class ObjectFetcherT>
  bool LoadCatalogs(ObjectFetcherT *object_fetcher) {
    typename CatalogTraversal<ObjectFetcherT>::Parameters params;
    const bool generate_full_catalog_tree = true;
    params.no_close       = generate_full_catalog_tree;
    params.object_fetcher = object_fetcher;
    CatalogTraversal<ObjectFetcherT> traversal(params);
    traversal.RegisterListener(&CommandMigrate::CatalogCallback, this);

    return traversal.Traverse();
  }

  void CatalogCallback(
    const CatalogTraversalData<catalog::WritableCatalog> &data);
  void MigrationCallback(PendingCatalog *const &data);
  void UploadCallback(const upload::SpoolerResult &result);

  void PrintStatusMessage(const PendingCatalog *catalog,
                          const shash::Any     &content_hash,
                          const std::string    &message);

  template <class MigratorT>
  bool DoMigrationAndCommit(const std::string                   &manifest_path,
                            typename MigratorT::worker_context  *context);

  template <class MigratorT>
  void ConvertCatalogsRecursively(PendingCatalog *catalog, MigratorT *migrator);
  bool RaiseFileDescriptorLimit() const;
  bool ConfigureSQLite() const;
  void AnalyzeCatalogStatistics() const;
  bool ReadPersona(const std::string &uid, const std::string &gid);
  bool ReadPersonaMaps(const std::string &uid_map_path,
                       const std::string &gid_map_path,
                             UidMap      *uid_map,
                             GidMap      *gid_map) const;

  bool GenerateNestedCatalogMarkerChunk();
  void CreateNestedCatalogMarkerDirent(const shash::Any &content_hash);

 private:
  unsigned int           file_descriptor_limit_;
  CatalogStatisticsList  catalog_statistics_list_;
  unsigned int           catalog_count_;
  atomic_int32           catalogs_processed_;
  bool                   has_committed_new_revision_;

  uid_t                  uid_;
  gid_t                  gid_;

  std::string                     temporary_directory_;
  std::string                     nested_catalog_marker_tmp_path_;
  static catalog::DirectoryEntry  nested_catalog_marker_;

  catalog::Catalog const*     root_catalog_;
  UniquePtr<upload::Spooler>  spooler_;
  PendingCatalogMap           pending_catalogs_;

  StopWatch  catalog_loading_stopwatch_;
  StopWatch  migration_stopwatch_;
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_MIGRATE_H_
