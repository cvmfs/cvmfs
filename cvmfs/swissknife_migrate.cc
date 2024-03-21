/**
 * This file is part of the CernVM File System.
 *
 * Careful: any real schema migration as of now requires taking care of
 * hash algorithm
 */

#include "swissknife_migrate.h"

#include <sys/resource.h>
#include <unistd.h>

#include "catalog_rw.h"
#include "catalog_sql.h"
#include "catalog_virtual.h"
#include "compression.h"
#include "crypto/hash.h"
#include "swissknife_history.h"
#include "util/concurrency.h"
#include "util/logging.h"

using namespace std;  // NOLINT

namespace swissknife {

catalog::DirectoryEntry  CommandMigrate::nested_catalog_marker_;

CommandMigrate::CommandMigrate() :
  file_descriptor_limit_(8192),
  catalog_count_(0),
  has_committed_new_revision_(false),
  uid_(0),
  gid_(0),
  root_catalog_(NULL)
{
  atomic_init32(&catalogs_processed_);
}


ParameterList CommandMigrate::GetParams() const {
  ParameterList r;
  r.push_back(Parameter::Mandatory('v',
    "migration base version ( 2.0.x | 2.1.7 | chown | hardlink | bulkhash | "
    "stats)"));
  r.push_back(Parameter::Mandatory('r',
    "repository URL (absolute local path or remote URL)"));
  r.push_back(Parameter::Mandatory('u', "upstream definition string"));
  r.push_back(Parameter::Mandatory('o', "manifest output file"));
  r.push_back(Parameter::Mandatory('t',
    "temporary directory for catalog decompress"));
  r.push_back(Parameter::Optional('p',
    "user id to be used for this repository"));
  r.push_back(Parameter::Optional('g',
    "group id to be used for this repository"));
  r.push_back(Parameter::Optional('n', "fully qualified repository name"));
  r.push_back(Parameter::Optional('h', "root hash (other than trunk)"));
  r.push_back(Parameter::Optional('k', "repository master key(s)"));
  r.push_back(Parameter::Optional('i', "UID map for chown"));
  r.push_back(Parameter::Optional('j', "GID map for chown"));
  r.push_back(Parameter::Optional('@', "proxy url"));
  r.push_back(Parameter::Switch('f', "fix nested catalog transition points"));
  r.push_back(Parameter::Switch('l', "disable linkcount analysis of files"));
  r.push_back(Parameter::Switch('s',
    "enable collection of catalog statistics"));
  return r;
}


static void Error(const std::string &message) {
  LogCvmfs(kLogCatalog, kLogStderr, "%s", message.c_str());
}


static void Error(const std::string                     &message,
                  const CommandMigrate::PendingCatalog  *catalog) {
  const std::string err_msg = message + "\n"
                              "Catalog: " + catalog->root_path();
  Error(err_msg);
}


static void Error(const std::string                     &message,
                  const catalog::SqlCatalog             &statement,
                  const CommandMigrate::PendingCatalog  *catalog) {
  const std::string err_msg =
    message + "\n"
    "SQLite: " + StringifyInt(statement.GetLastError()) +
    " - " + statement.GetLastErrorMsg();
  Error(err_msg, catalog);
}


int CommandMigrate::Main(const ArgumentList &args) {
  shash::Any manual_root_hash;
  const std::string &migration_base     = *args.find('v')->second;
  const std::string &repo_url           = *args.find('r')->second;
  const std::string &spooler            = *args.find('u')->second;
  const std::string &manifest_path      = *args.find('o')->second;
  const std::string &tmp_dir            = *args.find('t')->second;
  const std::string &uid                = (args.count('p') > 0)      ?
                                             *args.find('p')->second :
                                             "";
  const std::string &gid                = (args.count('g') > 0)      ?
                                             *args.find('g')->second :
                                             "";
  const std::string &repo_name          = (args.count('n') > 0)      ?
                                             *args.find('n')->second :
                                             "";
  const std::string &repo_keys          = (args.count('k') > 0)      ?
                                             *args.find('k')->second :
                                             "";
  const std::string &uid_map_path       = (args.count('i') > 0)      ?
                                             *args.find('i')->second :
                                             "";
  const std::string &gid_map_path       = (args.count('j') > 0)      ?
                                             *args.find('j')->second :
                                             "";
  const bool fix_transition_points      = (args.count('f') > 0);
  const bool analyze_file_linkcounts    = (args.count('l') == 0);
  const bool collect_catalog_statistics = (args.count('s') > 0);
  if (args.count('h') > 0) {
    manual_root_hash = shash::MkFromHexPtr(shash::HexPtr(
      *args.find('h')->second), shash::kSuffixCatalog);
  }

  // We might need a lot of file descriptors
  if (!RaiseFileDescriptorLimit()) {
    Error("Failed to raise file descriptor limits");
    return 2;
  }

  // Put SQLite into multithreaded mode
  if (!ConfigureSQLite()) {
    Error("Failed to preconfigure SQLite library");
    return 3;
  }

  // Create an upstream spooler
  temporary_directory_ = tmp_dir;
  const upload::SpoolerDefinition spooler_definition(spooler, shash::kSha1);
  spooler_ = upload::Spooler::Construct(spooler_definition);
  if (!spooler_.IsValid()) {
    Error("Failed to create upstream Spooler.");
    return 5;
  }
  spooler_->RegisterListener(&CommandMigrate::UploadCallback, this);

  // Load the full catalog hierarchy
  LogCvmfs(kLogCatalog, kLogStdout, "Loading current catalog tree...");

  catalog_loading_stopwatch_.Start();
  bool loading_successful = false;
  if (IsHttpUrl(repo_url)) {
    typedef HttpObjectFetcher<catalog::WritableCatalog> ObjectFetcher;

    const bool follow_redirects = false;
    const string proxy = (args.count('@') > 0) ? *args.find('@')->second : "";
    if (!this->InitDownloadManager(follow_redirects, proxy) ||
        !this->InitSignatureManager(repo_keys)) {
      LogCvmfs(kLogCatalog, kLogStderr, "Failed to init repo connection");
      return 1;
    }

    ObjectFetcher fetcher(repo_name,
                          repo_url,
                          tmp_dir,
                          download_manager(),
                          signature_manager());

    loading_successful = LoadCatalogs(manual_root_hash, &fetcher);
  } else {
    typedef LocalObjectFetcher<catalog::WritableCatalog> ObjectFetcher;
    ObjectFetcher fetcher(repo_url, tmp_dir);
    loading_successful = LoadCatalogs(manual_root_hash, &fetcher);
  }
  catalog_loading_stopwatch_.Stop();

  if (!loading_successful) {
    Error("Failed to load catalog tree");
    return 4;
  }

  LogCvmfs(kLogCatalog, kLogStdout, "Loaded %d catalogs", catalog_count_);
  assert(root_catalog_ != NULL);

  // Do the actual migration step
  bool migration_succeeded = false;
  if (migration_base == "2.0.x") {
    if (!ReadPersona(uid, gid)) {
      return 1;
    }

    // Generate and upload a nested catalog marker
    if (!GenerateNestedCatalogMarkerChunk()) {
      Error("Failed to create a nested catalog marker.");
      return 6;
    }
    spooler_->WaitForUpload();

    // Configure the concurrent catalog migration facility
    MigrationWorker_20x::worker_context context(temporary_directory_,
                                                collect_catalog_statistics,
                                                fix_transition_points,
                                                analyze_file_linkcounts,
                                                uid_,
                                                gid_);
    migration_succeeded =
      DoMigrationAndCommit<MigrationWorker_20x>(manifest_path, &context);
  } else if (migration_base == "2.1.7") {
    MigrationWorker_217::worker_context context(temporary_directory_,
                                                collect_catalog_statistics);
    migration_succeeded =
      DoMigrationAndCommit<MigrationWorker_217>(manifest_path, &context);
  } else if (migration_base == "chown") {
    UidMap uid_map;
    GidMap gid_map;
    if (!ReadPersonaMaps(uid_map_path, gid_map_path, &uid_map, &gid_map)) {
      Error("Failed to read UID and/or GID map");
      return 1;
    }
    ChownMigrationWorker::worker_context context(temporary_directory_,
                                                 collect_catalog_statistics,
                                                 uid_map,
                                                 gid_map);
    migration_succeeded =
      DoMigrationAndCommit<ChownMigrationWorker>(manifest_path, &context);
  } else if (migration_base == "hardlink") {
    HardlinkRemovalMigrationWorker::worker_context
      context(temporary_directory_, collect_catalog_statistics);
    migration_succeeded =
      DoMigrationAndCommit<HardlinkRemovalMigrationWorker>(manifest_path,
                                                           &context);
  } else if (migration_base == "bulkhash") {
    BulkhashRemovalMigrationWorker::worker_context
      context(temporary_directory_, collect_catalog_statistics);
    migration_succeeded =
      DoMigrationAndCommit<BulkhashRemovalMigrationWorker>(manifest_path,
                                                           &context);
  } else if (migration_base == "stats") {
    StatsMigrationWorker::worker_context context(
      temporary_directory_, collect_catalog_statistics);
    migration_succeeded =
      DoMigrationAndCommit<StatsMigrationWorker>(manifest_path, &context);
  } else {
    const std::string err_msg = "Unknown migration base: " + migration_base;
    Error(err_msg);
    return 1;
  }

  // Check if everything went well
  if (!migration_succeeded) {
    Error("Migration failed!");
    return 5;
  }

  // Analyze collected statistics
  if (collect_catalog_statistics && has_committed_new_revision_) {
    LogCvmfs(kLogCatalog, kLogStdout, "\nCollected statistics results:");
    AnalyzeCatalogStatistics();
  }

  LogCvmfs(kLogCatalog, kLogStdout, "\nCatalog Migration succeeded");
  return 0;
}


bool CommandMigrate::ReadPersona(const std::string &uid,
                                   const std::string &gid) {
  if (uid.empty()) {
    Error("Please provide a user ID");
    return false;
  }
  if (gid.empty()) {
    Error("Please provide a group ID");
    return false;
  }

  uid_ = String2Int64(uid);
  gid_ = String2Int64(gid);
  return true;
}



bool CommandMigrate::ReadPersonaMaps(const std::string &uid_map_path,
                                     const std::string &gid_map_path,
                                           UidMap      *uid_map,
                                           GidMap      *gid_map) const {
  if (!uid_map->Read(uid_map_path) || !uid_map->IsValid()) {
    Error("Failed to read UID map");
    return false;
  }

  if (!gid_map->Read(gid_map_path) || !gid_map->IsValid()) {
    Error("Failed to read GID map");
    return false;
  }

  if (uid_map->RuleCount() == 0 && !uid_map->HasDefault()) {
    Error("UID map appears to be empty");
    return false;
  }

  if (gid_map->RuleCount() == 0 && !gid_map->HasDefault()) {
    Error("GID map appears to be empty");
    return false;
  }

  return true;
}


void CommandMigrate::UploadHistoryClosure(
  const upload::SpoolerResult &result,
  Future<shash::Any> *hash)
{
  assert(!result.IsChunked());
  if (result.return_code != 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to upload history database (%d)",
             result.return_code);
    hash->Set(shash::Any());
  } else {
    hash->Set(result.content_hash);
  }
}


bool CommandMigrate::UpdateUndoTags(
  PendingCatalog *root_catalog,
  uint64_t revision,
  time_t timestamp,
  shash::Any *history_hash)
{
  string filename_old = history_upstream_->filename();
  string filename_new = filename_old + ".new";
  bool retval = CopyPath2Path(filename_old, filename_new);
  if (!retval) return false;
  UniquePtr<history::SqliteHistory> history(
    history::SqliteHistory::OpenWritable(filename_new));
  history->TakeDatabaseFileOwnership();

  history::History::Tag tag_trunk;
  bool exists = history->GetByName(CommandTag::kHeadTag, &tag_trunk);
  if (exists) {
    retval = history->Remove(CommandTag::kHeadTag);
    if (!retval) return false;

    history::History::Tag tag_trunk_previous = tag_trunk;
    tag_trunk_previous.name = CommandTag::kPreviousHeadTag;
    tag_trunk_previous.description = CommandTag::kPreviousHeadTagDescription;
    history->Remove(CommandTag::kPreviousHeadTag);

    tag_trunk.root_hash = root_catalog->new_catalog_hash;
    tag_trunk.size = root_catalog->new_catalog_size;
    tag_trunk.revision = revision;
    tag_trunk.timestamp = timestamp;

    retval = history->Insert(tag_trunk_previous);
    if (!retval) return false;
    retval = history->Insert(tag_trunk);
    if (!retval) return false;
  }

  history->SetPreviousRevision(manifest_upstream_->history());
  history->DropDatabaseFileOwnership();
  history.Destroy();

  Future<shash::Any> history_hash_new;
  upload::Spooler::CallbackPtr callback = spooler_->RegisterListener(
    &CommandMigrate::UploadHistoryClosure, this, &history_hash_new);
  spooler_->ProcessHistory(filename_new);
  spooler_->WaitForUpload();
  spooler_->UnregisterListener(callback);
  unlink(filename_new.c_str());
  *history_hash = history_hash_new.Get();
  if (history_hash->IsNull()) {
    Error("failed to upload tag database");
    return false;
  }

  return true;
}


template <class MigratorT>
bool CommandMigrate::DoMigrationAndCommit(
  const std::string                   &manifest_path,
  typename MigratorT::worker_context  *context
) {
  // Create a concurrent migration context for catalog migration
  const unsigned int cpus = GetNumberOfCpuCores();
  ConcurrentWorkers<MigratorT> concurrent_migration(cpus, cpus * 10, context);

  if (!concurrent_migration.Initialize()) {
    Error("Failed to initialize worker migration system.");
    return false;
  }
  concurrent_migration.RegisterListener(&CommandMigrate::MigrationCallback,
                                         this);

  // Migrate catalogs recursively (starting with the deepest nested catalogs)
  LogCvmfs(kLogCatalog, kLogStdout, "\nMigrating catalogs...");
  PendingCatalog *root_catalog = new PendingCatalog(root_catalog_);
  migration_stopwatch_.Start();
  ConvertCatalogsRecursively(root_catalog, &concurrent_migration);
  concurrent_migration.WaitForEmptyQueue();
  spooler_->WaitForUpload();
  spooler_->UnregisterListeners();
  migration_stopwatch_.Stop();

  // check for possible errors during the migration process
  const unsigned int errors = concurrent_migration.GetNumberOfFailedJobs() +
                              spooler_->GetNumberOfErrors();
  LogCvmfs(kLogCatalog, kLogStdout,
           "Catalog Migration finished with %d errors.", errors);
  if (errors > 0) {
    LogCvmfs(kLogCatalog, kLogStdout,
             "\nCatalog Migration produced errors\nAborting...");
    return false;
  }

  if (root_catalog->was_updated.Get()) {
    LogCvmfs(kLogCatalog, kLogStdout,
             "\nCommitting migrated repository revision...");
    manifest::Manifest manifest = *manifest_upstream_;
    manifest.set_catalog_hash(root_catalog->new_catalog_hash);
    manifest.set_catalog_size(root_catalog->new_catalog_size);
    manifest.set_root_path(root_catalog->root_path());
    const catalog::Catalog* new_catalog = (root_catalog->HasNew())
                                          ? root_catalog->new_catalog
                                          : root_catalog->old_catalog;
    manifest.set_ttl(new_catalog->GetTTL());
    manifest.set_revision(new_catalog->GetRevision());

    // Commit the new (migrated) repository revision...
    if (history_upstream_.IsValid()) {
      shash::Any history_hash(manifest_upstream_->history());
      LogCvmfs(kLogCatalog, kLogStdout | kLogNoLinebreak,
               "Updating repository tag database... ");
      if (!UpdateUndoTags(root_catalog,
                          new_catalog->GetRevision(),
                          new_catalog->GetLastModified(),
                          &history_hash))
      {
        Error("Updating tag database failed.\nAborting...");
        return false;
      }
      manifest.set_history(history_hash);
      LogCvmfs(kLogCvmfs, kLogStdout, "%s", history_hash.ToString().c_str());
    }

    if (!manifest.Export(manifest_path)) {
      Error("Manifest export failed.\nAborting...");
      return false;
    }
    has_committed_new_revision_ = true;
  } else {
    LogCvmfs(kLogCatalog, kLogStdout,
             "\nNo catalogs migrated, skipping the commit...");
  }

  // Get rid of the open root catalog
  delete root_catalog;

  return true;
}


void CommandMigrate::CatalogCallback(
                   const CatalogTraversalData<catalog::WritableCatalog> &data) {
  std::string tree_indent;
  std::string hash_string;
  std::string path;

  for (unsigned int i = 1; i < data.tree_level; ++i) {
    tree_indent += "\u2502  ";
  }

  if (data.tree_level > 0) {
    tree_indent += "\u251C\u2500 ";
  }

  hash_string = data.catalog_hash.ToString();

  path = data.catalog->mountpoint().ToString();
  if (path.empty()) {
    path = "/";
    root_catalog_ = data.catalog;
  }

  LogCvmfs(kLogCatalog, kLogStdout, "%s%s %s",
    tree_indent.c_str(),
    hash_string.c_str(),
    path.c_str());

  ++catalog_count_;
}


void CommandMigrate::MigrationCallback(PendingCatalog *const &data) {
  // Check if the migration of the catalog was successful
  if (!data->success) {
    Error("Catalog migration failed! Aborting...");
    exit(1);
    return;
  }

  if (!data->HasChanges()) {
    PrintStatusMessage(data, data->GetOldContentHash(), "preserved");
    data->was_updated.Set(false);
    return;
  }

  const string &path = (data->HasNew()) ? data->new_catalog->database_path()
                                        : data->old_catalog->database_path();

  // Save the processed catalog in the pending map
  {
    LockGuard<PendingCatalogMap> guard(&pending_catalogs_);
    assert(pending_catalogs_.find(path) == pending_catalogs_.end());
    pending_catalogs_[path] = data;
  }
  catalog_statistics_list_.Insert(data->statistics);

  // check the size of the uncompressed catalog file
  size_t new_catalog_size = GetFileSize(path);
  if (new_catalog_size <= 0) {
    Error("Failed to get uncompressed file size of catalog!", data);
    exit(2);
    return;
  }
  data->new_catalog_size = new_catalog_size;

  // Schedule the compression and upload of the catalog
  spooler_->ProcessCatalog(path);
}


void CommandMigrate::UploadCallback(const upload::SpoolerResult &result) {
  const string &path = result.local_path;

  // Check if the upload was successful
  if (result.return_code != 0) {
    Error("Failed to upload file " + path + "\nAborting...");
    exit(2);
    return;
  }
  assert(result.file_chunks.size() == 0);

  // Remove the just uploaded file
  unlink(path.c_str());

  // Uploaded nested catalog marker... generate and cache DirectoryEntry for it
  if (path == nested_catalog_marker_tmp_path_) {
    CreateNestedCatalogMarkerDirent(result.content_hash);
    return;
  } else {
    // Find the catalog path in the pending catalogs and remove it from the list
    PendingCatalog *catalog;
    {
      LockGuard<PendingCatalogMap> guard(&pending_catalogs_);
      PendingCatalogMap::iterator i = pending_catalogs_.find(path);
      assert(i != pending_catalogs_.end());
      catalog = const_cast<PendingCatalog*>(i->second);
      pending_catalogs_.erase(i);
    }

    PrintStatusMessage(catalog, result.content_hash, "migrated and uploaded");

    // The catalog is completely processed... fill the content_hash to allow the
    // processing of parent catalogs (Notified by 'was_updated'-future)
    // NOTE: From now on, this PendingCatalog structure could be deleted and
    //       should not be used anymore!
    catalog->new_catalog_hash = result.content_hash;
    catalog->was_updated.Set(true);
  }
}


void CommandMigrate::PrintStatusMessage(const PendingCatalog *catalog,
                                        const shash::Any     &content_hash,
                                        const std::string    &message) {
  atomic_inc32(&catalogs_processed_);
  const unsigned int processed = (atomic_read32(&catalogs_processed_) * 100) /
                                  catalog_count_;
  LogCvmfs(kLogCatalog, kLogStdout, "[%d%%] %s %sC %s",
           processed,
           message.c_str(),
           content_hash.ToString().c_str(),
           catalog->root_path().c_str());
}


template <class MigratorT>
void CommandMigrate::ConvertCatalogsRecursively(PendingCatalog *catalog,
                                                MigratorT       *migrator) {
  // First migrate all nested catalogs (depth first traversal)
  const catalog::CatalogList nested_catalogs =
    catalog->old_catalog->GetChildren();
  catalog::CatalogList::const_iterator i    = nested_catalogs.begin();
  catalog::CatalogList::const_iterator iend = nested_catalogs.end();
  catalog->nested_catalogs.reserve(nested_catalogs.size());
  for (; i != iend; ++i) {
    PendingCatalog *new_nested = new PendingCatalog(*i);
    catalog->nested_catalogs.push_back(new_nested);
    ConvertCatalogsRecursively(new_nested, migrator);
  }

  // Migrate this catalog referencing all its (already migrated) children
  migrator->Schedule(catalog);
}


bool CommandMigrate::RaiseFileDescriptorLimit() const {
  struct rlimit rpl;
  memset(&rpl, 0, sizeof(rpl));
  getrlimit(RLIMIT_NOFILE, &rpl);
  if (rpl.rlim_cur < file_descriptor_limit_) {
    if (rpl.rlim_max < file_descriptor_limit_)
      rpl.rlim_max = file_descriptor_limit_;
    rpl.rlim_cur = file_descriptor_limit_;
    const bool retval = setrlimit(RLIMIT_NOFILE, &rpl);
    if (retval != 0) {
      return false;
    }
  }
  return true;
}


bool CommandMigrate::ConfigureSQLite() const {
  int retval = sqlite3_config(SQLITE_CONFIG_MULTITHREAD);
  return (retval == SQLITE_OK);
}


void CommandMigrate::AnalyzeCatalogStatistics() const {
  const unsigned int number_of_catalogs = catalog_statistics_list_.size();
  unsigned int       aggregated_entry_count = 0;
  unsigned int       aggregated_max_row_id = 0;
  unsigned int       aggregated_hardlink_count = 0;
  unsigned int       aggregated_linkcounts = 0;
  double             aggregated_migration_time = 0.0;

  CatalogStatisticsList::const_iterator i    = catalog_statistics_list_.begin();
  CatalogStatisticsList::const_iterator iend = catalog_statistics_list_.end();
  for (; i != iend; ++i) {
    aggregated_entry_count    += i->entry_count;
    aggregated_max_row_id     += i->max_row_id;
    aggregated_hardlink_count += i->hardlink_group_count;
    aggregated_linkcounts     += i->aggregated_linkcounts;
    aggregated_migration_time += i->migration_time;
  }

  // Inode quantization
  assert(aggregated_max_row_id > 0);
  const unsigned int unused_inodes =
                                 aggregated_max_row_id - aggregated_entry_count;
  const float ratio =
    (static_cast<float>(unused_inodes) /
     static_cast<float>(aggregated_max_row_id)) * 100.0f;
  LogCvmfs(kLogCatalog, kLogStdout, "Actual Entries:                %d\n"
                                    "Allocated Inodes:              %d\n"
                                    "  Unused Inodes:               %d\n"
                                    "  Percentage of wasted Inodes: %.1f%%\n",
           aggregated_entry_count, aggregated_max_row_id, unused_inodes, ratio);

  // Hardlink statistics
  const float average_linkcount = (aggregated_hardlink_count > 0)
                                  ? aggregated_linkcounts /
                                    aggregated_hardlink_count
                                  : 0.0f;
  LogCvmfs(kLogCatalog, kLogStdout, "Generated Hardlink Groups:     %d\n"
                                    "Average Linkcount per Group:   %.1f\n",
           aggregated_hardlink_count, average_linkcount);

  // Performance measures
  const double average_migration_time =
    aggregated_migration_time / static_cast<double>(number_of_catalogs);
  LogCvmfs(kLogCatalog, kLogStdout, "Catalog Loading Time:          %.2fs\n"
                                    "Average Migration Time:        %.2fs\n"
                                    "Overall Migration Time:        %.2fs\n"
                                    "Aggregated Migration Time:     %.2fs\n",
           catalog_loading_stopwatch_.GetTime(),
           average_migration_time,
           migration_stopwatch_.GetTime(),
           aggregated_migration_time);
}


CommandMigrate::PendingCatalog::~PendingCatalog() {
  delete old_catalog;
  old_catalog = NULL;

  if (new_catalog != NULL) {
    delete new_catalog;
    new_catalog = NULL;
  }
}


template<class DerivedT>
CommandMigrate::AbstractMigrationWorker<DerivedT>::AbstractMigrationWorker(
  const worker_context *context)
  : temporary_directory_(context->temporary_directory)
  , collect_catalog_statistics_(context->collect_catalog_statistics)
{ }


template<class DerivedT>
CommandMigrate::AbstractMigrationWorker<DerivedT>::~AbstractMigrationWorker() {}


template<class DerivedT>
void CommandMigrate::AbstractMigrationWorker<DerivedT>::operator()(
                                                    const expected_data &data) {
  migration_stopwatch_.Start();
  const bool success = static_cast<DerivedT*>(this)->RunMigration(data) &&
                       UpdateNestedCatalogReferences(data) &&
                       UpdateCatalogMetadata(data)         &&
                       CollectAndAggregateStatistics(data) &&
                       CleanupNestedCatalogs(data);
  data->success = success;
  migration_stopwatch_.Stop();

  data->statistics.migration_time = migration_stopwatch_.GetTime();
  migration_stopwatch_.Reset();

  // Note: MigrationCallback() will take care of the result...
  if (success) {
    ConcurrentWorker<DerivedT>::master()->JobSuccessful(data);
  } else {
    ConcurrentWorker<DerivedT>::master()->JobFailed(data);
  }
}


template<class DerivedT>
bool CommandMigrate::AbstractMigrationWorker<DerivedT>::
     UpdateNestedCatalogReferences(PendingCatalog *data) const
{
  const catalog::Catalog *new_catalog =
    (data->HasNew()) ? data->new_catalog : data->old_catalog;
  const catalog::CatalogDatabase &writable = new_catalog->database();

  catalog::SqlCatalog add_nested_catalog(writable,
    "INSERT OR REPLACE INTO nested_catalogs (path,   sha1,  size) "
    "                VALUES                 (:path, :sha1, :size);");

  // go through all nested catalogs and update their references (we are
  // currently in their parent catalog)
  // Note: we might need to wait for the nested catalog to be fully processed.
  PendingCatalogList::const_iterator i    = data->nested_catalogs.begin();
  PendingCatalogList::const_iterator iend = data->nested_catalogs.end();
  for (; i != iend; ++i) {
    PendingCatalog    *nested_catalog  = *i;

    if (!nested_catalog->was_updated.Get()) {
      continue;
    }

    const std::string &root_path    = nested_catalog->root_path();
    const shash::Any   catalog_hash = nested_catalog->new_catalog_hash;
    const size_t       catalog_size = nested_catalog->new_catalog_size;

    // insert the updated nested catalog reference into the new catalog
    const bool retval =
      add_nested_catalog.BindText(1, root_path)               &&
      add_nested_catalog.BindText(2, catalog_hash.ToString()) &&
      add_nested_catalog.BindInt64(3, catalog_size)           &&
      add_nested_catalog.Execute();
    if (!retval) {
      Error("Failed to add nested catalog link", add_nested_catalog, data);
      return false;
    }
    add_nested_catalog.Reset();
  }

  return true;
}


template<class DerivedT>
bool CommandMigrate::AbstractMigrationWorker<DerivedT>::
     UpdateCatalogMetadata(PendingCatalog *data) const
{
  if (!data->HasChanges()) {
    return true;
  }

  catalog::WritableCatalog *catalog =
    (data->HasNew()) ? data->new_catalog : GetWritable(data->old_catalog);

  // Set the previous revision hash in the new catalog to the old catalog
  // we are doing the whole migration as a new snapshot that does not change
  // any files, but just applies the necessary data schema migrations
  catalog->SetPreviousRevision(data->old_catalog->hash());
  catalog->IncrementRevision();
  catalog->UpdateLastModified();

  return true;
}


template<class DerivedT>
bool CommandMigrate::AbstractMigrationWorker<DerivedT>::
     CollectAndAggregateStatistics(PendingCatalog *data) const
{
  if (!collect_catalog_statistics_) {
    return true;
  }

  const catalog::Catalog *new_catalog =
    (data->HasNew()) ? data->new_catalog : data->old_catalog;
  const catalog::CatalogDatabase &writable = new_catalog->database();
  bool retval;

  // Find out the discrepancy between MAX(rowid) and COUNT(*)
  catalog::SqlCatalog wasted_inodes(writable,
    "SELECT COUNT(*), MAX(rowid) FROM catalog;");
  retval = wasted_inodes.FetchRow();
  if (!retval) {
    Error("Failed to count entries in catalog", wasted_inodes, data);
    return false;
  }
  const unsigned int entry_count = wasted_inodes.RetrieveInt64(0);
  const unsigned int max_row_id  = wasted_inodes.RetrieveInt64(1);

  // Save collected information into the central statistics aggregator
  data->statistics.root_path   = data->root_path();
  data->statistics.max_row_id  = max_row_id;
  data->statistics.entry_count = entry_count;

  return true;
}


template<class DerivedT>
bool CommandMigrate::AbstractMigrationWorker<DerivedT>::CleanupNestedCatalogs(
  PendingCatalog *data) const
{
  // All nested catalogs of PendingCatalog 'data' are fully processed and
  // accounted. It is safe to get rid of their data structures here!
  PendingCatalogList::const_iterator i    = data->nested_catalogs.begin();
  PendingCatalogList::const_iterator iend = data->nested_catalogs.end();
  for (; i != iend; ++i) {
    delete *i;
  }

  data->nested_catalogs.clear();
  return true;
}


/**
 * Those values _must_ reflect the schema version in catalog_sql.h so that a
 * legacy catalog migration generates always the latest catalog revision.
 * This is a deliberately duplicated piece of information to ensure that always
 * both the catalog management and migration classes get updated.
 */
const float    CommandMigrate::MigrationWorker_20x::kSchema         = 2.5;
const unsigned CommandMigrate::MigrationWorker_20x::kSchemaRevision = 6;


template<class DerivedT>
catalog::WritableCatalog*
CommandMigrate::AbstractMigrationWorker<DerivedT>::GetWritable(
                                        const catalog::Catalog *catalog) const {
  return dynamic_cast<catalog::WritableCatalog*>(const_cast<catalog::Catalog*>(
    catalog));
}


//------------------------------------------------------------------------------


CommandMigrate::MigrationWorker_20x::MigrationWorker_20x(
  const worker_context *context)
  : AbstractMigrationWorker<MigrationWorker_20x>(context)
  , fix_nested_catalog_transitions_(context->fix_nested_catalog_transitions)
  , analyze_file_linkcounts_(context->analyze_file_linkcounts)
  , uid_(context->uid)
  , gid_(context->gid) { }


bool CommandMigrate::MigrationWorker_20x::RunMigration(PendingCatalog *data)
  const
{
  // double-check that we are generating compatible catalogs to the actual
  // catalog management classes
  assert(kSchema         == catalog::CatalogDatabase::kLatestSupportedSchema);
  assert(kSchemaRevision == catalog::CatalogDatabase::kLatestSchemaRevision);

  return CreateNewEmptyCatalog(data) &&
         CheckDatabaseSchemaCompatibility(data) &&
         AttachOldCatalogDatabase(data) &&
         StartDatabaseTransaction(data) &&
         MigrateFileMetadata(data) &&
         MigrateNestedCatalogMountPoints(data) &&
         FixNestedCatalogTransitionPoints(data) &&
         RemoveDanglingNestedMountpoints(data) &&
         GenerateCatalogStatistics(data) &&
         FindRootEntryInformation(data) &&
         CommitDatabaseTransaction(data) &&
         DetachOldCatalogDatabase(data);
}

bool CommandMigrate::MigrationWorker_20x::CreateNewEmptyCatalog(
  PendingCatalog *data) const
{
  const string root_path = data->root_path();

  // create a new catalog database schema
  const string clg_db_path =
    CreateTempPath(temporary_directory_ + "/catalog", 0666);
  if (clg_db_path.empty()) {
    Error("Failed to create temporary file for the new catalog database.");
    return false;
  }
  const bool volatile_content = false;

  {
    // TODO(rmeusel): Attach catalog should work with an open catalog database
    // as well, to remove this inefficiency
    UniquePtr<catalog::CatalogDatabase>
      new_clg_db(catalog::CatalogDatabase::Create(clg_db_path));
    if (!new_clg_db.IsValid() ||
        !new_clg_db->InsertInitialValues(root_path, volatile_content, "")) {
      Error("Failed to create database for new catalog");
      unlink(clg_db_path.c_str());
      return false;
    }
  }

  // Attach the just created nested catalog database
  catalog::WritableCatalog *writable_catalog =
    catalog::WritableCatalog::AttachFreely(root_path, clg_db_path,
                                           shash::Any(shash::kSha1));
  if (writable_catalog == NULL) {
    Error("Failed to open database for new catalog");
    unlink(clg_db_path.c_str());
    return false;
  }

  data->new_catalog = writable_catalog;
  return true;
}


bool CommandMigrate::MigrationWorker_20x::CheckDatabaseSchemaCompatibility(
  PendingCatalog *data) const
{
  const catalog::CatalogDatabase &old_catalog = data->old_catalog->database();
  const catalog::CatalogDatabase &new_catalog = data->new_catalog->database();

  if ((new_catalog.schema_version() <
         catalog::CatalogDatabase::kLatestSupportedSchema -
         catalog::CatalogDatabase::kSchemaEpsilon
       ||
       new_catalog.schema_version() >
         catalog::CatalogDatabase::kLatestSupportedSchema +
         catalog::CatalogDatabase::kSchemaEpsilon)
       ||
       (old_catalog.schema_version() > 2.1 +
         catalog::CatalogDatabase::kSchemaEpsilon))
  {
    Error("Failed to meet database requirements for migration.", data);
    return false;
  }
  return true;
}


bool CommandMigrate::MigrationWorker_20x::AttachOldCatalogDatabase(
  PendingCatalog *data) const
{
  const catalog::CatalogDatabase &old_catalog = data->old_catalog->database();
  const catalog::CatalogDatabase &new_catalog = data->new_catalog->database();

  catalog::SqlCatalog sql_attach_new(new_catalog,
    "ATTACH '" + old_catalog.filename() + "' AS old;");
  bool retval = sql_attach_new.Execute();

  // remove the hardlink to the old database file (temporary file), it will not
  // be needed anymore... data will get deleted when the database is closed
  unlink(data->old_catalog->database().filename().c_str());

  if (!retval) {
    Error("Failed to attach database of old catalog", sql_attach_new, data);
    return false;
  }
  return true;
}


bool CommandMigrate::MigrationWorker_20x::StartDatabaseTransaction(
  PendingCatalog *data) const
{
  assert(data->HasNew());
  data->new_catalog->Transaction();
  return true;
}


bool CommandMigrate::MigrationWorker_20x::MigrateFileMetadata(
  PendingCatalog *data) const
{
  assert(!data->new_catalog->IsDirty());
  assert(data->HasNew());
  bool retval;
  const catalog::CatalogDatabase &writable = data->new_catalog->database();

  // Hardlinks scratch space.
  // This temporary table is used for the hardlink analysis results.
  // The old catalog format did not have a direct notion of hardlinks and their
  // linkcounts,  but this information can be partly retrieved from the under-
  // lying file system semantics.
  //
  //   Hardlinks:
  //     groupid   : this group id can be used for the new catalog schema
  //     inode     : the inodes that were part of a hardlink group before
  //     linkcount : the linkcount for hardlink group id members
  catalog::SqlCatalog sql_create_hardlinks_table(writable,
    "CREATE TEMPORARY TABLE hardlinks "
    "  (  hardlink_group_id  INTEGER PRIMARY KEY AUTOINCREMENT, "
    "     inode              INTEGER, "
    "     linkcount          INTEGER, "
    "     CONSTRAINT unique_inode UNIQUE (inode)  );");
  retval = sql_create_hardlinks_table.Execute();
  if (!retval) {
    Error("Failed to create temporary hardlink analysis table",
          sql_create_hardlinks_table, data);
    return false;
  }

  // Directory Linkcount scratch space.
  // Directory linkcounts can be obtained from the directory hierarchy reflected
  // in the old style catalogs. The new catalog schema asks for this specific
  // linkcount. Directory linkcount analysis results will be put into this
  // temporary table
  catalog::SqlCatalog sql_create_linkcounts_table(writable,
    "CREATE TEMPORARY TABLE dir_linkcounts "
    "  (  inode      INTEGER PRIMARY KEY, "
    "     linkcount  INTEGER  );");
  retval = sql_create_linkcounts_table.Execute();
  if (!retval) {
    Error("Failed to create tmeporary directory linkcount analysis table",
          sql_create_linkcounts_table, data);
  }

  // It is possible to skip this step.
  // In that case all hardlink inodes with a (potential) linkcount > 1 will get
  // degraded to files containing the same content
  if (analyze_file_linkcounts_) {
    retval = AnalyzeFileLinkcounts(data);
    if (!retval) {
      return false;
    }
  }

  // Analyze the linkcounts of directories
  //   - each directory has a linkcount of at least 2 (empty directory)
  //     (link in parent directory and self reference (cd .) )
  //   - for each child directory, the parent's link count is incremented by 1
  //     (parent reference in child (cd ..) )
  //
  // Note: nested catalog mountpoints will be miscalculated here, since we can't
  //       check the number of containing directories. They are defined in a the
  //       linked nested catalog and need to be added later on.
  //       (see: MigrateNestedCatalogMountPoints() for details)
  catalog::SqlCatalog sql_dir_linkcounts(writable,
    "INSERT INTO dir_linkcounts "
    "  SELECT c1.inode as inode, "
    "         SUM(IFNULL(MIN(c2.inode,1),0)) + 2 as linkcount "
    "  FROM old.catalog as c1 "
    "  LEFT JOIN old.catalog as c2 "
    "    ON c2.parent_1 = c1.md5path_1 AND "
    "       c2.parent_2 = c1.md5path_2 AND "
    "       c2.flags & :flag_dir_1 "
    "  WHERE c1.flags & :flag_dir_2 "
    "  GROUP BY c1.inode;");
  retval =
    sql_dir_linkcounts.BindInt64(1, catalog::SqlDirent::kFlagDir) &&
    sql_dir_linkcounts.BindInt64(2, catalog::SqlDirent::kFlagDir) &&
    sql_dir_linkcounts.Execute();
  if (!retval) {
    Error("Failed to analyze directory specific linkcounts",
          sql_dir_linkcounts, data);
    if (sql_dir_linkcounts.GetLastError() == SQLITE_CONSTRAINT) {
      Error("Obviously your catalogs are corrupted, since we found a directory"
            "inode that is a file inode at the same time!");
    }
    return false;
  }

  // Copy the old file meta information into the new catalog schema
  //   here we also add the previously analyzed hardlink/linkcount information
  //   from both temporary tables "hardlinks" and "dir_linkcounts".
  //
  // Note: nested catalog mountpoints still need to be treated separately
  //       (see MigrateNestedCatalogMountPoints() for details)
  catalog::SqlCatalog migrate_file_meta_data(writable,
    "INSERT INTO catalog "
    "  SELECT md5path_1, md5path_2, "
    "         parent_1, parent_2, "
    "         IFNULL(hardlink_group_id, 0) << 32 | "
    "         COALESCE(hardlinks.linkcount, dir_linkcounts.linkcount, 1) "
    "           AS hardlinks, "
    "         hash, size, mode, mtime, "
    "         flags, name, symlink, "
    "         :uid, "
    "         :gid, "
    "         NULL "  // set empty xattr BLOB (default)
    "  FROM old.catalog "
    "  LEFT JOIN hardlinks "
    "    ON catalog.inode = hardlinks.inode "
    "  LEFT JOIN dir_linkcounts "
    "    ON catalog.inode = dir_linkcounts.inode;");
  retval = migrate_file_meta_data.BindInt64(1, uid_) &&
           migrate_file_meta_data.BindInt64(2, gid_) &&
           migrate_file_meta_data.Execute();
  if (!retval) {
    Error("Failed to migrate the file system meta data",
          migrate_file_meta_data, data);
    return false;
  }

  // If we deal with a nested catalog, we need to add a .cvmfscatalog entry
  // since it was not present in the old repository specification but is needed
  // now!
  if (!data->IsRoot()) {
    const catalog::DirectoryEntry &nested_marker =
      CommandMigrate::GetNestedCatalogMarkerDirent();
    catalog::SqlDirentInsert insert_nested_marker(writable);
    const std::string   root_path   = data->root_path();
    const std::string   file_path   = root_path +
                                      "/" + nested_marker.name().ToString();
    const shash::Md5    &path_hash   = shash::Md5(file_path.data(),
                                                  file_path.size());
    const shash::Md5    &parent_hash = shash::Md5(root_path.data(),
                                                  root_path.size());
    retval = insert_nested_marker.BindPathHash(path_hash)         &&
             insert_nested_marker.BindParentPathHash(parent_hash) &&
             insert_nested_marker.BindDirent(nested_marker)       &&
             insert_nested_marker.BindXattrEmpty()                &&
             insert_nested_marker.Execute();
    if (!retval) {
      Error("Failed to insert nested catalog marker into new nested catalog.",
            insert_nested_marker, data);
      return false;
    }
  }

  // Copy (and update) the properties fields
  //
  // Note: The 'schema' is explicitly not copied to the new catalog.
  //       Each catalog contains a revision, which is also copied here and that
  //       is later updated by calling catalog->IncrementRevision()
  catalog::SqlCatalog copy_properties(writable,
    "INSERT OR REPLACE INTO properties "
    "  SELECT key, value "
    "  FROM old.properties "
    "  WHERE key != 'schema';");
  retval = copy_properties.Execute();
  if (!retval) {
    Error("Failed to migrate the properties table.", copy_properties, data);
    return false;
  }

  return true;
}


bool CommandMigrate::MigrationWorker_20x::AnalyzeFileLinkcounts(
  PendingCatalog *data) const
{
  assert(data->HasNew());
  const catalog::CatalogDatabase &writable = data->new_catalog->database();
  bool retval;

  // Analyze the hardlink relationships in the old catalog
  //   inodes used to be assigned at publishing time, implicitly constituating
  //   those relationships. We now need them explicitly in the file catalogs
  // This looks for directory entries with matching inodes but differing path-
  // hashes and saves the results in a temporary table called 'hl_scratch'
  //
  // Note: We only support hardlink groups that reside in the same directory!
  //       Therefore we first need to figure out hardlink candidates (which
  //       might still contain hardlink groups spanning more than one directory)
  //       In a second step these candidates will be analyzed to kick out un-
  //       supported hardlink groups.
  //       Unsupported hardlink groups will be be treated as normal files with
  //       the same content
  catalog::SqlCatalog sql_create_hardlinks_scratch_table(writable,
    "CREATE TEMPORARY TABLE hl_scratch AS "
    "  SELECT c1.inode AS inode, c1.md5path_1, c1.md5path_2, "
    "         c1.parent_1 as c1p1, c1.parent_2 as c1p2, "
    "         c2.parent_1 as c2p1, c2.parent_2 as c2p2 "
    "  FROM old.catalog AS c1 "
    "  INNER JOIN old.catalog AS c2 "
    "  ON c1.inode == c2.inode AND "
    "     (c1.md5path_1 != c2.md5path_1 OR "
    "      c1.md5path_2 != c2.md5path_2);");
  retval = sql_create_hardlinks_scratch_table.Execute();
  if (!retval) {
    Error("Failed to create temporary scratch table for hardlink analysis",
          sql_create_hardlinks_scratch_table, data);
    return false;
  }

  // Figures out which hardlink candidates are supported by CVMFS and can be
  // transferred into the new catalog as so called hardlink groups. Unsupported
  // hardlinks need to be discarded and treated as normal files containing the
  // exact same data
  catalog::SqlCatalog fill_linkcount_table_for_files(writable,
    "INSERT INTO hardlinks (inode, linkcount)"
    "  SELECT inode, count(*) as linkcount "
    "  FROM ( "
         // recombine supported hardlink inodes with their actual manifested
         // hard-links in the catalog.
         // Note: for each directory entry pointing to the same supported
         // hardlink inode we have a distinct MD5 path hash
    "    SELECT DISTINCT hl.inode, hl.md5path_1, hl.md5path_2 "
    "    FROM ( "
           // sort out supported hardlink inodes from unsupported ones by
           // locality
           // Note: see the next comment for the nested SELECT
    "      SELECT inode "
    "      FROM ( "
    "        SELECT inode, count(*) AS cnt "
    "        FROM ( "
               // go through the potential hardlinks and collect location infor-
               // mation about them.
               // Note: we only support hardlinks that all reside in the same
               //       directory, thus having the same parent (c1p* == c2p*)
               //   --> For supported hardlink candidates the SELECT DISTINCT
               // will produce only a single row, whereas others produce more
    "          SELECT DISTINCT inode,c1p1,c1p1,c2p1,c2p2 "
    "          FROM hl_scratch AS hl "
    "        ) "
    "        GROUP BY inode "
    "      ) "
    "      WHERE cnt = 1 "
    "    ) AS supported_hardlinks "
    "    LEFT JOIN hl_scratch AS hl "
    "    ON supported_hardlinks.inode = hl.inode "
    "  ) "
    "  GROUP BY inode;");
  retval = fill_linkcount_table_for_files.Execute();
  if (!retval) {
    Error("Failed to analyze hardlink relationships for files.",
          fill_linkcount_table_for_files, data);
    return false;
  }

  // The file linkcount and hardlink analysis is finished and the scratch table
  // can be deleted...
  catalog::SqlCatalog drop_hardlink_scratch_space(writable,
                                                  "DROP TABLE hl_scratch;");
  retval = drop_hardlink_scratch_space.Execute();
  if (!retval) {
    Error("Failed to remove file linkcount analysis scratch table",
          drop_hardlink_scratch_space, data);
    return false;
  }

  // Do some statistics if asked for...
  if (collect_catalog_statistics_) {
    catalog::SqlCatalog count_hardlinks(writable,
      "SELECT count(*), sum(linkcount) FROM hardlinks;");
    retval = count_hardlinks.FetchRow();
    if (!retval) {
      Error("Failed to count the generated file hardlinks for statistics",
            count_hardlinks, data);
      return false;
    }

    data->statistics.hardlink_group_count  += count_hardlinks.RetrieveInt64(0);
    data->statistics.aggregated_linkcounts += count_hardlinks.RetrieveInt64(1);
  }

  return true;
}


bool CommandMigrate::MigrationWorker_20x::MigrateNestedCatalogMountPoints(
  PendingCatalog *data) const
{
  assert(data->HasNew());
  const catalog::CatalogDatabase &writable = data->new_catalog->database();
  bool retval;

  // preparing the SQL statement for nested catalog mountpoint update
  catalog::SqlCatalog update_mntpnt_linkcount(writable,
    "UPDATE catalog "
    "SET hardlinks = :linkcount "
    "WHERE md5path_1 = :md5_1 AND md5path_2 = :md5_2;");

  // update all nested catalog mountpoints
  // (Note: we might need to wait for the nested catalog to be processed)
  PendingCatalogList::const_iterator i    = data->nested_catalogs.begin();
  PendingCatalogList::const_iterator iend = data->nested_catalogs.end();
  for (; i != iend; ++i) {
    // collect information about the nested catalog
    PendingCatalog *nested_catalog = *i;
    const catalog::DirectoryEntry root_entry = nested_catalog->root_entry.Get();
    const string &root_path = nested_catalog->root_path();

    // update the nested catalog mountpoint directory entry with the correct
    // linkcount that was determined while processing the nested catalog
    const shash::Md5 mountpoint_hash = shash::Md5(root_path.data(),
                                                  root_path.size());
    retval =
      update_mntpnt_linkcount.BindInt64(1, root_entry.linkcount()) &&
      update_mntpnt_linkcount.BindMd5(2, 3, mountpoint_hash)       &&
      update_mntpnt_linkcount.Execute();
    if (!retval) {
      Error("Failed to update linkcount of nested catalog mountpoint",
            update_mntpnt_linkcount, data);
      return false;
    }
    update_mntpnt_linkcount.Reset();
  }

  return true;
}


bool CommandMigrate::MigrationWorker_20x::FixNestedCatalogTransitionPoints(
  PendingCatalog *data) const
{
  assert(data->HasNew());
  if (!fix_nested_catalog_transitions_) {
    // Fixing transition point mismatches is not enabled...
    return true;
  }

  typedef catalog::DirectoryEntry::Difference Difference;

  const catalog::CatalogDatabase &writable = data->new_catalog->database();
  bool retval;

  catalog::SqlLookupPathHash lookup_mountpoint(writable);
  catalog::SqlDirentUpdate   update_directory_entry(writable);

  // Unbox the nested catalogs (possibly waiting for migration of them first)
  PendingCatalogList::const_iterator i    = data->nested_catalogs.begin();
  PendingCatalogList::const_iterator iend = data->nested_catalogs.end();
  for (; i != iend; ++i) {
    // Collect information about the nested catalog
    PendingCatalog *nested_catalog = *i;
    const catalog::DirectoryEntry nested_root_entry =
      nested_catalog->root_entry.Get();
    const string &nested_root_path = nested_catalog->root_path();
    const shash::Md5 mountpoint_path_hash =
      shash::Md5(nested_root_path.data(), nested_root_path.size());

    // Retrieve the nested catalog mountpoint from the current catalog
    retval = lookup_mountpoint.BindPathHash(mountpoint_path_hash) &&
             lookup_mountpoint.FetchRow();
    if (!retval) {
      Error("Failed to fetch nested catalog mountpoint to check for compatible"
            "transition points", lookup_mountpoint, data);
      return false;
    }

    catalog::DirectoryEntry mountpoint_entry =
      lookup_mountpoint.GetDirent(data->new_catalog);
    lookup_mountpoint.Reset();

    // Compare nested catalog mountpoint and nested catalog root entries
    catalog::DirectoryEntry::Differences diffs =
      mountpoint_entry.CompareTo(nested_root_entry);

    // We MUST deal with two directory entries that are a pair of nested cata-
    // log mountpoint and root entry! Thus we expect their transition flags to
    // differ and their name to be the same.
    assert(diffs & Difference::kNestedCatalogTransitionFlags);
    assert((diffs & Difference::kName) == 0);

    // Check if there are other differences except the nested catalog transition
    // flags and fix them...
    if ((diffs ^ Difference::kNestedCatalogTransitionFlags) != 0) {
      // If we found differences, we still assume a couple of directory entry
      // fields to be the same, otherwise some severe stuff would be wrong...
      if ((diffs & Difference::kChecksum)        ||
          (diffs & Difference::kLinkcount)       ||
          (diffs & Difference::kSymlink)         ||
          (diffs & Difference::kChunkedFileFlag)    )
      {
        Error("Found an irreparable mismatch in a nested catalog transition "
              "point at '" + nested_root_path + "'\nAborting...\n");
      }

      // Copy the properties from the nested catalog root entry into the mount-
      // point entry to bring them in sync again
      CommandMigrate::FixNestedCatalogTransitionPoint(
        nested_root_entry, &mountpoint_entry);

      // save the nested catalog mountpoint entry into the catalog
      retval = update_directory_entry.BindPathHash(mountpoint_path_hash) &&
               update_directory_entry.BindDirent(mountpoint_entry)       &&
               update_directory_entry.Execute();
      if (!retval) {
        Error("Failed to save resynchronized nested catalog mountpoint into "
              "catalog database", update_directory_entry, data);
        return false;
      }
      update_directory_entry.Reset();

      // Fixing of this mountpoint went well... inform the user that this minor
      // issue occurred
      LogCvmfs(kLogCatalog, kLogStdout,
               "NOTE: fixed incompatible nested catalog transition point at: "
               "'%s' ", nested_root_path.c_str());
    }
  }

  return true;
}


void CommandMigrate::FixNestedCatalogTransitionPoint(
  const catalog::DirectoryEntry &nested_root,
  catalog::DirectoryEntry *mountpoint
) {
  // Replace some file system parameters in the mountpoint to resync it with
  // the nested root of the corresponding nested catalog
  //
  // Note: this method relies on CommandMigrate being a friend of DirectoryEntry
  mountpoint->mode_  = nested_root.mode_;
  mountpoint->uid_   = nested_root.uid_;
  mountpoint->gid_   = nested_root.gid_;
  mountpoint->size_  = nested_root.size_;
  mountpoint->mtime_ = nested_root.mtime_;
}


bool CommandMigrate::MigrationWorker_20x::RemoveDanglingNestedMountpoints(
  PendingCatalog *data) const
{
  assert(data->HasNew());
  const catalog::CatalogDatabase &writable = data->new_catalog->database();
  bool retval = false;

  // build a set of registered nested catalog path hashes
  typedef catalog::Catalog::NestedCatalogList NestedCatalogList;
  typedef std::map<shash::Md5, catalog::Catalog::NestedCatalog>
    NestedCatalogMap;
  const NestedCatalogList& nested_clgs =
    data->old_catalog->ListNestedCatalogs();
  NestedCatalogList::const_iterator i = nested_clgs.begin();
  const NestedCatalogList::const_iterator iend = nested_clgs.end();
  NestedCatalogMap nested_catalog_path_hashes;
  for (; i != iend; ++i) {
    const PathString &path = i->mountpoint;
    const shash::Md5  hash(path.GetChars(), path.GetLength());
    nested_catalog_path_hashes[hash] = *i;
  }

  // Retrieve nested catalog mountpoints that have child entries directly inside
  // the current catalog (which is a malformed state)
  catalog::SqlLookupDanglingMountpoints sql_dangling_mountpoints(writable);
  catalog::SqlDirentUpdate save_updated_mountpoint(writable);

  std::vector<catalog::DirectoryEntry> todo_dirent;
  std::vector<shash::Md5> todo_hash;

  // go through the list of dangling nested catalog mountpoints and fix them
  // where needed (check if there is no nested catalog registered for them)
  while (sql_dangling_mountpoints.FetchRow()) {
    catalog::DirectoryEntry dangling_mountpoint =
      sql_dangling_mountpoints.GetDirent(data->new_catalog);
    const shash::Md5 path_hash = sql_dangling_mountpoints.GetPathHash();
    assert(dangling_mountpoint.IsNestedCatalogMountpoint());

    // check if the nested catalog mountpoint is registered in the nested cata-
    // log list of the currently migrated catalog
    const NestedCatalogMap::const_iterator nested_catalog =
                                     nested_catalog_path_hashes.find(path_hash);
    if (nested_catalog != nested_catalog_path_hashes.end()) {
      LogCvmfs(kLogCatalog, kLogStderr,
               "WARNING: found a non-empty nested catalog mountpoint under "
               "'%s'", nested_catalog->second.mountpoint.c_str());
      continue;
    }

    // the mountpoint was confirmed to be dangling and needs to be removed
    dangling_mountpoint.set_is_nested_catalog_mountpoint(false);
    todo_dirent.push_back(dangling_mountpoint);
    todo_hash.push_back(path_hash);
  }

  for (unsigned i = 0; i < todo_dirent.size(); ++i) {
    retval = save_updated_mountpoint.BindPathHash(todo_hash[i])  &&
             save_updated_mountpoint.BindDirent(todo_dirent[i])  &&
             save_updated_mountpoint.Execute()                   &&
             save_updated_mountpoint.Reset();
    if (!retval) {
      Error("Failed to remove dangling nested catalog mountpoint entry in "
            "catalog", save_updated_mountpoint, data);
      return false;
    }

    // tell the user that this intervention has been taken place
    LogCvmfs(kLogCatalog, kLogStdout, "NOTE: fixed dangling nested catalog "
                                      "mountpoint entry called: '%s' ",
                                      todo_dirent[i].name().c_str());
  }

  return true;
}


const catalog::DirectoryEntry& CommandMigrate::GetNestedCatalogMarkerDirent() {
  // This is pre-initialized singleton... it MUST be already there...
  assert(nested_catalog_marker_.name_.ToString() == ".cvmfscatalog");
  return nested_catalog_marker_;
}

bool CommandMigrate::GenerateNestedCatalogMarkerChunk() {
  // Create an empty nested catalog marker file
  nested_catalog_marker_tmp_path_ =
      CreateTempPath(temporary_directory_ + "/.cvmfscatalog", 0644);
  if (nested_catalog_marker_tmp_path_.empty()) {
    Error("Failed to create temp file for nested catalog marker dummy.");
    return false;
  }

  // Process and upload it to the backend storage
  IngestionSource *source =
      new FileIngestionSource(nested_catalog_marker_tmp_path_);
  spooler_->Process(source);
  return true;
}

void CommandMigrate::CreateNestedCatalogMarkerDirent(
  const shash::Any &content_hash)
{
  // Generate it only once
  assert(nested_catalog_marker_.name_.ToString() != ".cvmfscatalog");

  // Fill the DirectoryEntry structure will all needed information
  nested_catalog_marker_.name_.Assign(".cvmfscatalog", strlen(".cvmfscatalog"));
  nested_catalog_marker_.mode_      = 33188;
  nested_catalog_marker_.uid_       = uid_;
  nested_catalog_marker_.gid_       = gid_;
  nested_catalog_marker_.size_      = 0;
  nested_catalog_marker_.mtime_     = time(NULL);
  nested_catalog_marker_.linkcount_ = 1;
  nested_catalog_marker_.checksum_  = content_hash;
}


bool CommandMigrate::MigrationWorker_20x::GenerateCatalogStatistics(
  PendingCatalog *data) const
{
  assert(data->HasNew());
  bool retval = false;
  const catalog::CatalogDatabase &writable = data->new_catalog->database();

  // Aggregated the statistics counters of all nested catalogs
  // Note: we might need to wait until nested catalogs are successfully
  // processed
  catalog::DeltaCounters stats_counters;
  PendingCatalogList::const_iterator i    = data->nested_catalogs.begin();
  PendingCatalogList::const_iterator iend = data->nested_catalogs.end();
  for (; i != iend; ++i) {
    const PendingCatalog *nested_catalog = *i;
    const catalog::DeltaCounters &s = nested_catalog->nested_statistics.Get();
    s.PopulateToParent(&stats_counters);
  }

  // Count various directory entry types in the catalog to fill up the catalog
  // statistics counters introduced in the current catalog schema
  catalog::SqlCatalog count_regular_files(writable,
    "SELECT count(*) FROM catalog "
    "                WHERE  flags & :flag_file "
    "                       AND NOT flags & :flag_link;");
  catalog::SqlCatalog count_symlinks(writable,
    "SELECT count(*) FROM catalog WHERE flags & :flag_link;");
  catalog::SqlCatalog count_directories(writable,
    "SELECT count(*) FROM catalog WHERE flags & :flag_dir;");
  catalog::SqlCatalog aggregate_file_size(writable,
    "SELECT sum(size) FROM catalog WHERE  flags & :flag_file "
    "                                     AND NOT flags & :flag_link");

  // Run the actual counting queries
  retval =
    count_regular_files.BindInt64(1, catalog::SqlDirent::kFlagFile) &&
    count_regular_files.BindInt64(2, catalog::SqlDirent::kFlagLink) &&
    count_regular_files.FetchRow();
  if (!retval) {
    Error("Failed to count regular files.", count_regular_files, data);
    return false;
  }
  retval =
    count_symlinks.BindInt64(1, catalog::SqlDirent::kFlagLink) &&
    count_symlinks.FetchRow();
  if (!retval) {
    Error("Failed to count symlinks.", count_symlinks, data);
    return false;
  }
  retval =
    count_directories.BindInt64(1, catalog::SqlDirent::kFlagDir) &&
    count_directories.FetchRow();
  if (!retval) {
    Error("Failed to count directories.", count_directories, data);
    return false;
  }
  retval =
    aggregate_file_size.BindInt64(1, catalog::SqlDirent::kFlagFile) &&
    aggregate_file_size.BindInt64(2, catalog::SqlDirent::kFlagLink) &&
    aggregate_file_size.FetchRow();
  if (!retval) {
    Error("Failed to aggregate the file sizes.", aggregate_file_size, data);
    return false;
  }

  // Insert the counted statistics into the DeltaCounters data structure
  stats_counters.self.regular_files    = count_regular_files.RetrieveInt64(0);
  stats_counters.self.symlinks         = count_symlinks.RetrieveInt64(0);
  stats_counters.self.directories      = count_directories.RetrieveInt64(0);
  stats_counters.self.nested_catalogs  = data->nested_catalogs.size();
  stats_counters.self.file_size        = aggregate_file_size.RetrieveInt64(0);

  // Write back the generated statistics counters into the catalog database
  stats_counters.WriteToDatabase(writable);

  // Push the generated statistics counters up to the parent catalog
  data->nested_statistics.Set(stats_counters);

  return true;
}


bool CommandMigrate::MigrationWorker_20x::FindRootEntryInformation(
  PendingCatalog *data) const
{
  const catalog::CatalogDatabase &writable = data->new_catalog->database();
  bool retval;

  std::string root_path = data->root_path();
  shash::Md5 root_path_hash = shash::Md5(root_path.data(), root_path.size());

  catalog::SqlLookupPathHash lookup_root_entry(writable);
  retval = lookup_root_entry.BindPathHash(root_path_hash) &&
           lookup_root_entry.FetchRow();
  if (!retval) {
    Error("Failed to retrieve root directory entry of migrated catalog",
          lookup_root_entry, data);
    return false;
  }

  catalog::DirectoryEntry entry =
    lookup_root_entry.GetDirent(data->new_catalog);
  if (entry.linkcount() < 2 || entry.hardlink_group() > 0) {
    Error("Retrieved linkcount of catalog root entry is not sane.", data);
    return false;
  }

  data->root_entry.Set(entry);
  return true;
}


bool CommandMigrate::MigrationWorker_20x::CommitDatabaseTransaction(
  PendingCatalog *data) const
{
  assert(data->HasNew());
  data->new_catalog->Commit();
  return true;
}


bool CommandMigrate::MigrationWorker_20x::DetachOldCatalogDatabase(
  PendingCatalog *data) const
{
  assert(data->HasNew());
  const catalog::CatalogDatabase &writable = data->new_catalog->database();
  catalog::SqlCatalog detach_old_catalog(writable, "DETACH old;");
  const bool retval = detach_old_catalog.Execute();
  if (!retval) {
    Error("Failed to detach old catalog database.", detach_old_catalog, data);
    return false;
  }
  return true;
}


//------------------------------------------------------------------------------


CommandMigrate::MigrationWorker_217::MigrationWorker_217(
  const worker_context *context)
  : AbstractMigrationWorker<MigrationWorker_217>(context)
{ }


bool CommandMigrate::MigrationWorker_217::RunMigration(PendingCatalog *data)
  const
{
  return CheckDatabaseSchemaCompatibility(data) &&
         StartDatabaseTransaction(data) &&
         GenerateNewStatisticsCounters(data) &&
         UpdateCatalogSchema(data) &&
         CommitDatabaseTransaction(data);
}


bool CommandMigrate::MigrationWorker_217::CheckDatabaseSchemaCompatibility(
  PendingCatalog *data) const
{
  assert(!data->HasNew());
  const catalog::CatalogDatabase &old_catalog = data->old_catalog->database();

  if ((old_catalog.schema_version() < 2.4 -
       catalog::CatalogDatabase::kSchemaEpsilon)
      ||
      (old_catalog.schema_version() > 2.4 +
       catalog::CatalogDatabase::kSchemaEpsilon))
  {
    Error("Given Catalog is not Schema 2.4.", data);
    return false;
  }

  return true;
}


bool CommandMigrate::MigrationWorker_217::StartDatabaseTransaction(
  PendingCatalog *data) const
{
  assert(!data->HasNew());
  GetWritable(data->old_catalog)->Transaction();
  return true;
}


bool CommandMigrate::MigrationWorker_217::GenerateNewStatisticsCounters
                                                  (PendingCatalog *data) const {
  assert(!data->HasNew());
  bool retval = false;
  const catalog::CatalogDatabase &writable =
    GetWritable(data->old_catalog)->database();

  // Aggregated the statistics counters of all nested catalogs
  // Note: we might need to wait until nested catalogs are successfully
  // processed
  catalog::DeltaCounters stats_counters;
  PendingCatalogList::const_iterator i = data->nested_catalogs.begin();
  PendingCatalogList::const_iterator iend = data->nested_catalogs.end();
  for (; i != iend; ++i) {
    const PendingCatalog *nested_catalog = *i;
    const catalog::DeltaCounters &s = nested_catalog->nested_statistics.Get();
    s.PopulateToParent(&stats_counters);
  }

  // Count various directory entry types in the catalog to fill up the catalog
  // statistics counters introduced in the current catalog schema
  catalog::SqlCatalog count_chunked_files(writable,
    "SELECT count(*), sum(size) FROM catalog "
    "                WHERE flags & :flag_chunked_file;");
  catalog::SqlCatalog count_file_chunks(writable,
    "SELECT count(*) FROM chunks;");
  catalog::SqlCatalog aggregate_file_size(writable,
    "SELECT sum(size) FROM catalog WHERE  flags & :flag_file "
    "                                     AND NOT flags & :flag_link;");

  // Run the actual counting queries
  retval =
    count_chunked_files.BindInt64(1, catalog::SqlDirent::kFlagFileChunk) &&
    count_chunked_files.FetchRow();
  if (!retval) {
    Error("Failed to count chunked files.", count_chunked_files, data);
    return false;
  }
  retval = count_file_chunks.FetchRow();
  if (!retval) {
    Error("Failed to count file chunks", count_file_chunks, data);
    return false;
  }
  retval =
    aggregate_file_size.BindInt64(1, catalog::SqlDirent::kFlagFile) &&
    aggregate_file_size.BindInt64(2, catalog::SqlDirent::kFlagLink) &&
    aggregate_file_size.FetchRow();
  if (!retval) {
    Error("Failed to aggregate the file sizes.", aggregate_file_size, data);
    return false;
  }

  // Insert the counted statistics into the DeltaCounters data structure
  stats_counters.self.chunked_files     = count_chunked_files.RetrieveInt64(0);
  stats_counters.self.chunked_file_size = count_chunked_files.RetrieveInt64(1);
  stats_counters.self.file_chunks       = count_file_chunks.RetrieveInt64(0);
  stats_counters.self.file_size         = aggregate_file_size.RetrieveInt64(0);

  // Write back the generated statistics counters into the catalog database
  catalog::Counters counters;
  retval = counters.ReadFromDatabase(writable, catalog::LegacyMode::kLegacy);
  if (!retval) {
    Error("Failed to read old catalog statistics counters", data);
    return false;
  }
  counters.ApplyDelta(stats_counters);
  retval = counters.InsertIntoDatabase(writable);
  if (!retval) {
    Error("Failed to write new statistics counters to database", data);
    return false;
  }

  // Push the generated statistics counters up to the parent catalog
  data->nested_statistics.Set(stats_counters);

  return true;
}


bool CommandMigrate::MigrationWorker_217::UpdateCatalogSchema
                                                  (PendingCatalog *data) const {
  assert(!data->HasNew());
  const catalog::CatalogDatabase &writable =
    GetWritable(data->old_catalog)->database();
  catalog::SqlCatalog update_schema_version(writable,
    "UPDATE properties SET value = :schema_version WHERE key = 'schema';");

  const bool retval =
    update_schema_version.BindDouble(1, 2.5) &&
    update_schema_version.Execute();
  if (!retval) {
    Error("Failed to update catalog schema version",
          update_schema_version,
          data);
    return false;
  }

  return true;
}


bool CommandMigrate::MigrationWorker_217::CommitDatabaseTransaction
                                                  (PendingCatalog *data) const {
  assert(!data->HasNew());
  GetWritable(data->old_catalog)->Commit();
  return true;
}


//------------------------------------------------------------------------------


CommandMigrate::ChownMigrationWorker::ChownMigrationWorker(
                                                const worker_context *context)
  : AbstractMigrationWorker<ChownMigrationWorker>(context)
  , uid_map_statement_(GenerateMappingStatement(context->uid_map, "uid"))
  , gid_map_statement_(GenerateMappingStatement(context->gid_map, "gid"))
{}

bool CommandMigrate::ChownMigrationWorker::RunMigration(
                                                   PendingCatalog *data) const {
  return ApplyPersonaMappings(data);
}


bool CommandMigrate::ChownMigrationWorker::ApplyPersonaMappings(
                                                   PendingCatalog *data) const {
  assert(data->old_catalog != NULL);
  assert(data->new_catalog == NULL);

  if (data->old_catalog->mountpoint() ==
      PathString("/" + string(catalog::VirtualCatalog::kVirtualPath)))
  {
    // skipping virtual catalog
    return true;
  }

  const catalog::CatalogDatabase &db =
                                     GetWritable(data->old_catalog)->database();

  if (!db.BeginTransaction()) {
    return false;
  }

  catalog::SqlCatalog uid_sql(db, uid_map_statement_);
  if (!uid_sql.Execute()) {
    Error("Failed to update UIDs", uid_sql, data);
    return false;
  }

  catalog::SqlCatalog gid_sql(db, gid_map_statement_);
  if (!gid_sql.Execute()) {
    Error("Failed to update GIDs", gid_sql, data);
    return false;
  }

  return db.CommitTransaction();
}


template <class MapT>
std::string CommandMigrate::ChownMigrationWorker::GenerateMappingStatement(
                                             const MapT         &map,
                                             const std::string  &column) const {
  assert(map.RuleCount() > 0 || map.HasDefault());

  std::string stmt = "UPDATE OR ABORT catalog SET " + column + " = ";

  if (map.RuleCount() == 0) {
    // map everything to the same value (just a simple UPDATE clause)
    stmt += StringifyInt(map.GetDefault());
  } else {
    // apply multiple ID mappings (UPDATE clause with CASE statement)
    stmt += "CASE " + column + " ";
    typedef typename MapT::map_type::const_iterator map_iterator;
          map_iterator i    = map.GetRuleMap().begin();
    const map_iterator iend = map.GetRuleMap().end();
    for (; i != iend; ++i) {
      stmt += "WHEN " + StringifyInt(i->first) +
             " THEN " + StringifyInt(i->second) + " ";
    }

    // add a default (if provided) or leave unchanged if no mapping fits
    stmt += (map.HasDefault())
                ? "ELSE " + StringifyInt(map.GetDefault()) + " "
                : "ELSE " + column + " ";
    stmt += "END";
  }

  stmt += ";";
  return stmt;
}


//------------------------------------------------------------------------------


bool CommandMigrate::HardlinkRemovalMigrationWorker::RunMigration(
                                                   PendingCatalog *data) const {
  return CheckDatabaseSchemaCompatibility(data) &&
         BreakUpHardlinks(data);
}


bool
CommandMigrate::HardlinkRemovalMigrationWorker::CheckDatabaseSchemaCompatibility
                                                  (PendingCatalog *data) const {
  assert(data->old_catalog != NULL);
  assert(data->new_catalog == NULL);

  const catalog::CatalogDatabase &clg = data->old_catalog->database();
  return clg.schema_version() >= 2.4 - catalog::CatalogDatabase::kSchemaEpsilon;
}


bool CommandMigrate::HardlinkRemovalMigrationWorker::BreakUpHardlinks(
                                                   PendingCatalog *data) const {
  assert(data->old_catalog != NULL);
  assert(data->new_catalog == NULL);

  const catalog::CatalogDatabase &db =
                                     GetWritable(data->old_catalog)->database();

  if (!db.BeginTransaction()) {
    return false;
  }

  // CernVM-FS catalogs do not contain inodes directly but they are assigned by
  // the CVMFS catalog at runtime. Hardlinks are treated with so-called hardlink
  // group IDs to indicate hardlink relationships that need to be respected at
  // runtime by assigning identical inodes accordingly.
  //
  // This updates all directory entries of a given catalog that have a linkcount
  // greater than 1 and are flagged as a 'file'. Note: Symlinks are flagged both
  // as 'file' and as 'symlink', hence they are updated implicitly as well.
  //
  // The 'hardlinks' field in the catalog contains two 32 bit integers:
  //   * the linkcount in the lower 32 bits
  //   * the (so called) hardlink group ID in the higher 32 bits
  //
  // Files that have a linkcount of exactly 1 do not have any hardlinks and have
  // the (implicit) hardlink group ID '0'. Hence, 'hardlinks == 1' means that a
  // file doesn't have any hardlinks (linkcount = 1) and doesn't need treatment
  // here.
  //
  // Files that have hardlinks (linkcount > 1) will have a very large integer in
  // their 'hardlinks' field (hardlink group ID > 0 in higher 32 bits). Those
  // files will be treated by setting their 'hardlinks' field to 1, effectively
  // clearing all hardlink information from the directory entry.
  const std::string stmt = "UPDATE OR ABORT catalog "
                           "SET hardlinks = 1 "
                           "WHERE flags & :file_flag "
                           "  AND hardlinks > 1;";
  catalog::SqlCatalog hardlink_removal_sql(db, stmt);
  hardlink_removal_sql.BindInt64(1, catalog::SqlDirent::kFlagFile);
  hardlink_removal_sql.Execute();

  return db.CommitTransaction();
}

//------------------------------------------------------------------------------


bool CommandMigrate::BulkhashRemovalMigrationWorker::RunMigration(
                                                   PendingCatalog *data) const {
  return CheckDatabaseSchemaCompatibility(data) &&
         RemoveRedundantBulkHashes(data);
}


bool
CommandMigrate::BulkhashRemovalMigrationWorker::CheckDatabaseSchemaCompatibility
                                                  (PendingCatalog *data) const {
  assert(data->old_catalog != NULL);
  assert(data->new_catalog == NULL);

  const catalog::CatalogDatabase &clg = data->old_catalog->database();
  return clg.schema_version() >= 2.4 - catalog::CatalogDatabase::kSchemaEpsilon;
}


bool CommandMigrate::BulkhashRemovalMigrationWorker::RemoveRedundantBulkHashes(
                                                   PendingCatalog *data) const {
  assert(data->old_catalog != NULL);
  assert(data->new_catalog == NULL);

  const catalog::CatalogDatabase &db =
                                     GetWritable(data->old_catalog)->database();

  if (!db.BeginTransaction()) {
    return false;
  }

  // Regular files with both bulk hashes and chunked hashes can drop the bulk
  // hash since modern clients >= 2.1.7 won't require them
  const std::string stmt = "UPDATE OR ABORT catalog "
                           "SET hash = NULL "
                           "WHERE flags & :file_chunked_flag;";
  catalog::SqlCatalog bulkhash_removal_sql(db, stmt);
  bulkhash_removal_sql.BindInt64(1, catalog::SqlDirent::kFlagFileChunk);
  bulkhash_removal_sql.Execute();

  return db.CommitTransaction();
}


//------------------------------------------------------------------------------


CommandMigrate::StatsMigrationWorker::StatsMigrationWorker(
  const worker_context *context)
  : AbstractMigrationWorker<StatsMigrationWorker>(context)
{ }


bool CommandMigrate::StatsMigrationWorker::RunMigration(PendingCatalog *data)
  const
{
  return CheckDatabaseSchemaCompatibility(data) &&
         StartDatabaseTransaction(data) &&
         RepairStatisticsCounters(data) &&
         CommitDatabaseTransaction(data);
}


bool CommandMigrate::StatsMigrationWorker::CheckDatabaseSchemaCompatibility(
  PendingCatalog *data) const
{
  assert(data->old_catalog != NULL);
  assert(data->new_catalog == NULL);

  const catalog::CatalogDatabase &clg = data->old_catalog->database();
  if (clg.schema_version() < 2.5 - catalog::CatalogDatabase::kSchemaEpsilon) {
    Error("Given catalog schema is < 2.5.", data);
    return false;
  }

  if (clg.schema_revision() < 5) {
    Error("Given catalog revision is < 5", data);
    return false;
  }

  return true;
}


bool CommandMigrate::StatsMigrationWorker::StartDatabaseTransaction(
  PendingCatalog *data) const
{
  assert(!data->HasNew());
  GetWritable(data->old_catalog)->Transaction();
  return true;
}


bool CommandMigrate::StatsMigrationWorker::RepairStatisticsCounters(
  PendingCatalog *data) const
{
  assert(!data->HasNew());
  bool retval = false;
  const catalog::CatalogDatabase &writable =
    GetWritable(data->old_catalog)->database();

  // Aggregated the statistics counters of all nested catalogs
  // Note: we might need to wait until nested catalogs are successfully
  // processed
  catalog::DeltaCounters stats_counters;
  PendingCatalogList::const_iterator i = data->nested_catalogs.begin();
  PendingCatalogList::const_iterator iend = data->nested_catalogs.end();
  for (; i != iend; ++i) {
    const PendingCatalog *nested_catalog = *i;
    const catalog::DeltaCounters &s = nested_catalog->nested_statistics.Get();
    s.PopulateToParent(&stats_counters);
  }

  // Count various directory entry types in the catalog to fill up the catalog
  // statistics counters introduced in the current catalog schema
  catalog::SqlCatalog count_regular(writable,
    std::string("SELECT count(*), sum(size) FROM catalog ") +
    "WHERE flags & " + StringifyInt(catalog::SqlDirent::kFlagFile) +
    " AND NOT flags & " + StringifyInt(catalog::SqlDirent::kFlagLink) +
    " AND NOT flags & " + StringifyInt(catalog::SqlDirent::kFlagFileSpecial) +
    ";");
  catalog::SqlCatalog count_external(writable,
    std::string("SELECT count(*), sum(size) FROM catalog ") +
    "WHERE flags & " + StringifyInt(catalog::SqlDirent::kFlagFileExternal) +
    ";");
  catalog::SqlCatalog count_symlink(writable,
    std::string("SELECT count(*) FROM catalog ") +
    "WHERE flags & " + StringifyInt(catalog::SqlDirent::kFlagLink) + ";");
  catalog::SqlCatalog count_special(writable,
    std::string("SELECT count(*) FROM catalog ") +
    "WHERE flags & " + StringifyInt(catalog::SqlDirent::kFlagFileSpecial) +
    ";");
  catalog::SqlCatalog count_xattr(writable,
    std::string("SELECT count(*) FROM catalog ") +
    "WHERE xattr IS NOT NULL;");
  catalog::SqlCatalog count_chunk(writable,
    std::string("SELECT count(*), sum(size) FROM catalog ") +
    "WHERE flags & " + StringifyInt(catalog::SqlDirent::kFlagFileChunk) + ";");
  catalog::SqlCatalog count_dir(writable,
    std::string("SELECT count(*) FROM catalog ") +
    "WHERE flags & " + StringifyInt(catalog::SqlDirent::kFlagDir) + ";");
  catalog::SqlCatalog count_chunk_blobs(writable,
    "SELECT count(*) FROM chunks;");

  retval = count_regular.FetchRow() &&
           count_external.FetchRow() &&
           count_symlink.FetchRow() &&
           count_special.FetchRow() &&
           count_xattr.FetchRow() &&
           count_chunk.FetchRow() &&
           count_dir.FetchRow() &&
           count_chunk_blobs.FetchRow();
  if (!retval) {
    Error("Failed to collect catalog statistics", data);
    return false;
  }

  stats_counters.self.regular_files       = count_regular.RetrieveInt64(0);
  stats_counters.self.symlinks            = count_symlink.RetrieveInt64(0);
  stats_counters.self.specials            = count_special.RetrieveInt64(0);
  stats_counters.self.directories         = count_dir.RetrieveInt64(0);
  stats_counters.self.nested_catalogs     = data->nested_catalogs.size();
  stats_counters.self.chunked_files       = count_chunk.RetrieveInt64(0);
  stats_counters.self.file_chunks         = count_chunk_blobs.RetrieveInt64(0);
  stats_counters.self.file_size           = count_regular.RetrieveInt64(1);
  stats_counters.self.chunked_file_size   = count_chunk.RetrieveInt64(1);
  stats_counters.self.xattrs              = count_xattr.RetrieveInt64(0);
  stats_counters.self.externals           = count_external.RetrieveInt64(0);
  stats_counters.self.external_file_size  = count_external.RetrieveInt64(1);

  // Write back the generated statistics counters into the catalog database
  catalog::Counters counters;
  counters.ApplyDelta(stats_counters);
  retval = counters.InsertIntoDatabase(writable);
  if (!retval) {
    Error("Failed to write new statistics counters to database", data);
    return false;
  }

  // Push the generated statistics counters up to the parent catalog
  data->nested_statistics.Set(stats_counters);

  return true;
}


bool CommandMigrate::StatsMigrationWorker::CommitDatabaseTransaction(
  PendingCatalog *data) const
{
  assert(!data->HasNew());
  GetWritable(data->old_catalog)->Commit();
  return true;
}

}  // namespace swissknife
