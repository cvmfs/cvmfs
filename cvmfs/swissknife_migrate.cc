/**
 * This file is part of the CernVM File System.
 */

#include "swissknife_migrate.h"

#include "catalog_traversal.h"
#include "catalog_sql.h"
#include "catalog_rw.h"

#include "logging.h"

#include <sys/resource.h>
#include <sstream>

using namespace swissknife;
using namespace catalog;

catalog::DirectoryEntry  CommandMigrate::nested_catalog_marker_;

CommandMigrate::CommandMigrate() :
  file_descriptor_limit_(8192),
  catalog_count_(0),
  uid_(0),
  gid_(0),
  root_catalog_(NULL)
{
  atomic_init32(&catalogs_processed_);
}


ParameterList CommandMigrate::GetParams() {
  ParameterList result;
  result.push_back(Parameter('r', "repository URL (absolute local path "
                                  "or remote URL)",
                             false, false));
  result.push_back(Parameter('u', "upstream definition string",
                             false, false));
  result.push_back(Parameter('o', "manifest output file",
                             false, false));
  result.push_back(Parameter('t', "temporary directory for catalog decompress",
                             false, false));
  result.push_back(Parameter('p', "user id to be used for this repository",
                             false, false));
  result.push_back(Parameter('g', "group id to be used for this repository",
                             false, false));
  result.push_back(Parameter('n', "fully qualified repository name",
                             true, false));
  result.push_back(Parameter('k', "repository master key(s)",
                             true, false));
  result.push_back(Parameter('f', "fix nested catalog transition points",
                             true, true));
  result.push_back(Parameter('l', "disable linkcount analysis of files",
                             true, true));
  result.push_back(Parameter('s', "enable collection of catalog statistics",
                             true, true));
  return result;
}


void Error(const std::string &message) {
  LogCvmfs(kLogCatalog, kLogStderr, message.c_str());
}


void Error(const std::string                     &message,
           const CommandMigrate::PendingCatalog  *catalog) {
  std::stringstream ss;
  ss << message << std::endl
     << "Catalog: " << catalog->root_path();
  Error(ss.str());
}


void Error(const std::string                     &message,
           const catalog::Sql                    &statement,
           const CommandMigrate::PendingCatalog  *catalog) {
  std::stringstream ss;
  ss << message << std::endl
     << "SQLite:  " << statement.GetLastError() << " - "
                    << statement.GetLastErrorMsg();
  Error(ss.str(), catalog);
}


int CommandMigrate::Main(const ArgumentList &args) {
  // parameter parsing
  const std::string &repo_url           = *args.find('r')->second;
  const std::string &spooler            = *args.find('u')->second;
  const std::string &manifest_path      = *args.find('o')->second;
  const std::string &decompress_tmp_dir = *args.find('t')->second;
  const std::string &uid                = *args.find('p')->second;
  const std::string &gid                = *args.find('g')->second;
  const std::string &repo_name          = (args.count('n') > 0)      ?
                                             *args.find('n')->second :
                                             "";
  const std::string &repo_keys          = (args.count('k') > 0)      ?
                                             *args.find('k')->second :
                                             "";
  const bool fix_transition_points      = (args.count('f') > 0);
  const bool analyze_file_linkcounts    = (args.count('l') == 0);
  const bool collect_catalog_statistics = (args.count('s') > 0);

  temporary_directory_ = decompress_tmp_dir;
  std::istringstream uid_ss(uid); uid_ss >> uid_;
  if (uid_ss.fail()) {
    Error("Failed to parse user ID");
    return 1;
  }
  std::istringstream gid_ss(gid); gid_ss >> gid_;
  if (gid_ss.fail()) {
    Error("Failed to parse group ID");
    return 1;
  }

  // we might need a lot of file descriptors
  if (! RaiseFileDescriptorLimit()) {
    Error("Failed to raise file descriptor limits");
    return 2;
  }

  // put SQLite into multithreaded mode
  if (! ConfigureSQLite()) {
    Error("Failed to preconfigure SQLite library");
    return 3;
  }

  // load the full catalog hierarchy
  LogCvmfs(kLogCatalog, kLogStdout, "Loading current catalog tree...");
  const bool generate_full_catalog_tree = true;
  CatalogTraversal<CommandMigrate> traversal(
    this,
    &CommandMigrate::CatalogCallback,
    repo_url,
    repo_name,
    repo_keys,
    generate_full_catalog_tree,
    decompress_tmp_dir);
  catalog_loading_stopwatch_.Start();
  const bool loading_successful = traversal.Traverse();
  catalog_loading_stopwatch_.Stop();

  if (!loading_successful) {
    Error("Failed to load catalog tree");
    return 4;
  }

  LogCvmfs(kLogCatalog, kLogStdout, "Loaded %d catalogs", catalog_count_);
  assert (root_catalog_ != NULL);

  // create an upstream spooler
  const upload::SpoolerDefinition spooler_definition(spooler);
  spooler_ = upload::Spooler::Construct(spooler_definition);
  if (!spooler_) {
    Error("Failed to create upstream Spooler.");
    return 5;
  }
  spooler_->RegisterListener(&CommandMigrate::UploadCallback, this);

  // generate and upload a nested catalog marker
  if (! GenerateNestedCatalogMarkerChunk()) {
    Error("Failed to create a nested catalog marker.");
    return 6;
  }
  spooler_->WaitForUpload();

  // create a concurrent catalog migration facility
  const unsigned int cpus = GetNumberOfCpuCores();
  MigrationWorker::worker_context context(spooler_definition.temporary_path,
                                          fix_transition_points,
                                          analyze_file_linkcounts,
                                          collect_catalog_statistics,
                                          uid_,
                                          gid_);
  concurrent_migration_ = new ConcurrentWorkers<MigrationWorker>(
                                cpus,
                                cpus * 10,
                                &context);
  if (! concurrent_migration_->Initialize()) {
    Error("Failed to initialize worker migration system.");
    return 7;
  }
  concurrent_migration_->RegisterListener(&CommandMigrate::MigrationCallback,
                                          this);

  // migrate catalogs recursively (starting with the deepest nested catalogs)
  LogCvmfs(kLogCatalog, kLogStdout, "\nMigrating catalogs...");
  PendingCatalog *root_catalog = new PendingCatalog(root_catalog_);
  migration_stopwatch_.Start();
  ConvertCatalogsRecursively(root_catalog);
  concurrent_migration_->WaitForEmptyQueue();
  spooler_->WaitForUpload();
  migration_stopwatch_.Stop();

  const unsigned int errors = concurrent_migration_->GetNumberOfFailedJobs() +
                              spooler_->GetNumberOfErrors();

  LogCvmfs(kLogCatalog, kLogStdout, "Catalog Migration finished with %d "
                                    "errors.",
           errors);
  if (errors > 0) {
    LogCvmfs(kLogCatalog, kLogStdout, "\nCatalog Migration produced errors\n"
                                      "Aborting...");
    return 8;
  }

  // commit the new (migrated) repository revision...
  LogCvmfs(kLogCatalog, kLogStdout, "\nCommitting migrated repository "
                                    "revision...");
  const hash::Any   &root_catalog_hash = root_catalog->new_catalog_hash.Get();
  const std::string &root_catalog_path = root_catalog->root_path();
  manifest::Manifest manifest(root_catalog_hash,
                              root_catalog_path);
  manifest.set_ttl(root_catalog->new_catalog->GetTTL());
  manifest.set_revision(root_catalog->new_catalog->GetRevision());
  if (! manifest.Export(manifest_path)) {
    Error("Manifest export failed.\nAborting...");
    return 9;
  }

  // get rid of the open root catalog
  delete root_catalog;

  // analyze collected statistics
  if (collect_catalog_statistics) {
    LogCvmfs(kLogCatalog, kLogStdout, "\nCollected statistics results:");
    AnalyzeCatalogStatistics();
  }

  // all done...
  LogCvmfs(kLogCatalog, kLogStdout, "\nCatalog Migration succeeded");
  return 0;
}


void CommandMigrate::CatalogCallback(const Catalog*    catalog,
                                     const hash::Any&  catalog_hash,
                                     const unsigned    tree_level) {
  std::string tree_indent;
  std::string hash_string;
  std::string path;

  for (unsigned int i = 1; i < tree_level; ++i) {
    tree_indent += "\u2502  ";
  }

  if (tree_level > 0) {
    tree_indent += "\u251C\u2500 ";
  }

  hash_string = catalog_hash.ToString();

  path = catalog->path().ToString();
  if (path.empty()) {
    path = "/";
    root_catalog_ = catalog;
  }

  LogCvmfs(kLogCatalog, kLogStdout, "%s%s %s",
    tree_indent.c_str(),
    hash_string.c_str(),
    path.c_str());

  ++catalog_count_;
}


void CommandMigrate::MigrationCallback(PendingCatalog *const &data) {
  // check if the migration of the catalog was successful
  if (!data->success) {
    Error("Catalog migration failed! Aborting...");
    exit(1);
    return;
  }

  const std::string &path = data->new_catalog->database_path();

  // save the processed catalog in the pending map
  {
    LockGuard<PendingCatalogMap> guard(pending_catalogs_);
    assert (pending_catalogs_.find(path) == pending_catalogs_.end());
    pending_catalogs_[path] = data;
  }
  catalog_statistics_list_.Insert(data->statistics);

  // schedule the compression and upload of the catalog
  spooler_->ProcessCatalog(path);
}


void CommandMigrate::UploadCallback(const upload::SpoolerResult &result) {
  const std::string &path = result.local_path;

  // check if the upload was successful
  if (result.return_code != 0) {
    std::stringstream ss;
    ss << "Failed to upload file " << path << std::endl
       << "Aborting...";
    Error(ss.str());
    exit(2);
    return;
  }
  assert (result.file_chunks.size() == 0);

  // remove the just uploaded file
  unlink(path.c_str());

  // uploaded nested catalog marker... generate and cache DirectoryEntry for it
  if (path == nested_catalog_marker_tmp_path_) {
    CreateNestedCatalogMarkerDirent(result.content_hash);
    return;

  } else {

    // find the catalog path in the pending catalogs and remove it from the list
    PendingCatalog *catalog;
    {
      LockGuard<PendingCatalogMap> guard(pending_catalogs_);
      PendingCatalogMap::iterator i = pending_catalogs_.find(path);
      assert (i != pending_catalogs_.end());
      catalog = const_cast<PendingCatalog*>(i->second);
      pending_catalogs_.erase(i);
    }

    // the catalog is completely processed... fill the hash-future to allow the
    // processing of parent catalogs
    catalog->new_catalog_hash.Set(result.content_hash);

    atomic_inc32(&catalogs_processed_);
    const unsigned int processed = (atomic_read32(&catalogs_processed_) * 100) /
                                   catalog_count_;
    LogCvmfs(kLogCatalog, kLogStdout, "[%d%%] migrated and uploaded %sC %s",
             processed,
             result.content_hash.ToString().c_str(),
             catalog->root_path().c_str());
  }
}


void CommandMigrate::ConvertCatalogsRecursively(PendingCatalog *catalog) {
  // first migrate all nested catalogs (depth first traversal)
  const CatalogList nested_catalogs = catalog->old_catalog->GetChildren();
  CatalogList::const_iterator i    = nested_catalogs.begin();
  CatalogList::const_iterator iend = nested_catalogs.end();
  catalog->nested_catalogs.reserve(nested_catalogs.size());
  for (; i != iend; ++i) {
    PendingCatalog *new_nested = new PendingCatalog(*i);
    catalog->nested_catalogs.push_back(new_nested);
    ConvertCatalogsRecursively(new_nested);
  }

  // migrate this catalog referencing all it's (already migrated) children
  concurrent_migration_->Schedule(catalog);
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
  const unsigned int number_of_catalogs        = catalog_statistics_list_.size();
  unsigned int       aggregated_entry_count    = 0;
  unsigned int       aggregated_max_row_id     = 0;
  unsigned int       aggregated_hardlink_count = 0;
  unsigned int       aggregated_linkcounts     = 0;
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
  const unsigned int unused_inodes =
                                 aggregated_max_row_id - aggregated_entry_count;
  const float        ratio = ((float)unused_inodes           /
                              (float)aggregated_max_row_id) * 100.0f;
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
  const double average_migration_time = aggregated_migration_time /
                                        (double)number_of_catalogs;
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


CommandMigrate::MigrationWorker::MigrationWorker(const worker_context *context) :
  temporary_directory_            (context->temporary_directory),
  fix_nested_catalog_transitions_ (context->fix_nested_catalog_transitions),
  analyze_file_linkcounts_        (context->analyze_file_linkcounts),
  collect_catalog_statistics_     (context->collect_catalog_statistics),
  uid_                            (context->uid),
  gid_                            (context->gid)
{}


CommandMigrate::MigrationWorker::~MigrationWorker() {}


void CommandMigrate::MigrationWorker::operator()(const expected_data &data) {
  migration_stopwatch_.Start();
  const bool success =
    CreateNewEmptyCatalog            (data) &&
    CheckDatabaseSchemaCompatibility (data) &&
    AttachOldCatalogDatabase         (data) &&
    StartDatabaseTransaction         (data) &&
    MigrateFileMetadata              (data) &&
    MigrateNestedCatalogReferences   (data) &&
    FixNestedCatalogTransitionPoints (data) &&
    GenerateCatalogStatistics        (data) &&
    FindRootEntryInformation         (data) &&
    CommitDatabaseTransaction        (data) &&
    CollectAndAggregateStatistics    (data) &&
    DetachOldCatalogDatabase         (data) &&
    CleanupNestedCatalogs            (data);
  data->success = success;
  migration_stopwatch_.Stop();

  data->statistics.migration_time = migration_stopwatch_.GetTime();
  migration_stopwatch_.Reset();

  // Note: MigrationCallback() will take care of the result...
  if (success) {
    master()->JobSuccessful(data);
  } else {
    master()->JobFailed(data);
  }
}


bool CommandMigrate::MigrationWorker::CreateNewEmptyCatalog(
                                                   PendingCatalog *data) const {
  const std::string root_path = data->root_path();

  // create a new catalog database schema
  bool retval;
  const std::string catalog_db = CreateTempPath(temporary_directory_ + "/catalog",
                                                0666);
  if (catalog_db.empty()) {
    Error("Failed to create temporary file for the new catalog database.");
    return false;
  }
  retval = Database::Create(catalog_db, root_path);
  if (! retval) {
    Error("Failed to create database for new catalog");
    unlink(catalog_db.c_str());
    return false;
  }

  // Attach the just created nested catalog database
  WritableCatalog *writable_catalog =
    AttachFreelyRw(root_path, catalog_db, hash::Any(hash::kSha1));
  if (writable_catalog == NULL) {
    Error("Failed to open database for new catalog");
    unlink(catalog_db.c_str());
    return false;
  }

  data->new_catalog = writable_catalog;
  return true;
}


bool CommandMigrate::MigrationWorker::CheckDatabaseSchemaCompatibility(
                                                   PendingCatalog *data) const {
  const catalog::Database &old_catalog = data->old_catalog->database();
  const catalog::Database &new_catalog = data->new_catalog->database();

  if (! new_catalog.ready()                                                  ||
      (new_catalog.schema_version() < Database::kLatestSupportedSchema -
                                      Database::kSchemaEpsilon         ||
       new_catalog.schema_version() > Database::kLatestSupportedSchema +
                                      Database::kSchemaEpsilon)              ||
      (old_catalog.schema_version() > 2.1 + Database::kSchemaEpsilon)) {
    Error("Failed to meet database requirements for migration.", data);
    return false;
  }
  return true;
}


bool CommandMigrate::MigrationWorker::AttachOldCatalogDatabase(
                                                   PendingCatalog *data) const {
  const catalog::Database &old_catalog = data->old_catalog->database();
  const catalog::Database &new_catalog = data->new_catalog->database();

  Sql sql_attach_new(new_catalog,
    "ATTACH '" + old_catalog.filename() + "' AS old;"
  );
  bool retval = sql_attach_new.Execute();

  // remove the hardlink to the old database file (temporary file), it will not
  // be needed anymore... data will get deleted when the database is closed
  unlink(data->old_catalog->database().filename().c_str());

  if (! retval) {
    Error("Failed to attach database of old catalog", sql_attach_new, data);
    return false;
  }
  return true;
}


bool CommandMigrate::MigrationWorker::StartDatabaseTransaction(
                                                   PendingCatalog *data) const {
  data->new_catalog->Transaction();
  return true;
}


bool CommandMigrate::MigrationWorker::MigrateFileMetadata(
                                                   PendingCatalog *data) const {
  assert (! data->new_catalog->IsDirty());
  bool retval;

  const Database &writable = data->new_catalog->database();

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
  Sql sql_create_hardlinks_table(writable,
    "CREATE TEMPORARY TABLE hardlinks "
    "  (  hardlink_group_id  INTEGER PRIMARY KEY AUTOINCREMENT, "
    "     inode              INTEGER, "
    "     linkcount          INTEGER, "
    "     CONSTRAINT unique_inode UNIQUE (inode)  );");
  retval = sql_create_hardlinks_table.Execute();
  if (! retval) {
    Error("Failed to create temporary hardlink analysis table",
          sql_create_hardlinks_table, data);
    return false;
  }

  // Directory Linkcount scratch space.
  // Directory linkcounts can be obtained from the directory hierarchy reflected
  // in the old style catalogs. The new catalog schema asks for this specific
  // linkcount. Directory linkcount analysis results will be put into this
  // temporary table
  Sql sql_create_linkcounts_table(writable,
    "CREATE TEMPORARY TABLE dir_linkcounts "
    "  (  inode      INTEGER PRIMARY KEY, "
    "     linkcount  INTEGER  );");
  retval = sql_create_linkcounts_table.Execute();
  if (! retval) {
    Error("Failed to create tmeporary directory linkcount analysis table",
          sql_create_linkcounts_table, data);
  }

  // it is possible to skip this step
  // in that case all hardlink inodes with a (potential) linkcount > 1 will get
  // degraded to files containing the same content
  if (analyze_file_linkcounts_) {
    retval = AnalyzeFileLinkcounts(data);
    if (! retval) {
      return false;
    }
  }

  // analyze the linkcounts of directories
  //   - each directory has a linkcount of at least 2 (empty directory)
  //     (link in parent directory and self reference (cd .) )
  //   - for each child directory, the parent's link count is incremented by 1
  //     (parent reference in child (cd ..) )
  //
  // Note: we deliberately exclude nested catalog mountpoints here, since we
  //       cannot check the number of containing directories here
  Sql sql_dir_linkcounts(writable,
    "INSERT INTO dir_linkcounts "
    "  SELECT c1.inode as inode, "
    "         SUM(IFNULL(MIN(c2.inode,1),0)) + 2 as linkcount "
    "  FROM old.catalog as c1 "
    "  LEFT JOIN old.catalog as c2 "
    "    ON c2.parent_1 = c1.md5path_1 AND "
    "       c2.parent_2 = c1.md5path_2 AND "
    "       c2.flags & :flag_dir_1 "
    "  WHERE c1.flags & :flag_dir_2 AND "
    "        NOT c1.flags & :flag_nested_mountpoint "
    "  GROUP BY c1.inode;");
  retval =
    sql_dir_linkcounts.BindInt64(1, SqlDirent::kFlagDir)                 &&
    sql_dir_linkcounts.BindInt64(2, SqlDirent::kFlagDir)                 &&
    sql_dir_linkcounts.BindInt64(3, SqlDirent::kFlagDirNestedMountpoint) &&
    sql_dir_linkcounts.Execute();
  if (! retval) {
    Error("Failed to analyze directory specific linkcounts",
          sql_dir_linkcounts, data);
    if (sql_dir_linkcounts.GetLastError() == SQLITE_CONSTRAINT) {
      Error("Obviously your catalogs are corrupted, since we found a directory"
            "inode that is a file inode at the same time!");
    }
    return false;
  }

  // copy the old file meta information into the new catalog schema
  //   here we also add the previously analyzed hardlink/linkcount information
  //   from both temporary tables "hardlinks" and "dir_linkcounts".
  //
  // Note: nested catalog mountpoints still need to be treated separately
  //       (see MigrateNestedCatalogReferences() for details)
  Sql migrate_file_meta_data(writable,
    "INSERT INTO catalog "
    "  SELECT md5path_1, md5path_2, "
    "         parent_1, parent_2, "
    "         IFNULL(hardlink_group_id, 0) << 32 | "
    "         COALESCE(hardlinks.linkcount, dir_linkcounts.linkcount, 1) "
    "           AS hardlinks, "
    "         hash, size, mode, mtime, "
    "         flags, name, symlink, "
    "         :uid, "
    "         :gid "
    "  FROM old.catalog "
    "  LEFT JOIN hardlinks "
    "    ON catalog.inode = hardlinks.inode "
    "  LEFT JOIN dir_linkcounts "
    "    ON catalog.inode = dir_linkcounts.inode;"
  );
  retval = migrate_file_meta_data.BindInt64(1, uid_) &&
           migrate_file_meta_data.BindInt64(2, gid_) &&
           migrate_file_meta_data.Execute();
  if (! retval) {
    Error("Failed to migrate the file system meta data",
          migrate_file_meta_data, data);
    return false;
  }

  // if we deal with a nested catalog, we need to add a .cvmfscatalog entry
  // since it was not present in the old repository specification but is needed
  // now!
  if (! data->IsRoot()) {
    const DirectoryEntry &nested_marker =
                                 CommandMigrate::GetNestedCatalogMarkerDirent();
    SqlDirentInsert insert_nested_marker(writable);
    const std::string   root_path   = data->root_path();
    const std::string   file_path   = root_path +
                                      "/" + nested_marker.name().ToString();
    const hash::Md5    &path_hash   = hash::Md5(file_path.data(),
                                                file_path.size());
    const hash::Md5    &parent_hash = hash::Md5(root_path.data(),
                                                root_path.size());
    retval = insert_nested_marker.BindPathHash(path_hash)         &&
             insert_nested_marker.BindParentPathHash(parent_hash) &&
             insert_nested_marker.BindDirent(nested_marker)       &&
             insert_nested_marker.Execute();
    if (! retval) {
      Error("Failed to insert nested catalog marker into new nested catalog.",
            insert_nested_marker, data);
      return false;
    }
  }

  // copy (and update) the properties fields
  //
  // Note: The 'schema' is explicitly not copied to the new catalog.
  //       Each catalog contains a revision, which is also copied here and that
  //       is later updated by calling catalog->IncrementRevision()
  Sql copy_properties(writable,
    "INSERT OR REPLACE INTO properties "
    "  SELECT key, value "
    "  FROM old.properties "
    "  WHERE key != 'schema';"
  );
  retval = copy_properties.Execute();
  if (! retval) {
    Error("Failed to migrate the properties table.", copy_properties, data);
    return false;
  }

  // set the previous revision hash in the new catalog to the old catalog
  //   we are doing the whole migration as a new snapshot that does not change
  //   any files, but just bumpes the catalog schema to the latest version
  data->new_catalog->SetPreviousRevision(data->old_catalog->hash());
  data->new_catalog->IncrementRevision();
  data->new_catalog->UpdateLastModified();

  return true;
}


bool CommandMigrate::MigrationWorker::AnalyzeFileLinkcounts(
                                                   PendingCatalog *data) const {
  const Database &writable = data->new_catalog->database();
  bool retval;

  // analyze the hardlink relationships in the old catalog
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
  Sql sql_create_hardlinks_scratch_table(writable,
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
  if (! retval) {
    Error("Failed to create temporary scratch table for hardlink analysis",
          sql_create_hardlinks_scratch_table, data);
    return false;
  }

  // Figures out which hardlink candidates are supported by CVMFS and can be
  // transferred into the new catalog as so called hardlink groups. Unsupported
  // hardlinks need to be discarded and treated as normal files containing the
  // exact same data
  Sql fill_linkcount_table_for_files(writable,
    "INSERT INTO hardlinks (inode, linkcount)"
    "  SELECT inode, count(*) as linkcount "
    "  FROM ( "
         // recombine supported hardlink inodes with their actual manifested hard-
         // links in the catalog.
         // Note: for each directory entry pointing to the same supported hardlink
         //       inode we have a distinct MD5 path hash
    "    SELECT DISTINCT hl.inode, hl.md5path_1, hl.md5path_2 "
    "    FROM ( "
           // sort out supported hardlink inodes from unsupported ones by locality
           // Note: see the next comment for the nested SELECT
    "      SELECT inode "
    "      FROM ( "
    "        SELECT inode, count(*) AS cnt "
    "        FROM ( "
               // go through the potential hardlinks and collect location infor-
               // mation about them.
               // Note: we only support hardlinks that all reside in the same
               //       directory, thus having the same parent (c1p* == c2p*)
               //   --> For supported hardlink candidates the SELECT DISTINCT will
               //       produce only a single row, whereas others produce more
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
  if (! retval) {
    Error("Failed to analyze hardlink relationships for files.",
          fill_linkcount_table_for_files, data);
    return false;
  }

  // The file linkcount and hardlink analysis is finished and the scratch table
  // can be deleted...
  Sql drop_hardlink_scratch_space(writable, "DROP TABLE hl_scratch;");
  retval = drop_hardlink_scratch_space.Execute();
  if (! retval) {
    Error("Failed to remove file linkcount analysis scratch table",
          drop_hardlink_scratch_space, data);
    return false;
  }

  // do some statistics if asked for...
  if (collect_catalog_statistics_) {
    Sql count_hardlinks(writable,
      "SELECT count(*), sum(linkcount) FROM hardlinks;");
    retval = count_hardlinks.FetchRow();
    if (! retval) {
      Error("Failed to count the generated file hardlinks for statistics",
            count_hardlinks, data);
      return false;
    }

    data->statistics.hardlink_group_count  += count_hardlinks.RetrieveInt64(0);
    data->statistics.aggregated_linkcounts += count_hardlinks.RetrieveInt64(1);
  }

  return true;
}


bool CommandMigrate::MigrationWorker::MigrateNestedCatalogReferences(
                                                   PendingCatalog *data) const {
  const Database &writable = data->new_catalog->database();
  bool retval;

  // preparing the SQL statements for nested catalog addition
  Sql add_nested_catalog(writable,
    "INSERT INTO nested_catalogs (path, sha1) VALUES (:path, :sha1);"
  );
  Sql update_mntpnt_linkcount(writable,
    "UPDATE catalog "
    "SET hardlinks = :linkcount "
    "WHERE md5path_1 = :md5_1 AND md5path_2 = :md5_2;"
  );

  // unbox the nested catalogs (possibly waiting for migration of them first)
  PendingCatalogList::const_iterator i    = data->nested_catalogs.begin();
  PendingCatalogList::const_iterator iend = data->nested_catalogs.end();
  for (; i != iend; ++i) {
    // collect information about the nested catalog
    PendingCatalog *nested_catalog = *i;
    const DirectoryEntry root_entry = nested_catalog->root_entry.Get();
    const std::string &root_path    = nested_catalog->root_path();
    const hash::Any catalog_hash    = nested_catalog->new_catalog_hash.Get();

    // update the nested catalog mountpoint directory entry with the correct
    // linkcount that was determined while processing the nested catalog
    const hash::Md5 mountpoint_hash = hash::Md5(root_path.data(),
                                                root_path.size());
    retval =
      update_mntpnt_linkcount.BindInt64(1, root_entry.linkcount());
      update_mntpnt_linkcount.BindMd5(2, 3, mountpoint_hash);
      update_mntpnt_linkcount.Execute();
    if (! retval) {
      Error("Failed to update linkcount of nested catalog mountpoint",
            update_mntpnt_linkcount, data);
      return false;
    }
    update_mntpnt_linkcount.Reset();

    // insert the updated nested catalog reference into the new catalog
    retval =
      add_nested_catalog.BindText(1, root_path) &&
      add_nested_catalog.BindText(2, catalog_hash.ToString()) &&
      add_nested_catalog.Execute();
    if (! retval) {
      Error("Failed to add nested catalog link", add_nested_catalog, data);
      return false;
    }
    add_nested_catalog.Reset();
  }

  return true;
}


bool CommandMigrate::MigrationWorker::FixNestedCatalogTransitionPoints(
                                                   PendingCatalog *data) const {
  if (! fix_nested_catalog_transitions_) {
    // fixing transition point mismatches is not enabled...
    return true;
  }

  typedef DirectoryEntry::Difference Difference;

  const Database &writable = data->new_catalog->database();
  bool retval;

  SqlLookupPathHash lookup_mountpoint(writable);
  SqlDirentUpdate   update_directory_entry(writable);

  // unbox the nested catalogs (possibly waiting for migration of them first)
  PendingCatalogList::const_iterator i    = data->nested_catalogs.begin();
  PendingCatalogList::const_iterator iend = data->nested_catalogs.end();
  for (; i != iend; ++i) {
    // collect information about the nested catalog
    PendingCatalog *nested_catalog = *i;
    const DirectoryEntry nested_root_entry = nested_catalog->root_entry.Get();
    const std::string &nested_root_path    = nested_catalog->root_path();
    const hash::Md5 mountpoint_path_hash = hash::Md5(nested_root_path.data(),
                                                     nested_root_path.size());

    // retrieve the nested catalog mountpoint from the current catalog
    retval = lookup_mountpoint.BindPathHash(mountpoint_path_hash) &&
             lookup_mountpoint.FetchRow();
    if (! retval) {
      Error("Failed to fetch nested catalog mountpoint to check for compatible"
            "transition points", lookup_mountpoint, data);
      return false;
    }

    DirectoryEntry mountpoint_entry =
                                 lookup_mountpoint.GetDirent(data->new_catalog);
    lookup_mountpoint.Reset();

    // compare nested catalog mountpoint and nested catalog root entries
    DirectoryEntry::Differences diffs =
                                  mountpoint_entry.CompareTo(nested_root_entry);

    // we MUST deal with two directory entries that are a pair of nested cata-
    // log mountpoint and root entry! Thus we expect their transition flags to
    // differ and their name to be the same.
    assert (diffs & Difference::kNestedCatalogTransitionFlags);
    assert ((diffs & Difference::kName) == 0);

    // check if there are other differences except the nested catalog transition
    // flags and fix them...
    if ((diffs ^ Difference::kNestedCatalogTransitionFlags) != 0) {
      // if we found differences, we still assume a couple of directory entry
      // fields to be the same, otherwise some severe stuff would be wrong...
      if ((diffs & Difference::kChecksum)        ||
          (diffs & Difference::kLinkcount)       ||
          (diffs & Difference::kSymlink)         ||
          (diffs & Difference::kChunkedFileFlag)    ) {
        std::stringstream ss;
        ss << "Found an irreparable mismatch in a nested catalog transition "
              "point at '" << nested_root_path << "'" << std::endl
           << "Aborting..." << std::endl;
        Error(ss.str());
      }

      // copy the properties from the nested catalog root entry into the mount-
      // point entry to bring them in sync again
      CommandMigrate::FixNestedCatalogTransitionPoint(mountpoint_entry,
                                                      nested_root_entry);

      // save the nested catalog mountpoint entry into the catalog
      retval = update_directory_entry.BindPathHash(mountpoint_path_hash) &&
               update_directory_entry.BindDirent(mountpoint_entry)       &&
               update_directory_entry.Execute();
      if (! retval) {
        Error("Failed to save resynchronized nested catalog mountpoint into "
              "catalog database", update_directory_entry, data);
        return false;
      }
      update_directory_entry.Reset();

      // fixing of this mountpoint went well... inform the user that this minor
      // issue occured
      LogCvmfs(kLogCatalog, kLogStdout, "NOTE: fixed incompatible nested "
                                        "catalog transition point at: '%s' ",
               nested_root_path.c_str());
    }
  }

  return true;
}


void CommandMigrate::FixNestedCatalogTransitionPoint(
                        catalog::DirectoryEntry &mountpoint,
                  const catalog::DirectoryEntry &nested_root) {
  // replace some file system parameters in the mountpoint to resync it with
  // the nested root of the corresponding nested catalog
  //
  // Note: this method relies on CommandMigrate being a friend of DirectoryEntry
  mountpoint.mode_  = nested_root.mode_;
  mountpoint.uid_   = nested_root.uid_;
  mountpoint.gid_   = nested_root.gid_;
  mountpoint.size_  = nested_root.size_;
  mountpoint.mtime_ = nested_root.mtime_;
}


const catalog::DirectoryEntry& CommandMigrate::GetNestedCatalogMarkerDirent() {
  // this is pre-initialized singleton... it MUST be already there...
  assert (nested_catalog_marker_.name_ == ".cvmfscatalog");
  return nested_catalog_marker_;
}


bool CommandMigrate::GenerateNestedCatalogMarkerChunk() {
  // create an empty nested catalog marker file
  nested_catalog_marker_tmp_path_ =
                         CreateTempPath(temporary_directory_ + "/.cvmfscatalog",
                                        0644);
  if (nested_catalog_marker_tmp_path_.empty()) {
    Error("Failed to create temp file for nested catalog marker dummy.");
    return false;
  }

  // process and upload it to the backend storage
  spooler_->Process(nested_catalog_marker_tmp_path_);
  return true;
}


void CommandMigrate::CreateNestedCatalogMarkerDirent(
                                                const hash::Any &content_hash) {
  // generate it only once
  assert (nested_catalog_marker_.name_ != ".cvmfscatalog");

  // fill the DirectoryEntry structure will all needed information
  nested_catalog_marker_.name_      = ".cvmfscatalog";
  nested_catalog_marker_.mode_      = 33188;
  nested_catalog_marker_.uid_       = uid_;
  nested_catalog_marker_.gid_       = gid_;
  nested_catalog_marker_.size_      = 0;
  nested_catalog_marker_.mtime_     = time(NULL);
  nested_catalog_marker_.linkcount_ = 1;
  nested_catalog_marker_.checksum_  = content_hash;
}


bool CommandMigrate::MigrationWorker::GenerateCatalogStatistics(
                                                   PendingCatalog *data) const {
  bool retval = false;
  const Database &writable = data->new_catalog->database();

  // Aggregated the statistics counters of all nested catalogs
  // Note: we might need to wait until nested catalogs are sucessfully processed
  DeltaCounters aggregated_counters;
  PendingCatalogList::const_iterator i    = data->nested_catalogs.begin();
  PendingCatalogList::const_iterator iend = data->nested_catalogs.end();
  for (; i != iend; ++i) {
    const PendingCatalog *nested_catalog = *i;
    const catalog::DeltaCounters &s = nested_catalog->nested_statistics.Get();
    s.PopulateToParent(aggregated_counters);
  }

  // count various directory entry types in the catalog to fill up the catalog
  // statistics counters introduced in the current catalog schema
  Sql count_regular_files(writable,
    "SELECT count(*) FROM catalog "
    "                WHERE  flags & :flag_file "
    "                       AND NOT flags & :flag_link;");
  Sql count_symlinks(writable,
    "SELECT count(*) FROM catalog WHERE flags & :flag_link;");
  Sql count_directories(writable,
    "SELECT count(*) FROM catalog WHERE flags & :flag_dir;");
  Sql count_nested_clgs(writable,
    "SELECT count(*) FROM nested_catalogs;");

  // run the actual counting queries
  retval =
    count_regular_files.BindInt64(1, SqlDirent::kFlagFile) &&
    count_regular_files.BindInt64(2, SqlDirent::kFlagLink) &&
    count_regular_files.Execute();
  if (! retval) {
    Error("Failed to count regular files.", count_regular_files, data);
    return false;
  }
  retval =
    count_symlinks.BindInt64(1, SqlDirent::kFlagLink) &&
    count_symlinks.Execute();
  if (! retval) {
    Error("Failed to count symlinks.", count_symlinks, data);
    return false;
  }
  retval =
    count_directories.BindInt64(1, SqlDirent::kFlagDir) &&
    count_directories.Execute();
  if (! retval) {
    Error("Failed to count directories.", count_directories, data);
    return false;
  }
  retval = count_nested_clgs.Execute();
  if (! retval) {
    Error("Failed to count nested catalog references.",
          count_nested_clgs, data);
    return false;
  }

  // insert the counted statistics into the DeltaCounters data structure
  aggregated_counters.self.regular_files = count_regular_files.RetrieveInt64(0);
  aggregated_counters.self.symlinks      = count_symlinks.RetrieveInt64(0);
  aggregated_counters.self.directories   = count_directories.RetrieveInt64(0);
  aggregated_counters.self.nested_catalogs = count_nested_clgs.RetrieveInt64(0);

  // write back the generated statistics counters into the catalog database
  aggregated_counters.WriteToDatabase(writable);

  // push the generated statistics counters up to the parent catalog
  data->nested_statistics.Set(aggregated_counters);

  return true;
}


bool CommandMigrate::MigrationWorker::FindRootEntryInformation(
                                                   PendingCatalog *data) const {
  const Database &writable = data->new_catalog->database();
  bool retval;

  std::string root_path = data->root_path();
  hash::Md5 root_path_hash = hash::Md5(root_path.data(), root_path.size());

  SqlLookupPathHash lookup_root_entry(writable);
  retval = lookup_root_entry.BindPathHash(root_path_hash) &&
           lookup_root_entry.FetchRow();
  if (! retval) {
    Error("Failed to retrieve root directory entry of migrated catalog",
          lookup_root_entry, data);
    return false;
  }

  DirectoryEntry entry = lookup_root_entry.GetDirent(data->new_catalog);
  if (entry.linkcount() < 2 || entry.linkcount() >= (1l << 32)) {
    Error("Retrieved linkcount of catalog root entry is not sane.", data);
    return false;
  }

  data->root_entry.Set(entry);
  return true;
}


bool CommandMigrate::MigrationWorker::CommitDatabaseTransaction(
                                                   PendingCatalog *data) const {
  data->new_catalog->Commit();
  return true;
}


bool CommandMigrate::MigrationWorker::CollectAndAggregateStatistics(
                                                   PendingCatalog *data) const {
  if (! collect_catalog_statistics_) {
    return true;
  }

  const Database &writable = data->new_catalog->database();
  bool retval;

  // find out the discrepancy between MAX(rowid) and COUNT(*)
  Sql wasted_inodes(writable,
    "SELECT COUNT(*), MAX(rowid) FROM old.catalog;");
  retval = wasted_inodes.FetchRow();
  if (! retval) {
    Error("Failed to count entries in catalog", wasted_inodes, data);
    return false;
  }
  const unsigned int entry_count = wasted_inodes.RetrieveInt64(0);
  const unsigned int max_row_id  = wasted_inodes.RetrieveInt64(1);

  // save collected information into the central statistics aggregator
  data->statistics.root_path   = data->root_path();
  data->statistics.max_row_id  = max_row_id;
  data->statistics.entry_count = entry_count;

  return true;
}


bool CommandMigrate::MigrationWorker::DetachOldCatalogDatabase(
                                                   PendingCatalog *data) const {
  const Database &writable = data->new_catalog->database();
  Sql detach_old_catalog(writable, "DETACH old;");
  const bool retval = detach_old_catalog.Execute();
  if (! retval) {
    Error("Failed to detach old catalog database.", detach_old_catalog, data);
    return false;
  }
  return true;
}


bool CommandMigrate::MigrationWorker::CleanupNestedCatalogs(
                                                   PendingCatalog *data) const {
  // All nested catalogs of PendingCatalog 'data' are fully processed and
  // accounted. It is safe to get rid of their data structures here!
  DeltaCounters aggregated_counters;
  PendingCatalogList::const_iterator i    = data->nested_catalogs.begin();
  PendingCatalogList::const_iterator iend = data->nested_catalogs.end();
  for (; i != iend; ++i) {
    delete *i;
  }

  data->nested_catalogs.clear();
  return true;
}

