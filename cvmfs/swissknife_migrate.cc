/**
 * This file is part of the CernVM File System.
 */

#include "swissknife_migrate.h"

#include "catalog_traversal.h"
#include "catalog_sql.h"
#include "catalog_rw.h"

#include "logging.h"

using namespace swissknife;
using namespace catalog;

CommandMigrate::CommandMigrate() :
  print_tree_(false),
  print_hash_(false),
  root_catalog_(NULL) {}


ParameterList CommandMigrate::GetParams() {
  ParameterList result;
  result.push_back(Parameter('r', "repository URL (absolute local path "
                                  "or remote URL)",
                             false, false));
  result.push_back(Parameter('u', "upstream definition string",
                             false, false));
  result.push_back(Parameter('n', "fully qualified repository name",
                             true, false));
  result.push_back(Parameter('k', "repository master key(s)",
                             true, false));
  return result;
}


int CommandMigrate::Main(const ArgumentList &args) {
  // parameter parsing
  const std::string &repo_url  = *args.find('r')->second;
  const std::string &spooler   = *args.find('u')->second;
  const std::string &repo_name = (args.count('n') > 0) ? *args.find('n')->second : "";
  const std::string &repo_keys = (args.count('k') > 0) ? *args.find('k')->second : "";

  // create an upstream spooler
  const upload::SpoolerDefinition spooler_definition(spooler);
  spooler_ = upload::Spooler::Construct(spooler_definition);
  if (!spooler_) {
    LogCvmfs(kLogCatalog, kLogStderr, "Failed to create upstream Spooler.");
    return 2;
  }
  spooler_->RegisterListener(&CommandMigrate::UploadCallback, this);

  // create a concurrent catalog migration facility
  const unsigned int cpus = GetNumberOfCpuCores();
  MigrationWorker::worker_context context(spooler_definition.temporary_path);
  concurrent_migration = new ConcurrentWorkers<MigrationWorker>(
                                cpus,
                                cpus * 10,
                                &context);
  if (! concurrent_migration->Initialize()) {
    LogCvmfs(kLogCatalog, kLogStderr, "Failed to initialize worker migration "
                                      "system.");
    return 3;
  }
  concurrent_migration->RegisterListener(&CommandMigrate::MigrationCallback,
                                         this);

  // load the full catalog hierarchy
  LogCvmfs(kLogCatalog, kLogStdout, "Loading current catalog tree...");
  const bool generate_full_catalog_tree = true;
  CatalogTraversal<CommandMigrate> traversal(
    this,
    &CommandMigrate::CatalogCallback,
    repo_url,
    repo_name,
    repo_keys,
    generate_full_catalog_tree);
  const bool loading_successful = traversal.Traverse();

  if (!loading_successful) {
    LogCvmfs(kLogCatalog, kLogStderr, "Failed to load catalog tree");
    return 4;
  }

  assert (root_catalog_ != NULL);

  // migrate catalogs recursively (starting with the deepest nested catalogs)
  LogCvmfs(kLogCatalog, kLogStdout, "\nMigrating catalogs...");
  FutureNestedCatalogReference *new_root_catalog =
                                             new FutureNestedCatalogReference;
  spooler_->DisablePrecaching();
  ConvertCatalogsRecursively(root_catalog_, new_root_catalog);
  concurrent_migration->WaitForEmptyQueue();
  spooler_->WaitForUpload();

  const unsigned int errors = concurrent_migration->GetNumberOfFailedJobs() +
                              spooler_->GetNumberOfErrors();

  LogCvmfs(kLogCatalog, kLogStdout, "\nMigration finished with %d errors.",
           errors);

  if (errors > 0) {
    return 5;
  }

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
}


void CommandMigrate::MigrationCallback(
                                  const CommandMigrate::PendingCatalog &data) {
  // check if the migration of the catalog was successful
  if (!data.success) {
    return;
  }

  const std::string &path = data.new_catalog->database_path();

  // save the processed catalog in the pending map
  {
    LockGuard<PendingCatalogMap> guard(pending_catalogs_);
    assert (pending_catalogs_.find(path) == pending_catalogs_.end());
    pending_catalogs_[path] = data;
  }

  // schedule the compression and upload of the catalog
  const bool chunking = false;
  spooler_->Process(path, chunking);
}


void CommandMigrate::UploadCallback(const upload::SpoolerResult &result) {
  const std::string &path = result.local_path;

  // remove the just uploaded file
  unlink(path.c_str());

  // check if the upload was successful
  if (result.return_code != 0) {
    LogCvmfs(kLogCatalog, kLogStderr, "FAILED upload of %s", path.c_str());
    return;
  }
  assert (result.file_chunks.size() == 0);

  // find the catalog path in the pending catalogs and remove it from the list
  PendingCatalog catalog;
  {
    LockGuard<PendingCatalogMap> guard(pending_catalogs_);
    PendingCatalogMap::iterator i = pending_catalogs_.find(path);
    assert (i != pending_catalogs_.end());
    catalog = i->second;
    pending_catalogs_.erase(i);
  }

  // the catalog is completely processed... fill the future to allow the
  // processing of parent catalogs
  NestedCatalogReference nested_catalog;
  nested_catalog.path                 = catalog.new_catalog->path();
  nested_catalog.hash                 = result.content_hash;
  nested_catalog.mountpoint_linkcount = catalog.mountpoint_linkcount;
  nested_catalog.nested_statistics    = catalog.nested_statistics;
  catalog.new_nested_ref->Set(nested_catalog);

  // TODO: do some more cleanup
  LogCvmfs(kLogCatalog, kLogStdout, "migrated and uploaded %sC %s",
           result.content_hash.ToString().c_str(),
           catalog.new_catalog->path().ToString().c_str());
}


void CommandMigrate::ConvertCatalog(
              const Catalog                           *catalog,
                    FutureNestedCatalogReference      *new_nested_ref,
              const FutureNestedCatalogReferenceList  &future_nested_catalogs) {
  MigrationWorker::expected_data data(catalog,
                                      new_nested_ref,
                                      future_nested_catalogs);
  concurrent_migration->Schedule(data);
}


void CommandMigrate::ConvertCatalogsRecursively(
                           const Catalog                       *catalog,
                                 FutureNestedCatalogReference  *new_nested_ref) {
  // first migrate all nested catalogs (depth first traversal)
  const CatalogList nested_catalogs = catalog->GetChildren();
  FutureNestedCatalogReferenceList future_nested_catalogs;
  future_nested_catalogs.reserve(nested_catalogs.size());
  CatalogList::const_iterator i    = nested_catalogs.begin();
  CatalogList::const_iterator iend = nested_catalogs.end();
  for (; i != iend; ++i) {
    FutureNestedCatalogReference *new_nested_ref = // TODO: this might produce a memory leak (??)
                                               new FutureNestedCatalogReference;
    future_nested_catalogs.push_back(new_nested_ref);
    ConvertCatalogsRecursively(*i, new_nested_ref);
  }

  // migrate this catalog referencing all it's (already migrated) children
  ConvertCatalog(catalog, new_nested_ref, future_nested_catalogs);
}


CommandMigrate::MigrationWorker::MigrationWorker(const worker_context *context) :
  temporary_directory_(context->temporary_directory)
{}


CommandMigrate::MigrationWorker::~MigrationWorker() {

}


void CommandMigrate::MigrationWorker::operator()(const expected_data &data) {
  // unbox data structure for convenience
  const Catalog                           *catalog        = data.catalog;
        FutureNestedCatalogReference      *new_nested_ref = data.new_nested_ref;
  const FutureNestedCatalogReferenceList  &future_nested_catalogs =
                                                    data.future_nested_catalogs;
  bool                                     retval               = false;
  unsigned int                             mountpoint_linkcount = 0;
  catalog::DeltaCounters                   nested_statistics;

  // create and attach an empty catalog with the newest schema
  WritableCatalog *writable_catalog = CreateNewEmptyCatalog(
                                                    catalog->path().ToString());
  if (writable_catalog == NULL) {
    master()->JobFailed(PendingCatalog(false));
    return;
  }

  // get the fresh database of the writable catalog and do some checks
  const Database &new_catalog = writable_catalog->database();
  const Database &old_catalog = catalog->database();
  retval = CheckDatabaseSchemaCompatibility(new_catalog, old_catalog);
  if (! retval) goto fail;

  // attach the new catalog database into the old one to perform the migration
  // after attaching, the readable catalog is unlinked since the temporary hard-
  // link is not needed anymore
  retval = AttachOldCatalogDatabase(new_catalog, old_catalog);
  unlink(old_catalog.filename().c_str());
  if (! retval) goto fail;

  // migrate the catalog data (file meta data)
  retval = MigrateFileMetadata(catalog, writable_catalog);
  if (! retval) goto fail;

  // migrate nested catalog references. We need to wait until all nested cata-
  // logs are successfully processed before we can do this.
  retval = MigrateNestedCatalogReferences(writable_catalog,
                                          future_nested_catalogs);
  if (! retval) goto fail;

  // generate statistics for the current catalog
  retval = GenerateCatalogStatistics(writable_catalog,
                                     future_nested_catalogs,
                                     &nested_statistics);
  if (! retval) goto fail;

  // find out about the catalog's mountpoint's linkcount that needs to be passed
  // to the parent, in order to synchronize it's respective directory entry
  retval = FindMountpointLinkcount(writable_catalog, &mountpoint_linkcount);
  if (! retval) goto fail;

  // all went well... migration of this catalog has finished
  master()->JobSuccessful(PendingCatalog(true,
                                         mountpoint_linkcount,
                                         nested_statistics,
                                         writable_catalog,
                                         new_nested_ref));
  return;

fail:
  master()->JobFailed(PendingCatalog(false));
  return;
}


bool CommandMigrate::MigrationWorker::CheckDatabaseSchemaCompatibility(
                                   const catalog::Database &new_catalog,
                                   const catalog::Database &old_catalog) const {
  if (! new_catalog.ready()                                                  ||
      (new_catalog.schema_version() < Database::kLatestSupportedSchema -
                                      Database::kSchemaEpsilon         ||
       new_catalog.schema_version() > Database::kLatestSupportedSchema +
                                      Database::kSchemaEpsilon)              ||
      (old_catalog.schema_version() > 2.1 + Database::kSchemaEpsilon)) {
    LogCvmfs(kLogCatalog, kLogStderr, "Failed to meet database requirements "
                                      "for migration.");
    return false;
  }
  return true;
}

bool CommandMigrate::MigrationWorker::AttachOldCatalogDatabase(
                                   const catalog::Database &new_catalog,
                                   const catalog::Database &old_catalog) const {
  Sql sql_attach_new(new_catalog,
    "ATTACH '" + old_catalog.filename() + "' AS old;"
  );
  bool retval = sql_attach_new.Execute();
  if (! retval) {
    SqlError("Failed to attach database of old catalog", sql_attach_new);
    return false;
  }
  return true;
}

WritableCatalog* CommandMigrate::MigrationWorker::CreateNewEmptyCatalog(
                                           const std::string &root_path) const {
  // create a new catalog database schema
  bool retval;
  const std::string catalog_db = CreateTempPath(temporary_directory_ + "/catalog",
                                                0666);
  retval = Database::Create(catalog_db, root_path);
  if (! retval) {
    LogCvmfs(kLogCatalog, kLogStderr, "Failed to create database for new "
                                      "catalog");
    unlink(catalog_db.c_str());
    return NULL;
  }

  // Attach the just created nested catalog database
  WritableCatalog *writable_catalog = new WritableCatalog(root_path,
                                                          hash::Any(hash::kSha1),
                                                          NULL);
  retval = writable_catalog->OpenDatabase(catalog_db);
  if (! retval) {
    LogCvmfs(kLogCatalog, kLogStderr, "Failed to open database for new catalog");
    delete writable_catalog;
    unlink(catalog_db.c_str());
    return NULL;
  }

  return writable_catalog;
}


bool CommandMigrate::MigrationWorker::MigrateFileMetadata(
                      const catalog::Catalog          *catalog,
                            catalog::WritableCatalog  *writable_catalog) const {
  assert (! writable_catalog->IsDirty());
  bool retval;

  const int uid = 0;
  const int gid = 0; // TODO: make this configurable

  const Database &writable = writable_catalog->database();

  // analyze the hardlink relationships in the old catalog
  //   inodes used to be assigned at publishing time, implicitly constituating
  //   those relationships. We now need them explicitly in the file catalogs
  // This looks for catalog entries with matching inodes but different pathes
  // and saves the results in a temporary table called 'hardlinks'
  //   Hardlinks:
  //     groupid   : this group id can be used for the new catalog schema
  //     inode     : the inodes that were part of a hardlink group before
  //     linkcount : the linkcount for hardlink group id members
  Sql sql_tmp_hardlinks(writable,
    "CREATE TEMPORARY TABLE hardlinks AS "
    "  SELECT rowid AS groupid, inode, COUNT(*) AS linkcount "
    "  FROM ( "
    "    SELECT DISTINCT c1.inode AS inode, c1.md5path_1, c1.md5path_2 "
    "    FROM old.catalog AS c1 "
    "    INNER JOIN old.catalog AS c2 "
    "      ON c1.inode == c2.inode AND "
    "        (c1.md5path_1 != c2.md5path_1 OR "
    "         c1.md5path_2 != c2.md5path_2) "
    "  ) "
    "  GROUP BY inode;"
  );
  retval = sql_tmp_hardlinks.Execute();
  if (! retval) {
    SqlError("Failed to analyze hardlink relationships",
             sql_tmp_hardlinks);
    return false;
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
    "INSERT INTO hardlinks "
    "  SELECT 0, c1.inode as inode, "
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
    SqlError("Failed to analyze directory specific link counts",
             sql_dir_linkcounts);
    return false;
  }

  // copy the old file meta information into the new catalog schema
  //   here we add the previously analyzed hardlink/linkcount information
  //
  // Note: nested catalog mountpoint still need to be treated separately
  Sql migrate_file_meta_data(writable,
    "INSERT INTO catalog "
    "  SELECT md5path_1, md5path_2, "
    "         parent_1, parent_2, "
    "         IFNULL(groupid, 0) << 32 | IFNULL(linkcount, 1) AS hardlinks, "
    "         hash, size, mode, mtime, "
    "         flags, name, symlink, "
    "         :uid, "
    "         :gid "
    "  FROM old.catalog "
    "  LEFT JOIN hardlinks ON catalog.inode = hardlinks.inode;"
  );
  retval = migrate_file_meta_data.BindInt64(1, uid) &&
           migrate_file_meta_data.BindInt64(2, gid) &&
           migrate_file_meta_data.Execute();
  if (! retval) {
    SqlError("Failed to migrate the file system meta data",
             migrate_file_meta_data);
    return false;
  }

  // copy (and update) the properties fields
  //
  // Note: The schema is explicitly not copied to the new catalog.
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
    SqlError("Failed to migrate the properties table.", copy_properties);
    return false;
  }

  writable_catalog->SetDirty();

  // set the previous revision hash in the new catalog to the old catalog
  //   we are doing the whole migration as a new snapshot that does not change
  //   any files, but just bumpes the catalog schema to the latest version
  writable_catalog->SetPreviousRevision(catalog->hash());
  writable_catalog->IncrementRevision();
  writable_catalog->UpdateLastModified();

  writable_catalog->Commit();

  return true;
}


bool CommandMigrate::MigrationWorker::MigrateNestedCatalogReferences(
        const catalog::WritableCatalog          *writable_catalog,
        const FutureNestedCatalogReferenceList  &future_nested_catalogs) const {
  const Database &writable = writable_catalog->database();
  bool retval;

  // preparing a SQL statements for nested catalog addition
  Sql add_nested_catalog(writable,
    "INSERT INTO nested_catalogs (path, sha1) VALUES (:path, :sha1);"
  );
  Sql update_mntpnt_linkcount(writable,
    "UPDATE catalog "
    "SET hardlinks = :linkcount "
    "WHERE md5path_1 = :md5_1 AND md5path_2 = :md5_2;"
  );

  // unbox the nested catalogs (possibly waiting for migration of them first)
  FutureNestedCatalogReferenceList::const_iterator i    =
                                                 future_nested_catalogs.begin();
  FutureNestedCatalogReferenceList::const_iterator iend =
                                                 future_nested_catalogs.end();
  for (; i != iend; ++i) {
    NestedCatalogReference &nested_catalog = (*i)->Get();

    // update the nested catalog mountpoint directory entry with the correct
    // linkcount that was determined while processing the nested catalog
    hash::Md5 mountpoint_hash = hash::Md5(nested_catalog.path.GetChars(),
                                          nested_catalog.path.GetLength());
    retval =
      update_mntpnt_linkcount.BindInt64(1, nested_catalog.mountpoint_linkcount);
      update_mntpnt_linkcount.BindMd5(2, 3, mountpoint_hash);
      update_mntpnt_linkcount.Execute();
    if (! retval) {
      SqlError("Failed to update linkcount of nested catalog mountpoint",
               update_mntpnt_linkcount);
      return false;
    }
    update_mntpnt_linkcount.Reset();

    // insert the updated nested catalog reference into the new catalog
    retval =
      add_nested_catalog.BindText(1, nested_catalog.path.ToString()) &&
      add_nested_catalog.BindText(2, nested_catalog.hash.ToString()) &&
      add_nested_catalog.Execute();
    if (! retval) {
      SqlError("Failed to add nested catalog link", add_nested_catalog);
      return false;
    }
    add_nested_catalog.Reset();
  }

  return true;
}


bool CommandMigrate::MigrationWorker::GenerateCatalogStatistics(
        catalog::WritableCatalog                *writable_catalog,
        const FutureNestedCatalogReferenceList  &future_nested_catalogs,
        catalog::DeltaCounters                  *nested_statistics) const {
  bool retval = false;

  // Aggregated the statistics counters of all nested catalogs
  // Note: we might need to wait until nested catalogs are sucessfully processed
  DeltaCounters aggregated_counters;
  FutureNestedCatalogReferenceList::const_iterator i    =
                                                 future_nested_catalogs.begin();
  FutureNestedCatalogReferenceList::const_iterator iend =
                                                 future_nested_catalogs.end();
  for (; i != iend; ++i) {
    const NestedCatalogReference &nested_catalog = (*i)->Get();
    nested_catalog.nested_statistics.PopulateToParent(&aggregated_counters);
  }

  // generate statistics for the current catalog and store the aggregated stats
  // for the child catalog tree
  const Database &writable = writable_catalog->database();
  Sql generate_stats(writable,
    "INSERT OR REPLACE INTO statistics "
    "SELECT 'self_regular', count(*) FROM old.catalog "
    "                                WHERE     flags & :flag_file  "
    "                                  AND NOT flags & :flag_link_1 "
    "  UNION "
    "SELECT 'self_symlink', count(*) FROM old.catalog "
    "                                WHERE flags & :flag_link_2 "
    "  UNION "
    "SELECT 'self_dir',     count(*) FROM old.catalog "
    "                                WHERE flags & :flag_dir "
    "  UNION "
    "SELECT 'self_nested',  count(*) FROM old.nested_catalogs "
    "  UNION "
    "SELECT 'subtree_regular', :subtree_regular "
    "  UNION "
    "SELECT 'subtree_symlink', :subtree_symlink "
    "  UNION "
    "SELECT 'subtree_dir',     :subtree_dir "
    "  UNION "
    "SELECT 'subtree_nested',  :subtree_nested;");
  retval =
    generate_stats.BindInt64(1, SqlDirent::kFlagFile)                  &&
    generate_stats.BindInt64(2, SqlDirent::kFlagLink)                  &&
    generate_stats.BindInt64(3, SqlDirent::kFlagLink)                  &&
    generate_stats.BindInt64(4, SqlDirent::kFlagDir)                   &&
    generate_stats.BindInt64(5, aggregated_counters.d_subtree_regular) &&
    generate_stats.BindInt64(6, aggregated_counters.d_subtree_symlink) &&
    generate_stats.BindInt64(7, aggregated_counters.d_subtree_dir)     &&
    generate_stats.BindInt64(8, aggregated_counters.d_subtree_nested)  &&
    generate_stats.Execute();
  if (! retval) {
    SqlError("Failed to generate catalog statistics.", generate_stats);
    return false;
  }

  // read out the generated information in order to pass it to parent catalog
  catalog::Counters statistics;
  retval = statistics.ReadCounters(writable);
  if (! retval) {
    LogCvmfs(kLogCatalog, kLogStderr, "Failed to read out generated statistics "
                                      "counters");
    return false;
  }
  nested_statistics->InitWithCounters(statistics);

  return true;
}


bool CommandMigrate::MigrationWorker::FindMountpointLinkcount(
                  const catalog::WritableCatalog  *writable_catalog,
                  unsigned int                    *mountpoint_linkcount) const {
  const Database &writable = writable_catalog->database();
  bool retval;

  PathString root_path = writable_catalog->path();
  hash::Md5 root_path_hash = hash::Md5(root_path.GetChars(),
                                       root_path.GetLength());

  Sql find_mountpoint_linkcount(writable,
    "SELECT hardlinks "
    "FROM catalog "
    "WHERE md5path_1 = :md5_1 AND "
    "      md5path_2 = :md5_2 "
    "LIMIT 1;"
  );
  retval = find_mountpoint_linkcount.BindMd5(1, 2, root_path_hash) &&
           find_mountpoint_linkcount.Execute();
  if (! retval) {
    SqlError("Failed to retrieve linkcount of catalog root entry",
             find_mountpoint_linkcount);
    return false;
  }

  int linkcount = find_mountpoint_linkcount.RetrieveInt64(0);
  if (linkcount < 2 || linkcount >= (1l << 32)) {
    LogCvmfs(kLogCatalog, kLogStderr, "Retrieved linkcount of catalog root "
                                      "entry is not sane. (found: %d)",
             linkcount);
    return false;
  }

  *mountpoint_linkcount = linkcount;
  return true;
}


void CommandMigrate::MigrationWorker::SqlError(
                                          const std::string  &message,
                                          const catalog::Sql &statement) const {
  LogCvmfs(kLogCatalog, kLogStderr, "%s\nSQLite: %d - %s",
           message.c_str(),
           statement.GetLastError(),
           statement.GetLastErrorMsg().c_str());
}

