/**
 * This file is part of the CernVM File System.
 */

#include "swissknife_migrate.h"

#include "catalog_traversal.h"
#include "catalog_sql.h"
#include "catalog_rw.h"

#include "logging.h"

#include <sys/resource.h>

using namespace swissknife;
using namespace catalog;

CommandMigrate::CommandMigrate() :
  file_descriptor_limit_(8192),
  root_catalog_(NULL) {}


ParameterList CommandMigrate::GetParams() {
  ParameterList result;
  result.push_back(Parameter('r', "repository URL (absolute local path "
                                  "or remote URL)",
                             false, false));
  result.push_back(Parameter('u', "upstream definition string",
                             false, false));
  result.push_back(Parameter('o', "manifest output file",
                             false, false));
  result.push_back(Parameter('n', "fully qualified repository name",
                             true, false));
  result.push_back(Parameter('k', "repository master key(s)",
                             true, false));
  result.push_back(Parameter('t', "fix nested catalog transition points",
                             true, true));
  result.push_back(Parameter('s', "enable collection of catalog statistics",
                             true, true));
  return result;
}


int CommandMigrate::Main(const ArgumentList &args) {
  // parameter parsing
  const std::string &repo_url           = *args.find('r')->second;
  const std::string &spooler            = *args.find('u')->second;
  const std::string &manifest_path      = *args.find('o')->second;
  const std::string &repo_name          = (args.count('n') > 0)      ?
                                             *args.find('n')->second :
                                             "";
  const std::string &repo_keys          = (args.count('k') > 0)      ?
                                             *args.find('k')->second :
                                             "";
  const bool fix_transition_points      = (args.count('t') > 0);
  const bool collect_catalog_statistics = (args.count('s') > 0);

  // we might need a lot of file descriptors
  if (! RaiseFileDescriptorLimit()) {
    LogCvmfs(kLogCatalog, kLogStderr, "Failed to raise file descriptor limits");
    return 2;
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
    generate_full_catalog_tree);
  const bool loading_successful = traversal.Traverse();

  if (!loading_successful) {
    LogCvmfs(kLogCatalog, kLogStderr, "Failed to load catalog tree");
    return 5;
  }

  assert (root_catalog_ != NULL);

  // create an upstream spooler
  const upload::SpoolerDefinition spooler_definition(spooler);
  spooler_ = upload::Spooler::Construct(spooler_definition);
  if (!spooler_) {
    LogCvmfs(kLogCatalog, kLogStderr, "Failed to create upstream Spooler.");
    return 3;
  }
  spooler_->RegisterListener(&CommandMigrate::UploadCallback, this);

  // create a concurrent catalog migration facility
  const unsigned int cpus = GetNumberOfCpuCores();
  MigrationWorker::worker_context context(spooler_definition.temporary_path,
                                          fix_transition_points,
                                          collect_catalog_statistics);
  concurrent_migration_ = new ConcurrentWorkers<MigrationWorker>(
                                cpus,
                                cpus * 10,
                                &context);
  if (! concurrent_migration_->Initialize()) {
    LogCvmfs(kLogCatalog, kLogStderr, "Failed to initialize worker migration "
                                      "system.");
    return 4;
  }
  concurrent_migration_->RegisterListener(&CommandMigrate::MigrationCallback,
                                          this);

  // migrate catalogs recursively (starting with the deepest nested catalogs)
  LogCvmfs(kLogCatalog, kLogStdout, "\nMigrating catalogs...");
  PendingCatalog *root_catalog = new PendingCatalog(root_catalog_);
  ConvertCatalogsRecursively(root_catalog);
  concurrent_migration_->WaitForEmptyQueue();
  spooler_->WaitForUpload();

  const unsigned int errors = concurrent_migration_->GetNumberOfFailedJobs() +
                              spooler_->GetNumberOfErrors();

  LogCvmfs(kLogCatalog, kLogStdout, "Catalog Migration finished with %d "
                                    "errors.",
           errors);
  if (errors > 0) {
    LogCvmfs(kLogCatalog, kLogStdout, "\nCatalog Migration produced errors\n"
                                      "Aborting...");
    return 6;
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
    LogCvmfs(kLogCatalog, kLogStderr, "Manifest export failed.\nAborting...");
    return 7;
  }

  // get rid of the open root catalog
  delete root_catalog;

  // all done...
  LogCvmfs(kLogCatalog, kLogStdout, "Catalog Migration succeeded");
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


void CommandMigrate::MigrationCallback(PendingCatalog *const &data) {
  // check if the migration of the catalog was successful
  if (!data->success) {
    LogCvmfs(kLogCatalog, kLogStderr, "Catalog migration failed! Aborting...");
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

  // schedule the compression and upload of the catalog
  spooler_->ProcessCatalog(path);
}


void CommandMigrate::UploadCallback(const upload::SpoolerResult &result) {
  const std::string &path = result.local_path;

  // remove the just uploaded file
  unlink(path.c_str());

  // check if the upload was successful
  if (result.return_code != 0) {
    LogCvmfs(kLogCatalog, kLogStderr, "Failed to upload catalog %s\nAborting...",
             path.c_str());
    exit(2);
    return;
  }
  assert (result.file_chunks.size() == 0);

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

  // TODO: do some more cleanup
  LogCvmfs(kLogCatalog, kLogStdout, "migrated and uploaded %sC %s",
           result.content_hash.ToString().c_str(),
           catalog->root_path().c_str());
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


CommandMigrate::PendingCatalog::~PendingCatalog() {
  delete old_catalog;
  old_catalog = NULL;

  if (new_catalog != NULL) {
    delete new_catalog;
    new_catalog = NULL;
  }
}


CommandMigrate::MigrationWorker::MigrationWorker(const worker_context *context) :
  temporary_directory_(context->temporary_directory),
  fix_nested_catalog_transitions_(context->fix_nested_catalog_transitions),
  collect_catalog_statistics_(context->collect_catalog_statistics)
{}


CommandMigrate::MigrationWorker::~MigrationWorker() {}


void CommandMigrate::MigrationWorker::operator()(const expected_data &data) {
  const bool success =
    CreateNewEmptyCatalog            (data) &&
    CheckDatabaseSchemaCompatibility (data) &&
    AttachOldCatalogDatabase         (data) &&
    MigrateFileMetadata              (data) &&
    MigrateNestedCatalogReferences   (data) &&
    FixNestedCatalogTransitionPoints (data) &&
    GenerateCatalogStatistics        (data) &&
    FindRootEntryInformation         (data) &&
    CollectAndAggregateStatistics    (data) &&
    DetachOldCatalogDatabase         (data) &&
    CleanupNestedCatalogs            (data);
  data->success = success;

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
    LogCvmfs(kLogCatalog, kLogStderr, "Failed to create temporary file for the "
                                      "new catalog database.");
    return false;
  }
  retval = Database::Create(catalog_db, root_path);
  if (! retval) {
    LogCvmfs(kLogCatalog, kLogStderr, "Failed to create database for new "
                                      "catalog");
    unlink(catalog_db.c_str());
    return false;
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
    LogCvmfs(kLogCatalog, kLogStderr, "Failed to meet database requirements "
                                      "for migration.");
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
    SqlError("Failed to attach database of old catalog", sql_attach_new);
    return false;
  }
  return true;
}


bool CommandMigrate::MigrationWorker::MigrateFileMetadata(
                                                   PendingCatalog *data) const {
  assert (! data->new_catalog->IsDirty());
  bool retval;

  const int uid = 0;
  const int gid = 0; // TODO: make this configurable

  const Database &writable = data->new_catalog->database();

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

  data->new_catalog->SetDirty();

  // set the previous revision hash in the new catalog to the old catalog
  //   we are doing the whole migration as a new snapshot that does not change
  //   any files, but just bumpes the catalog schema to the latest version
  data->new_catalog->SetPreviousRevision(data->old_catalog->hash());
  data->new_catalog->IncrementRevision();
  data->new_catalog->UpdateLastModified();

  data->new_catalog->Commit();

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
      SqlError("Failed to update linkcount of nested catalog mountpoint",
               update_mntpnt_linkcount);
      return false;
    }
    update_mntpnt_linkcount.Reset();

    // insert the updated nested catalog reference into the new catalog
    retval =
      add_nested_catalog.BindText(1, root_path) &&
      add_nested_catalog.BindText(2, catalog_hash.ToString()) &&
      add_nested_catalog.Execute();
    if (! retval) {
      SqlError("Failed to add nested catalog link", add_nested_catalog);
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
      SqlError("Failed to fetch nested catalog mountpoint to check for "
               "compatible transition points", lookup_mountpoint);
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
        LogCvmfs(kLogCatalog, kLogStderr, "Found an irreparable mismatch in a "
                                          "nested catalog transtion point at "
                                          "'%s'  Aborting...",
                 nested_root_path.c_str());
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
        SqlError("Failed to save resynchronized nested catalog mountpoint into "
                 "catalog database", update_directory_entry);
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


bool CommandMigrate::MigrationWorker::GenerateCatalogStatistics(
                                                   PendingCatalog *data) const {
  bool retval = false;

  // Aggregated the statistics counters of all nested catalogs
  // Note: we might need to wait until nested catalogs are sucessfully processed
  DeltaCounters aggregated_counters;
  PendingCatalogList::const_iterator i    = data->nested_catalogs.begin();
  PendingCatalogList::const_iterator iend = data->nested_catalogs.end();
  for (; i != iend; ++i) {
    const PendingCatalog *nested_catalog = *i;
    const catalog::DeltaCounters &s = nested_catalog->nested_statistics.Get();
    s.PopulateToParent(&aggregated_counters);
  }

  // generate statistics for the current catalog and store the aggregated stats
  // for the child catalog tree
  const Database &writable = data->new_catalog->database();
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
  catalog::Counters catalog_statistics;
  retval = catalog_statistics.ReadCounters(writable);
  if (! retval) {
    LogCvmfs(kLogCatalog, kLogStderr, "Failed to read out generated statistics "
                                      "counters");
    return false;
  }
  catalog::DeltaCounters statistics;
  statistics.InitWithCounters(catalog_statistics);
  data->nested_statistics.Set(statistics);

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
    SqlError("Failed to retrieve root directory entry of migrated catalog",
             lookup_root_entry);
    return false;
  }

  DirectoryEntry entry = lookup_root_entry.GetDirent(data->new_catalog);
  if (entry.linkcount() < 2 || entry.linkcount() >= (1l << 32)) {
    LogCvmfs(kLogCatalog, kLogStderr, "Retrieved linkcount of catalog root "
                                      "entry is not sane. (found: %d)",
             entry.linkcount());
    return false;
  }

  data->root_entry.Set(entry);
  return true;
}


bool CommandMigrate::MigrationWorker::CollectAndAggregateStatistics(
                                                   PendingCatalog *data) const {
  if (! collect_catalog_statistics_) {
    return true;
  }

  return true;
}


bool CommandMigrate::MigrationWorker::DetachOldCatalogDatabase(
                                                   PendingCatalog *data) const {
  const Database &writable = data->new_catalog->database();
  Sql detach_old_catalog(writable, "DETACH old;");
  const bool retval = detach_old_catalog.Execute();
  if (! retval) {
    SqlError("Failed to detach old catalog database.", detach_old_catalog);
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

void CommandMigrate::MigrationWorker::SqlError(
                                          const std::string  &message,
                                          const catalog::Sql &statement) const {
  LogCvmfs(kLogCatalog, kLogStderr, "%s\nSQLite: %d - %s",
           message.c_str(),
           statement.GetLastError(),
           statement.GetLastErrorMsg().c_str());
}

