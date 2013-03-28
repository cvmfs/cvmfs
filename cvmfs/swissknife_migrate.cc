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
  Future<Catalog::NestedCatalog> *new_root_catalog =
                                             new Future<Catalog::NestedCatalog>;
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
  const std::string &path = data.new_catalog->database_path();

  // check if the migration of the catalog was successful
  if (!data.success) {
    LogCvmfs(kLogCatalog, kLogStderr, "FAILED migration of %s", path.c_str());
    return;
  }

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
  Catalog::NestedCatalog nested_catalog;
  nested_catalog.path = PathString(path);
  nested_catalog.hash = result.content_hash;
  catalog.new_nested_ref->Set(nested_catalog);

  // TODO: do some more cleanup
  LogCvmfs(kLogCatalog, kLogStdout, "migrated and uploaded %sC %s",
           result.content_hash.ToString().c_str(),
           catalog.new_catalog->path().ToString().c_str());
}


void CommandMigrate::ConvertCatalog(
                const Catalog                         *catalog,
                      Future<Catalog::NestedCatalog>  *new_nested_ref,
                const FutureNestedCatalogList         &future_nested_catalogs) {
  MigrationWorker::expected_data data(catalog,
                                      new_nested_ref,
                                      future_nested_catalogs);
  concurrent_migration->Schedule(data);
}


void CommandMigrate::ConvertCatalogsRecursively(
                           const Catalog                         *catalog,
                                 Future<Catalog::NestedCatalog>  *new_nested_ref) {
  // first migrate all nested catalogs (depth first traversal)
  const CatalogList nested_catalogs = catalog->GetChildren();
  FutureNestedCatalogList future_nested_catalogs;
  future_nested_catalogs.reserve(nested_catalogs.size());
  CatalogList::const_iterator i    = nested_catalogs.begin();
  CatalogList::const_iterator iend = nested_catalogs.end();
  for (; i != iend; ++i) {
    Future<Catalog::NestedCatalog> *new_nested_ref =
                                         new Future<Catalog::NestedCatalog>;
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
  const Catalog                        *catalog     = data.catalog;
        Future<Catalog::NestedCatalog> *new_nested_ref = data.new_nested_ref;
  const FutureNestedCatalogList        &future_nested_catalogs =
                                                    data.future_nested_catalogs;

  // create and attach an empty catalog with the newest schema
  WritableCatalog *writable_catalog = CreateNewEmptyCatalog(
                                                    catalog->path().ToString());
  if (writable_catalog == NULL) {
    master()->JobFailed(returned_data(false));
    return;
  }

  // migrate the catalog data (file meta data)
  const bool retval = MigrateFileMetadata(catalog, writable_catalog);
  if (!retval) {
    master()->JobFailed(returned_data(false));
    return;
  }

  // unbox the nested catalogs (possibly waiting for migration of them first)
  Catalog::NestedCatalogList nested_catalogs;
  nested_catalogs.reserve(future_nested_catalogs.size());
  FutureNestedCatalogList::const_iterator i    = future_nested_catalogs.begin();
  FutureNestedCatalogList::const_iterator iend = future_nested_catalogs.end();
  for (; i != iend; ++i) {
    nested_catalogs.push_back((*i)->Get());
  }

  master()->JobSuccessful(returned_data(true,
                                        writable_catalog,
                                        new_nested_ref));
}


WritableCatalog* CommandMigrate::MigrationWorker::CreateNewEmptyCatalog(
                                           const std::string &root_path) const {
  // create a new catalog database schema
  bool retval;
  const std::string catalog_db = CreateTempPath(temporary_directory_ + "/catalog",
                                                0666);
  retval = Database::Create(catalog_db, root_path);
  if (!retval) {
    unlink(catalog_db.c_str());
    return NULL;
  }

  // Attach the just created nested catalog database
  WritableCatalog *writable_catalog = new WritableCatalog(root_path, NULL);
  retval = writable_catalog->OpenDatabase(catalog_db);
  if (!retval) {
    delete writable_catalog;
    unlink(catalog_db.c_str());
    return NULL;
  }

  return writable_catalog;
}


bool CommandMigrate::MigrationWorker::MigrateFileMetadata(
                      const catalog::Catalog          *catalog,
                            catalog::WritableCatalog  *writable_catalog) const {
  assert (!writable_catalog->IsDirty());
  bool retval;

  const int uid = 0;
  const int gid = 0; // TODO: make this configurable

  // get the fresh database of the writable catalog and do some checks
  const Database &writable = writable_catalog->database();
  const Database &readable = catalog->database();
  assert (writable.ready());
  assert (writable.schema_version() >= Database::kLatestSupportedSchema -
                                        Database::kSchemaEpsilon &&
          writable.schema_version() <= Database::kLatestSupportedSchema +
                                        Database::kSchemaEpsilon);
  assert (readable.schema_version() < 2.1-Database::kSchemaEpsilon);

  // ATTACH '/tmp/catalog.2qzUCN' as new;
  // CREATE TEMPORARY TABLE hardlinks AS SELECT rowid as groupid, inode, COUNT(*) as linkcount FROM (SELECT DISTINCT c1.inode as inode, c1.md5path_1, c1.md5path_2 FROM catalog as c1, catalog as c2 WHERE c1.inode == c2.inode AND (c1.md5path_1 != c2.md5path_1 OR c1.md5path_2 != c2.md5path_2)) GROUP BY inode;
  // SELECT name, IFNULL(groupid, 0) << 32 | IFNULL(linkcount, 1) as hardlinks FROM catalog LEFT JOIN hardlinks ON catalog.inode = hardlinks.inode ORDER BY linkcount DESC LIMIT 30;

  // Old Schema:
  // CREATE TABLE catalog (
  //    md5path_1 INTEGER,
  //    md5path_2 INTEGER,
  //    parent_1 INTEGER,
  //    parent_2 INTEGER,
  //    inode INTEGER,
  //    hash BLOB,
  //    size INTEGER,
  //    mode INTEGER,
  //    mtime INTEGER,
  //    flags INTEGER,
  //    name TEXT,
  //    symlink TEXT,
  //    CONSTRAINT pk_catalog PRIMARY KEY (md5path_1, md5path_2));


  // New Schema:
  // CREATE TABLE catalog (
  //    md5path_1 INTEGER,
  //    md5path_2 INTEGER,
  //    parent_1 INTEGER,
  //    parent_2 INTEGER,
  //    hardlinks INTEGER,
  //    hash BLOB,
  //    size INTEGER,
  //    mode INTEGER,
  //    mtime INTEGER,
  //    flags INTEGER,
  //    name TEXT,
  //    symlink TEXT,
  //    uid INTEGER,
  //    gid INTEGER,  CONSTRAINT pk_catalog PRIMARY KEY (md5path_1, md5path_2));


  // attach the new catalog database into the old one to perform the migration
  // after attaching, the readable catalog is unlinked since the temporary hard-
  // link is not needed anymore
  Sql sql_attach_new(writable,
    "ATTACH '" + readable.filename() + "' AS old;"
  );
  retval = sql_attach_new.Execute();
  unlink(readable.filename().c_str());
  if (! retval) {
    LogCvmfs(kLogCatalog, kLogStderr, "Attaching database '%s' failed.\n"
                                      "SQLite: %d - %s",
             readable.filename().c_str(),
             sql_attach_new.GetLastError(),
             sql_attach_new.GetLastErrorMsg().c_str());
    return false;
  }


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
    LogCvmfs(kLogCatalog, kLogStderr, "Creating temporary table 'hardlinks' "
                                      "failed:\n SQLite: %d - %s",
             sql_tmp_hardlinks.GetLastError(),
             sql_tmp_hardlinks.GetLastErrorMsg().c_str());
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
    LogCvmfs(kLogCatalog, kLogStderr, "Creating temporary table 'dir_linkcounts' "
                                      "failed:\n SQLite: %d - %s",
             sql_dir_linkcounts.GetLastError(),
             sql_dir_linkcounts.GetLastErrorMsg().c_str());
    return false;
  }

  Sql migrate_step1(writable,
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
  retval = migrate_step1.BindInt64(1, uid) &&
           migrate_step1.BindInt64(2, gid) &&
           migrate_step1.Execute();
  if (! retval) {
    LogCvmfs(kLogCatalog, kLogStderr, "Doing first migration step failed.\n"
                                      "SQLite: %d - %s",
             migrate_step1.GetLastError(),
             migrate_step1.GetLastErrorMsg().c_str());
    return false;
  }

  writable_catalog->SetDirty();
  writable_catalog->Commit();

  return true;
}
