/**
 * This file is part of the CernVM File System.
 */

#include "swissknife_migrate.h"

#include "catalog_traversal.h"
#include "logging.h"

using namespace swissknife;
using namespace catalog;

CommandMigrate::CommandMigrate() :
  print_tree_(false),
  print_hash_(false),
  root_catalog_(NULL) {}


ParameterList CommandMigrate::GetParams() {
  ParameterList result;
  result.push_back(Parameter('r', "repository URL (absolute local path or remote URL)",
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
  const std::string &repo_url = *args.find('r')->second;
  const std::string &repo_name = (args.count('n') > 0) ? *args.find('n')->second : "";
  const std::string &repo_keys = (args.count('k') > 0) ? *args.find('k')->second : "";

  // create a concurrent catalog migration facility
  const unsigned int cpus = GetNumberOfCpuCores();
  MigrationWorker::worker_context context;
  concurrent_migration = new ConcurrentWorkers<MigrationWorker>(
                                cpus,
                                cpus * 10,
                                &context);
  if (! concurrent_migration->Initialize()) {
    LogCvmfs(kLogCatalog, kLogStderr, "Failed to initialize worker migration "
                                      "system.");
    return 2;
  }
  concurrent_migration->RegisterListener(&CommandMigrate::MigrationCallback,
                                         this);

  // load the full catalog hierarchy
  LogCvmfs(kLogCatalog, kLogStdout, "Loading current catalog tree");
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
    return 3;
  }

  assert (root_catalog_ != NULL);

  // migrate catalogs recursively (starting with the deepest nested catalogs)
  LogCvmfs(kLogCatalog, kLogStdout, "\nConverting catalogs");
  Future<Catalog::NestedCatalog> *new_root_catalog =
                                             new Future<Catalog::NestedCatalog>;
  ConvertCatalogsRecursively(root_catalog_, new_root_catalog);

  concurrent_migration->WaitForEmptyQueue();

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


void CommandMigrate::MigrationCallback(const MigrationWorker::returned_data &data) {
  LogCvmfs(kLogCatalog, kLogStdout, "got callback");
}


void CommandMigrate::ConvertCatalog(
                const Catalog                         *catalog,
                      Future<Catalog::NestedCatalog>  *new_catalog,
                const FutureNestedCatalogList         &future_nested_catalogs) {
  MigrationWorker::expected_data data(catalog,
                                      new_catalog,
                                      future_nested_catalogs);
  concurrent_migration->Schedule(data);
}


void CommandMigrate::ConvertCatalogsRecursively(
                           const Catalog                         *catalog,
                                 Future<Catalog::NestedCatalog>  *new_catalog) {
  // first migrate all nested catalogs (depth first traversal)
  const CatalogList nested_catalogs = catalog->GetChildren();
  FutureNestedCatalogList future_nested_catalogs;
  future_nested_catalogs.reserve(nested_catalogs.size());
  CatalogList::const_iterator i    = nested_catalogs.begin();
  CatalogList::const_iterator iend = nested_catalogs.end();
  for (; i != iend; ++i) {
    Future<Catalog::NestedCatalog> *new_catalog =
                                         new Future<Catalog::NestedCatalog>;
    future_nested_catalogs.push_back(new_catalog);
    ConvertCatalogsRecursively(*i, new_catalog);
  }

  // migrate this catalog referencing all it's (already migrated) children
  ConvertCatalog(catalog, new_catalog, future_nested_catalogs);
}


CommandMigrate::MigrationWorker::MigrationWorker(const worker_context *context) {

}


CommandMigrate::MigrationWorker::~MigrationWorker() {

}


void CommandMigrate::MigrationWorker::operator()(const expected_data &data) {
  // unbox data structure for convenience
  const Catalog                        *catalog     = data.catalog;
        Future<Catalog::NestedCatalog> *new_catalog = data.new_catalog;
  const FutureNestedCatalogList        &future_nested_catalogs =
                                                    data.future_nested_catalogs;

  // unbox the nested catalogs (possibly waiting for migration of them first)
  Catalog::NestedCatalogList nested_catalogs;
  nested_catalogs.reserve(future_nested_catalogs.size());
  FutureNestedCatalogList::const_iterator i    = future_nested_catalogs.begin();
  FutureNestedCatalogList::const_iterator iend = future_nested_catalogs.end();
  for (; i != iend; ++i) {
    nested_catalogs.push_back((*i)->Get());
  }

  // migrate the catalog
  Catalog::NestedCatalog migrated_catalog;
  migrated_catalog.path = catalog->path();
  migrated_catalog.hash = hash::Any(hash::kSha1);
  new_catalog->Set(migrated_catalog);

  master()->JobSuccessful(returned_data());
}
