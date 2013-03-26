/**
 * This file is part of the CernVM File System.
 */

#include "swissknife_migrate.h"

#include "catalog_traversal.h"
#include "logging.h"

using namespace swissknife;

CommandMigrate::CommandMigrate() :
  print_tree_(false),
  print_hash_(false) {}


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
  const std::string &repo_url = *args.find('r')->second;
  const std::string &repo_name = (args.count('n') > 0) ? *args.find('n')->second : "";
  const std::string &repo_keys = (args.count('k') > 0) ? *args.find('k')->second : "";

  const bool generate_full_catalog_tree = true;
  CatalogTraversal<CommandMigrate> traversal(
    this,
    &CommandMigrate::CatalogCallback,
    repo_url,
    repo_name,
    repo_keys,
    generate_full_catalog_tree);

  return traversal.Traverse() ? 0 : 1;
}


void CommandMigrate::CatalogCallback(const catalog::Catalog* catalog,
                                     const hash::Any&        catalog_hash,
                                     const unsigned          tree_level) {
  std::string tree_indent;
  std::string hash_string;
  std::string path;

  for (unsigned int i = 1; i < tree_level; ++i) {
    tree_indent += "\u2502  ";
  }

  if (tree_level > 0)
    tree_indent += "\u251C\u2500 ";

  hash_string = catalog_hash.ToString() + " ";

  path = catalog->path().ToString();
  if (path.empty())
    path = "/";

  LogCvmfs(kLogCatalog, kLogStdout, "%s%s%s %d",
    tree_indent.c_str(), hash_string.c_str(), path.c_str(), catalog->ListNestedCatalogs()->size());
}
