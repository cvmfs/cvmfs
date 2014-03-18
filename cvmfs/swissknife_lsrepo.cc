/**
 * This file is part of the CernVM File System.
 */

#include "swissknife_lsrepo.h"

#include "logging.h"

using namespace swissknife;

CommandListCatalogs::CommandListCatalogs() :
  print_tree_(false),
  print_hash_(false) {}


ParameterList CommandListCatalogs::GetParams() {
  ParameterList result;
  result.push_back(Parameter('r', "repository URL (absolute local path or remote URL)",
                             false, false));
  result.push_back(Parameter('n', "fully qualified repository name",
                             true, false));
  result.push_back(Parameter('k', "repository master key(s)",
                             true, false));
  result.push_back(Parameter('t', "print tree structure of catalogs",
                             true, true));
  result.push_back(Parameter('d', "print digest for each catalog",
                             true, true));
  return result;
}


int CommandListCatalogs::Main(const ArgumentList &args) {
  print_tree_ = (args.count('t') > 0);
  print_hash_ = (args.count('d') > 0);

  const std::string &repo_url = *args.find('r')->second;
  const std::string &repo_name = (args.count('n') > 0) ? *args.find('n')->second : "";
  const std::string &repo_keys = (args.count('k') > 0) ? *args.find('k')->second : "";

  SimpleCatalogTraversal traversal(repo_url, repo_name, repo_keys);
  traversal.RegisterListener(&CommandListCatalogs::CatalogCallback, this);

  return traversal.Traverse() ? 0 : 1;
}


void CommandListCatalogs::CatalogCallback(const CatalogTraversalData &data) {
  std::string tree_indent;
  std::string hash_string;
  std::string path;

  if (print_tree_) {
    for (unsigned int i = 1; i < data.tree_level; ++i) {
      tree_indent += "\u2502  ";
    }

    if (data.tree_level > 0)
      tree_indent += "\u251C\u2500 ";
  }

  if (print_hash_) {
    hash_string = data.catalog_hash.ToString() + " ";
  }

  path = data.catalog->path().ToString();
  if (path.empty())
    path = "/";

  LogCvmfs(kLogCatalog, kLogStdout, "%s%s%s",
    tree_indent.c_str(), hash_string.c_str(), path.c_str());
}
