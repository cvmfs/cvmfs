/**
 * This file is part of the CernVM File System.
 */

#include "swissknife_lsrepo.h"

#include "logging.h"

using namespace swissknife;


ParameterList CommandListCatalogs::GetParameters() {
  ParameterList r;
  r.push_back(Parameter::Mandatory('r', "repository URL (absolute local path or remote URL)"));
  r.push_back(Parameter::Optional ('n', "fully qualified repository name"));
  r.push_back(Parameter::Optional ('k', "repository master key(s)"));
  r.push_back(Parameter::Switch   ('t', "print tree structure of catalogs"));
  r.push_back(Parameter::Switch   ('d', "print digest for each catalog"));
  return r;
}


int CommandListCatalogs::Run(const ArgumentList &args) {
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
