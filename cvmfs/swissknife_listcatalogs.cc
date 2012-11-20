/**
 * This file is part of the CernVM File System.
 */

#include "swissknife_listcatalogs.h"

#include "catalog_traversal.h"
#include "logging.h"

using namespace swissknife;

CommandListCatalogs::CommandListCatalogs() :
  print_tree_(false),
  print_hash_(false) {}


ParameterList CommandListCatalogs::GetParams() {
  ParameterList result;
  result.push_back(Parameter('r', "repository name",
                             false, false));
  result.push_back(Parameter('t', "print tree structure of catalogs",
                             true, true));
  result.push_back(Parameter('h', "print hash for each catalog",
                             true, true));
  return result;
}


int CommandListCatalogs::Main(const ArgumentList &args) {
  print_tree_ = (args.count('t') > 0);
  print_hash_ = (args.count('h') > 0);

  assert(args.count('r') > 0);
  const std::string &repository = *args.find('r')->second;

  CatalogTraversal<CommandListCatalogs> traversal(
    this, 
    &CommandListCatalogs::CatalogCallback,
    repository);

  return traversal.Traverse();
}


void CommandListCatalogs::CatalogCallback(const catalog::Catalog* catalog,
                                          const hash::Any&        catalog_hash,
                                          const unsigned          recursion_depth) {
  std::string tree_indent;
  std::string hash_string;
  std::string path;

  if (print_tree_) {
    for (unsigned int i = 1; i < recursion_depth; ++i) {
      tree_indent += "\u2502  ";
    }

    if (recursion_depth > 0)
      tree_indent += "\u251C\u2500 ";
  }

  if (print_hash_) {
    hash_string = catalog_hash.ToString() + " ";
  }

  path = catalog->path().ToString();
  if (path.empty())
    path = "/";
  
  LogCvmfs(kLogCatalog, kLogStdout, "%s%s%s",
    tree_indent.c_str(), hash_string.c_str(), path.c_str());
}
