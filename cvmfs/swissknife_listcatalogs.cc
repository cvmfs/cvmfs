/**
 * This file is part of the CernVM File System.
 */

#include "swissknife_listcatalogs.h"

#include "cache.h"
#include "catalog_mgr.h"

#include "catalog_traversal.h"

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

  traversal.Traverse();

  // const bool ignore_signature = true;
  // cache::CatalogManager* catalog_manager =
  //     new cache::CatalogManager(repository, ignore_signature);

  // if (!catalog_manager)
  // {
  //   LogCvmfs(kLogCvmfs, kLogStderr, "failed to initialize catalog manager");
  //   return 1;
  // }

  return 0;
}


void CommandListCatalogs::CatalogCallback(const catalog::Catalog* catalog,
                                          const unsigned          recursion_depth) {
  LogCvmfs(kLogCvmfs, kLogStdout, "Catalog: %s Depth: %d",
    catalog->path().c_str(), recursion_depth);
}
