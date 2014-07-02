/**
 * This file is part of the CernVM File System.
 *
 * This command processes a repository's catalog structure to detect and remove
 * outdated and/or unneeded data objects.
 */

#include "swissknife_gc.h"


#include <string>

using namespace swissknife;  // NOLINT


ParameterList CommandGC::GetParams() {
  ParameterList r;
  r.push_back(Parameter::Mandatory('r', "repository directory / url"));
  r.push_back(Parameter::Mandatory('h', "conserve <h> revisions"));
  r.push_back(Parameter::Optional ('n', "fully qualified repository name"));
  r.push_back(Parameter::Optional ('k', "repository master key(s)"));
  r.push_back(Parameter::Switch   ('d', "dry run"));
  r.push_back(Parameter::Switch   ('l', "list objects to be removed"));
  // to be extended...
  return r;
}


int CommandGC::Main(const ArgumentList &args) {
  const std::string &repo_url               = *args.find('r')->second;
  const int64_t      revisions              = String2Int64(*args.find('h')->second);
  const std::string &repo_name              = (args.count('n') > 0) ? *args.find('n')->second : "";
  const std::string &repo_keys              = (args.count('k') > 0) ? *args.find('k')->second : "";
  const bool         dry_run                = (args.count('d') > 0);
  const bool         list_condemned_objects = (args.count('l') > 0);

  if (revisions <= 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "at least one revision needs to be preserved");
    return 1;
  }

  CatalogTraversalParams params;
  params.repo_url          = repo_url;
  params.repo_name         = repo_name;
  params.repo_keys         = repo_keys;
  params.history           = revisions;
  params.no_repeat_history = true;
  params.no_close          = false;

  ReadonlyCatalogTraversal traversal(params);
  traversal.RegisterListener(&CommandGC::CatalogCallback, this);

  const bool result = traversal.Traverse();
  return (result) ? 0 : 1;
}


void CommandGC::CatalogCallback(const ReadonlyCatalogTraversal::CallbackData &data) {

}
