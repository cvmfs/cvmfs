/**
 * This file is part of the CernVM File System.
 *
 * This command processes a repository's catalog structure to detect and remove
 * outdated and/or unneeded data objects.
 */

#include "swissknife_gc.h"

#include "garbage_collection/garbage_collector.h"
#include "garbage_collection/hash_filter.h"
#include "upload_facility.h"

#include <string>

using namespace swissknife;  // NOLINT
using namespace upload;


typedef GarbageCollector<ReadonlyCatalogTraversal, SimpleHashFilter> GC;
typedef GC::Configuration                                            GcConfig;


ParameterList CommandGC::GetParams() {
  ParameterList r;
  r.push_back(Parameter::Mandatory('r', "repository directory / url"));
  r.push_back(Parameter::Mandatory('u', "spooler definition string"));
  r.push_back(Parameter::Mandatory('h', "conserve <h> revisions"));
  r.push_back(Parameter::Mandatory('n', "fully qualified repository name"));
  r.push_back(Parameter::Optional ('k', "repository master key(s)"));
  r.push_back(Parameter::Switch   ('d', "dry run"));
  r.push_back(Parameter::Switch   ('l', "list objects to be removed"));
  // to be extended...
  return r;
}


int CommandGC::Main(const ArgumentList &args) {
  const std::string &repo_url               = *args.find('r')->second;
  const std::string &spooler                = *args.find('u')->second;
  const int64_t      revisions              = String2Int64(*args.find('h')->second);
  const std::string &repo_name              = (args.count('n') > 0) ? *args.find('n')->second : "";
  const std::string &repo_keys              = (args.count('k') > 0) ? *args.find('k')->second : "";
  const bool         dry_run                = (args.count('d') > 0);
  const bool         list_condemned_objects = (args.count('l') > 0);

  if (revisions <= 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "at least one revision needs to be preserved");
    return 1;
  }

  GcConfig config;
  const upload::SpoolerDefinition spooler_definition(spooler, shash::kAny);
  config.uploader             = AbstractUploader::Construct(spooler_definition);
  config.keep_history_depth   = revisions;
  config.keep_named_snapshots = true;
  config.dry_run              = dry_run;
  config.verbose              = list_condemned_objects;
  config.repo_url             = repo_url;
  config.repo_name            = repo_name;
  config.repo_keys            = repo_keys;
  config.tmp_dir              = "/tmp";

  if (config.uploader == NULL) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to initialize spooler for '%s'",
             spooler.c_str());
    return 1;
  }

  GC collector(config);
  return collector.Collect() ? 0 : 1;
}
