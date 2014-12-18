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

#include "manifest.h"

#include <string>

using namespace swissknife;  // NOLINT
using namespace upload;      // NOLINT

typedef HttpObjectFetcher<>                                          ObjectFetcher;
typedef CatalogTraversal<ObjectFetcher>                              ReadonlyCatalogTraversal;
typedef GarbageCollector<ReadonlyCatalogTraversal, SimpleHashFilter> GC;
typedef GC::Configuration                                            GcConfig;


ParameterList CommandGc::GetParams() {
  ParameterList r;
  r.push_back(Parameter::Mandatory('r', "repository url"));
  r.push_back(Parameter::Mandatory('u', "spooler definition string"));
  r.push_back(Parameter::Mandatory('n', "fully qualified repository name"));
  r.push_back(Parameter::Optional ('h', "conserve <h> revisions"));
  r.push_back(Parameter::Optional ('z', "conserve revisions younger than <z>"));
  r.push_back(Parameter::Optional ('k', "repository master key(s)"));
  r.push_back(Parameter::Optional ('t', "temporary directory"));
  r.push_back(Parameter::Switch   ('d', "dry run"));
  r.push_back(Parameter::Switch   ('l', "list objects to be removed"));
  // to be extended...
  return r;
}


int CommandGc::Main(const ArgumentList &args) {
  const std::string &repo_url               = *args.find('r')->second;
  const std::string &spooler                = *args.find('u')->second;
  const std::string &repo_name              = *args.find('n')->second;
  const int64_t      revisions              = (args.count('h') > 0) ? String2Int64(*args.find('h')->second) : GcConfig::kFullHistory;
  const time_t       timestamp              = (args.count('z') > 0) ? static_cast<time_t>(String2Int64(*args.find('z')->second)) : GcConfig::kNoTimestamp;
  const std::string &repo_keys              = (args.count('k') > 0) ? *args.find('k')->second : "";
  const bool         dry_run                = (args.count('d') > 0);
  const bool         list_condemned_objects = (args.count('l') > 0);
  const std::string  temp_directory         = (args.count('t') > 0) ? *args.find('t')->second : "/tmp";

  if (revisions < 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "at least one revision needs to be preserved");
    return 1;
  }

  if (timestamp == GcConfig::kNoTimestamp &&
      revisions == GcConfig::kFullHistory) {
    LogCvmfs(kLogCvmfs, kLogStderr, "neither a timestamp nor history threshold given");
    return 1;
  }

  UniquePtr<ObjectFetcher> object_fetcher(ObjectFetcher::Create(repo_name,
                                                                repo_url,
                                                                repo_keys,
                                                                temp_directory));
  if (! object_fetcher) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to connect to repository");
    return 1;
  }


  UniquePtr<manifest::Manifest> manifest(object_fetcher->FetchManifest());
  if (! manifest || ! manifest->garbage_collectable()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "repository does not allow garbage collection");
    return 1;
  }

  GcConfig config;
  const upload::SpoolerDefinition spooler_definition(spooler, shash::kAny);
  config.uploader               = AbstractUploader::Construct(spooler_definition);
  config.keep_history_depth     = revisions;
  config.keep_history_timestamp = timestamp;
  config.dry_run                = dry_run;
  config.verbose                = list_condemned_objects;
  config.object_fetcher         = object_fetcher.weak_ref();

  if (config.uploader == NULL) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to initialize spooler for '%s'",
             spooler.c_str());
    return 1;
  }

  GC collector(config);
  return collector.Collect() ? 0 : 1;
}
