/**
 * This file is part of the CernVM File System.
 *
 * This command processes a repository's catalog structure to detect and remove
 * outdated and/or unneeded data objects.
 */

#include "cvmfs_config.h"
#include "swissknife_gc.h"

#include <string>

#include "garbage_collection/garbage_collector.h"
#include "garbage_collection/hash_filter.h"
#include "manifest.h"
#include "upload_facility.h"

namespace swissknife {

typedef HttpObjectFetcher<> ObjectFetcher;
typedef CatalogTraversal<ObjectFetcher> ReadonlyCatalogTraversal;
typedef GarbageCollector<ReadonlyCatalogTraversal, SimpleHashFilter> GC;
typedef GC::Configuration GcConfig;


ParameterList CommandGc::GetParams() {
  ParameterList r;
  r.push_back(Parameter::Mandatory('r', "repository url"));
  r.push_back(Parameter::Mandatory('u', "spooler definition string"));
  r.push_back(Parameter::Mandatory('n', "fully qualified repository name"));
  r.push_back(Parameter::Optional('h', "conserve <h> revisions"));
  r.push_back(Parameter::Optional('z', "conserve revisions younger than <z>"));
  r.push_back(Parameter::Optional('k', "repository master key(s)"));
  r.push_back(Parameter::Optional('t', "temporary directory"));
  r.push_back(Parameter::Optional('L', "path to deletion log file"));
  r.push_back(Parameter::Switch('d', "dry run"));
  r.push_back(Parameter::Switch('l', "list objects to be removed"));
  return r;
}


int CommandGc::Main(const ArgumentList &args) {
  const std::string &repo_url = *args.find('r')->second;
  const std::string &spooler = *args.find('u')->second;
  const std::string &repo_name = *args.find('n')->second;
  const int64_t revisions = (args.count('h') > 0) ?
    String2Int64(*args.find('h')->second) : GcConfig::kFullHistory;
  const time_t timestamp  = (args.count('z') > 0)
    ? static_cast<time_t>(String2Int64(*args.find('z')->second))
    : GcConfig::kNoTimestamp;
  const std::string &repo_keys = (args.count('k') > 0) ?
    *args.find('k')->second : "";
  const bool dry_run = (args.count('d') > 0);
  const bool list_condemned_objects = (args.count('l') > 0);
  const std::string temp_directory = (args.count('t') > 0) ?
    *args.find('t')->second : "/tmp";
  const std::string deletion_log_path = (args.count('L') > 0) ?
    *args.find('L')->second : "";

  if (revisions < 0) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "at least one revision needs to be preserved");
    return 1;
  }

  if (timestamp == GcConfig::kNoTimestamp &&
      revisions == GcConfig::kFullHistory) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "neither a timestamp nor history threshold given");
    return 1;
  }

  FILE *deletion_log_file = NULL;
  if (!deletion_log_path.empty()) {
    deletion_log_file = fopen(deletion_log_path.c_str(), "a+");
    if (NULL == deletion_log_file) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to open deletion log file "
                                      "(errno: %d)", errno);
      return 1;
    }
  }

  const bool follow_redirects = false;
  if (!this->InitDownloadManager(follow_redirects) ||
      !this->InitVerifyingSignatureManager(repo_keys)) {
    LogCvmfs(kLogCatalog, kLogStderr, "failed to init repo connection");
    return 1;
  }

  ObjectFetcher object_fetcher(repo_name,
                               repo_url,
                               temp_directory,
                               download_manager(),
                               signature_manager());

  UniquePtr<manifest::Manifest> manifest;
  ObjectFetcher::Failures retval = object_fetcher.FetchManifest(&manifest);
  if (retval != ObjectFetcher::kFailOk) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load repository manifest "
                                    "(%d - %s)",
                                    retval, Code2Ascii(retval));
    return 1;
  }

  if (!manifest->garbage_collectable()) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "repository does not allow garbage collection");
    return 1;
  }

  GcConfig config;
  const upload::SpoolerDefinition spooler_definition(spooler, shash::kAny);
  config.uploader = upload::AbstractUploader::Construct(spooler_definition);
  config.keep_history_depth = revisions;
  config.keep_history_timestamp = timestamp;
  config.dry_run = dry_run;
  config.verbose = list_condemned_objects;
  config.object_fetcher = &object_fetcher;
  config.deleted_objects_logfile = deletion_log_file;

  if (config.uploader == NULL) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to initialize spooler for '%s'",
             spooler.c_str());
    return 1;
  }

  if (deletion_log_file != NULL) {
    const int bytes_written = fprintf(deletion_log_file,
                                      "# Garbage Collection started at %s\n",
                                      StringifyTime(time(NULL), true).c_str());
    if (bytes_written < 0) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to write to deletion log '%s' "
                                      "(errno: %d)",
                                      deletion_log_path.c_str(), errno);
      return 1;
    }
  }

  GC collector(config);
  const bool success = collector.Collect();

  if (deletion_log_file != NULL) {
    const int bytes_written = fprintf(deletion_log_file,
                                      "# Garbage Collection finished at %s\n\n",
                                      StringifyTime(time(NULL), true).c_str());
    assert(bytes_written >= 0);
    fclose(deletion_log_file);
  }

  return success ? 0 : 1;
}

}  // namespace swissknife
