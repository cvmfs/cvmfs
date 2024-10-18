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
#include "garbage_collection/gc_aux.h"
#include "garbage_collection/hash_filter.h"
#include "manifest.h"
#include "reflog.h"
#include "statistics_database.h"
#include "upload_facility.h"
#include "util/posix.h"
#include "util/string.h"

namespace swissknife {

typedef HttpObjectFetcher<> ObjectFetcher;
typedef CatalogTraversalParallel<ObjectFetcher> ReadonlyCatalogTraversal;
typedef SmallhashFilter HashFilter;
typedef GarbageCollector<ReadonlyCatalogTraversal, HashFilter> GC;
typedef GarbageCollectorAux<ReadonlyCatalogTraversal, HashFilter> GCAux;
typedef GC::Configuration GcConfig;


ParameterList CommandGc::GetParams() const {
  ParameterList r;
  r.push_back(Parameter::Mandatory('r', "repository url"));
  r.push_back(Parameter::Mandatory('u', "spooler definition string"));
  r.push_back(Parameter::Mandatory('n', "fully qualified repository name"));
  r.push_back(Parameter::Mandatory('R', "path to reflog.chksum file"));
  r.push_back(Parameter::Optional('h', "conserve <h> revisions"));
  r.push_back(Parameter::Optional('z', "conserve revisions younger than <z>"));
  r.push_back(Parameter::Optional('k', "repository master key(s) / dir"));
  r.push_back(Parameter::Optional('t', "temporary directory"));
  r.push_back(Parameter::Optional('L', "path to deletion log file"));
  r.push_back(Parameter::Optional('N', "number of threads to use"));
  r.push_back(Parameter::Optional('@', "proxy url"));
  r.push_back(Parameter::Switch('d', "dry run"));
  r.push_back(Parameter::Switch('l', "list objects to be removed"));
  r.push_back(Parameter::Switch('I', "upload updated statistics DB file"));
  return r;
}


int CommandGc::Main(const ArgumentList &args) {
  std::string start_time = GetGMTimestamp();

  const std::string &repo_url = *args.find('r')->second;
  const std::string &spooler = *args.find('u')->second;
  const std::string &repo_name = *args.find('n')->second;
  const std::string &reflog_chksum_path = *args.find('R')->second;
  shash::Any reflog_hash;
  if (!manifest::Reflog::ReadChecksum(reflog_chksum_path, &reflog_hash)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Could not read reflog checksum");
    return 1;
  }

  const uint64_t revisions = (args.count('h') > 0) ?
    String2Int64(*args.find('h')->second) : GcConfig::kFullHistory;
  const time_t timestamp  = (args.count('z') > 0)
    ? static_cast<time_t>(String2Int64(*args.find('z')->second))
    : GcConfig::kNoTimestamp;
  std::string repo_keys = (args.count('k') > 0) ?
    *args.find('k')->second : "";
  if (DirectoryExists(repo_keys))
    repo_keys = JoinStrings(FindFilesBySuffix(repo_keys, ".pub"), ":");
  const bool dry_run = (args.count('d') > 0);
  const bool list_condemned_objects = (args.count('l') > 0);
  const std::string temp_directory = (args.count('t') > 0) ?
    *args.find('t')->second : "/tmp";
  const std::string deletion_log_path = (args.count('L') > 0) ?
    *args.find('L')->second : "";
  const bool upload_statsdb = (args.count('I') > 0);
  const unsigned int num_threads = (args.count('N') > 0) ?
    String2Uint64(*args.find('N')->second) : 8;

  if (timestamp == GcConfig::kNoTimestamp &&
      revisions == GcConfig::kFullHistory) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "neither a timestamp nor history threshold given");
    return 1;
  }

  const bool follow_redirects = false;
  const std::string proxy = ((args.count('@') > 0) ?
                             *args.find('@')->second : "");
  if (!this->InitDownloadManager(follow_redirects, proxy) ||
      !this->InitSignatureManager(repo_keys)) {
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

  UniquePtr<manifest::Reflog> reflog;
  reflog = FetchReflog(&object_fetcher, repo_name, reflog_hash);
  assert(reflog.IsValid());

  const upload::SpoolerDefinition spooler_definition(spooler, shash::kAny);
  UniquePtr<upload::AbstractUploader> uploader(
                       upload::AbstractUploader::Construct(spooler_definition));

  if (!uploader.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to initialize spooler for '%s'",
             spooler.c_str());
    return 1;
  }

  FILE *deletion_log_file = NULL;
  if (!deletion_log_path.empty()) {
    deletion_log_file = fopen(deletion_log_path.c_str(), "a+");
    if (NULL == deletion_log_file) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to open deletion log file "
                                      "(errno: %d)", errno);
      uploader->TearDown();
      return 1;
    }
  }

  bool extended_stats = StatisticsDatabase::GcExtendedStats(repo_name);

  reflog->BeginTransaction();

  GcConfig config;
  config.uploader                = uploader.weak_ref();
  config.keep_history_depth      = revisions;
  config.keep_history_timestamp  = timestamp;
  config.dry_run                 = dry_run;
  config.verbose                 = list_condemned_objects;
  config.object_fetcher          = &object_fetcher;
  config.reflog                  = reflog.weak_ref();
  config.deleted_objects_logfile = deletion_log_file;
  config.statistics              = statistics();
  config.extended_stats          = extended_stats;
  config.num_threads             = num_threads;

  if (deletion_log_file != NULL) {
    const int bytes_written = fprintf(deletion_log_file,
                                      "# Garbage Collection started at %s\n",
                                      StringifyTime(time(NULL), true).c_str());
    if (bytes_written < 0) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to write to deletion log '%s' "
                                      "(errno: %d)",
                                      deletion_log_path.c_str(), errno);
      uploader->TearDown();
      return 1;
    }
  }

  StatisticsDatabase *stats_db = StatisticsDatabase::OpenStandardDB(repo_name);

  // File catalogs
  GC collector(config);
  collector.UseReflogTimestamps();
  bool success = collector.Collect();

  if (!success) {
    LogCvmfs(kLogCvmfs, kLogStderr, "garbage collection failed");
    if (!dry_run) {
      stats_db->StoreGCStatistics(this->statistics(), start_time, false);
      if (upload_statsdb) {
        stats_db->UploadStatistics(uploader.weak_ref());
      }
    }
    uploader->TearDown();
    return 1;
  }

  // Tag databases, meta infos, certificates
  HashFilter preserved_objects;
  preserved_objects.Fill(manifest->certificate());
  preserved_objects.Fill(manifest->history());
  preserved_objects.Fill(manifest->meta_info());
  GCAux collector_aux(config);
  success = collector_aux.CollectOlderThan(
    collector.oldest_trunk_catalog(), preserved_objects);
  if (!success) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "garbage collection of auxiliary files failed");
    if (!dry_run) {
      stats_db->StoreGCStatistics(this->statistics(), start_time, false);
      if (upload_statsdb) {
        stats_db->UploadStatistics(uploader.weak_ref());
      }
    }
    uploader->TearDown();
    return 1;
  }

  // As of here: garbage collection succeeded, cleanup & commit

  if (deletion_log_file != NULL) {
    const int bytes_written = fprintf(deletion_log_file,
                                      "# Garbage Collection finished at %s\n\n",
                                      StringifyTime(time(NULL), true).c_str());
    assert(bytes_written >= 0);
    fclose(deletion_log_file);
  }

  reflog->CommitTransaction();
  // Has to be outside the transaction
  success = reflog->Vacuum();
  assert(success);
  reflog->DropDatabaseFileOwnership();
  const std::string reflog_db = reflog->database_file();
  reflog.Destroy();

  if (!dry_run) {
    uploader->UploadFile(reflog_db, ".cvmfsreflog");
    manifest::Reflog::HashDatabase(reflog_db, &reflog_hash);
    uploader->WaitForUpload();
    manifest::Reflog::WriteChecksum(reflog_chksum_path, reflog_hash);
  }

  unlink(reflog_db.c_str());

  if (uploader->GetNumberOfErrors() > 0 && !dry_run) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to upload updated Reflog");

    stats_db->StoreGCStatistics(this->statistics(), start_time, false);
    if (upload_statsdb) {
      stats_db->UploadStatistics(uploader.weak_ref());
    }

    uploader->TearDown();
    return 1;
  }

  if (!dry_run) {
    stats_db->StoreGCStatistics(this->statistics(), start_time, true);
    if (upload_statsdb) {
      stats_db->UploadStatistics(uploader.weak_ref());
    }
  }
  uploader->TearDown();
  return 0;
}

}  // namespace swissknife
