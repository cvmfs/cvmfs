/**
 * This file is part of the CernVM File System.
 */

#include "params.h"

#include <vector>

#include "options.h"
#include "util/string.h"

namespace receiver {

std::string GetSpoolerTempDir(const std::string &spooler_config) {
  const std::vector<std::string> tokens = SplitString(spooler_config, ',');
  assert(tokens.size() == 3);
  return tokens[1];
}

bool GetParamsFromFile(const std::string &repo_name, Params *params) {
  const std::string repo_config_file = "/etc/cvmfs/repositories.d/" + repo_name
                                       + "/server.conf";

  SimpleOptionsParser parser = SimpleOptionsParser(
      new DefaultOptionsTemplateManager(repo_name));
  if (!parser.TryParsePath(repo_config_file)) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "Could not parse repository configuration: %s.",
             repo_config_file.c_str());
    return false;
  }

  if (!parser.GetValue("CVMFS_STRATUM0", &params->stratum0)) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "Missing parameter %s in repository configuration file.",
             "CVMFS_STRATUM0");
    return false;
  }

  if (parser.IsDefined("CVMFS_SERVER_PROXY")) {
    parser.GetValue("CVMFS_SERVER_PROXY", &params->proxy);
  }

  // Note: TEST_CVMFS_RECEIVER_UPSTREAM_STORAGE is used to provide an
  //       an overriding value for CVMFS_UPSTREAM_STORAGE, to be used
  //       only by the cvmfs_receiver application. Useful for testing
  //       when the release manager and the repository gateway are
  //       running on the same machine.
  if (parser.IsDefined("TEST_CVMFS_RECEIVER_UPSTREAM_STORAGE")) {
    parser.GetValue("TEST_CVMFS_RECEIVER_UPSTREAM_STORAGE",
                    &params->spooler_configuration);
  } else {
    if (!parser.GetValue("CVMFS_UPSTREAM_STORAGE",
                         &params->spooler_configuration)) {
      LogCvmfs(kLogReceiver, kLogSyslogErr,
               "Missing parameter %s in repository configuration file.",
               "CVMFS_UPSTREAM_STORAGE");
      return false;
    }
  }

  std::string use_local_cache_str;
  if (parser.GetValue("CVMFS_SERVER_USE_CATALOG_CACHE", &use_local_cache_str)) {
    params->use_local_cache = parser.IsOn(use_local_cache_str);
  } else {
    params->use_local_cache = false;
  }


  std::string hash_algorithm_str;
  if (!parser.GetValue("CVMFS_HASH_ALGORITHM", &hash_algorithm_str)) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "Missing parameter %s in repository configuration file.",
             "CVMFS_HASH_ALGORITHM");
    return false;
  }
  params->hash_alg = shash::ParseHashAlgorithm(hash_algorithm_str);
  params->hash_alg_str = hash_algorithm_str;

  // The receiver does not store files, only catalogs.
  // We can safely hard-code the following options
  params->generate_legacy_bulk_chunks = false;
  params->compression_alg = zlib::kZlibDefault;

  std::string use_chunking_str;
  if (!parser.GetValue("CVMFS_USE_FILE_CHUNKING", &use_chunking_str)) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "Missing parameter %s in repository configuration file.",
             "CVMFS_USE_FILE_CHUNKING");
    return false;
  }
  params->use_file_chunking = parser.IsOn(use_chunking_str);

  std::string min_chunk_size_str;
  if (!parser.GetValue("CVMFS_MIN_CHUNK_SIZE", &min_chunk_size_str)) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "Missing parameter %s in repository configuration file.",
             "CVMFS_MIN_CHUNK_SIZE");
    return false;
  }
  params->min_chunk_size = String2Uint64(min_chunk_size_str);

  std::string avg_chunk_size_str;
  if (!parser.GetValue("CVMFS_AVG_CHUNK_SIZE", &avg_chunk_size_str)) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "Missing parameter %s in repository configuration file.",
             "CVMFS_AVG_CHUNK_SIZE");
    return false;
  }
  params->avg_chunk_size = String2Uint64(avg_chunk_size_str);

  std::string max_chunk_size_str;
  if (!parser.GetValue("CVMFS_MAX_CHUNK_SIZE", &max_chunk_size_str)) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "Missing parameter %s in repository configuration file.",
             "CVMFS_MAX_CHUNK_SIZE");
    return false;
  }
  params->max_chunk_size = String2Uint64(max_chunk_size_str);

  std::string garbage_collection_str;
  if (!parser.GetValue("CVMFS_GARBAGE_COLLECTION", &garbage_collection_str)) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "Missing parameter %s in repository configuration file.",
             "CVMFS_GARBAGE_COLLECTION");
    return false;
  }
  params->garbage_collection = parser.IsOn(garbage_collection_str);

  std::string use_autocatalogs_str;
  if (!parser.GetValue("CVMFS_AUTOCATALOGS", &use_autocatalogs_str)) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "Missing parameter %s in repository configuration file.",
             "CVMFS_AUTOCATALOGS");
    return false;
  }
  params->use_autocatalogs = parser.IsOn(use_autocatalogs_str);

  std::string max_weight_str;
  if (parser.GetValue("CVMFS_AUTOCATALOGS_MAX_WEIGHT", &max_weight_str)) {
    params->max_weight = String2Uint64(max_weight_str);
  }

  std::string min_weight_str;
  if (parser.GetValue("CVMFS_AUTOCATALOGS_MIN_WEIGHT", &min_weight_str)) {
    params->min_weight = String2Uint64(min_weight_str);
  }

  std::string enforce_limits_str;
  if (parser.GetValue("CVMFS_ENFORCE_LIMITS", &enforce_limits_str)) {
    params->enforce_limits = parser.IsOn(enforce_limits_str);
  }

  // TODO(dwd): the next 3 limit variables should take defaults from
  // SyncParameters
  params->nested_kcatalog_limit = 0;
  std::string nested_kcatalog_limit_str;
  if (parser.GetValue("CVMFS_NESTED_KCATALOG_LIMIT",
                      &nested_kcatalog_limit_str)) {
    params->nested_kcatalog_limit = String2Uint64(nested_kcatalog_limit_str);
  }

  params->root_kcatalog_limit = 0;
  std::string root_kcatalog_limit_str;
  if (parser.GetValue("CVMFS_ROOT_KCATALOG_LIMIT", &root_kcatalog_limit_str)) {
    params->root_kcatalog_limit = String2Uint64(root_kcatalog_limit_str);
  }

  params->file_mbyte_limit = 0;
  std::string file_mbyte_limit_str;
  if (parser.GetValue("CVMFS_FILE_MBYTE_LIMIT", &file_mbyte_limit_str)) {
    params->file_mbyte_limit = String2Uint64(file_mbyte_limit_str);
  }

  std::string upload_stats_db_str;
  if (parser.GetValue("CVMFS_UPLOAD_STATS_DB", &upload_stats_db_str)) {
    params->upload_stats_db = parser.IsOn(upload_stats_db_str);
  } else {
    params->upload_stats_db = false;
  }

  return true;
}

}  // namespace receiver
