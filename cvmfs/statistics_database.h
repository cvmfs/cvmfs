/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_STATISTICS_DATABASE_H_
#define CVMFS_STATISTICS_DATABASE_H_

#include <ctime>
#include <string>

#include "options.h"
#include "sql.h"
#include "statistics.h"
#include "upload.h"
#include "util/posix.h"
#include "util/string.h"

struct Stats;

class StatisticsDatabase : public sqlite::Database<StatisticsDatabase> {
 protected:
  friend class sqlite::Database<StatisticsDatabase>;
  StatisticsDatabase(const std::string &filename, const OpenMode open_mode);

  std::string repo_name_;

  bool StoreEntry(const std::string &insert_statement);

  /**
   * Prune the statistics DB (delete records older than certain threshold)
   * @param days number of days of records to keep, 0 means no pruning
   * @return true on success, false otherwise
   */
  bool Prune(uint32_t days);

 public:
  // not const - needs to be adaptable!
  static float kLatestSchema;
  // not const - needs to be adaptable!
  static unsigned kLatestSchemaRevision;
  static const float kLatestCompatibleSchema;
  static bool compacting_fails;
  static unsigned int instances;
  unsigned int create_empty_db_calls;
  unsigned int check_compatibility_calls;
  unsigned int live_upgrade_calls;
  mutable unsigned int compact_calls;

  bool CreateEmptyDatabase();
  bool CheckSchemaCompatibility();
  bool LiveSchemaUpgradeIfNecessary();
  bool CompactDatabase() const;
  ~StatisticsDatabase();

  /**
   * Opens or creates a statistics DB in standard path
   * for repo_name repository
   *
   * @return StatisticsDatabase pointer on success, NULL otherwise
   */
  static StatisticsDatabase *OpenStandardDB(const std::string repo_name);

  /**
   * Store a publish entry into the database
   *
   * @return true on success, false otherwise
   */
  bool StorePublishStatistics(const perf::Statistics *statistics,
                              const std::string &start_time,
                              const bool success);

  /**
   * Store a publish entry into the database
   *
   * @return true on success, false otherwise
   */
  bool StoreGCStatistics(const perf::Statistics *statistics,
                         const std::string &start_time,
                         const bool success);

  /**
   * Upload the statistics database into the storage backend
   * configured in spooler. This is a convenience method
   * useful when only the spooler (and not the underlying)
   * uploader) is available to the developer.
   *
   * @return true on success, false otherwise
   */
  bool UploadStatistics(upload::Spooler *spooler, std::string local_path = "");

  /**
   * Upload the statistics database into the storage backend
   * configured in uploader.
   *
   * @return true on success, false otherwise
   */
  bool UploadStatistics(upload::AbstractUploader *uploader,
                        std::string local_path = "");

  /**
   * Get the parameters for the database file
   * user can specify them in the server.conf
   * DB file path:
   *   key: CVMFS_STATISTICS_DB
   *   default: /var/spool/cvmfs/$repo_name/stats.db
   * maximum age (in days) of records to keep when pruning
   * (zero means keep forever)
   *   key: CVMFS_STATS_DB_DAYS_TO_KEEP
   *   default: 365
   *
   * @param repo_name Fully qualified name of the repository
   * @param path pointer to save the db path in
   * @param days_to_keep pointer to save the number of days
   */
  static void GetDBParams(const std::string &repo_name,
                          std::string *path,
                          uint32_t *days_to_keep);

  /**
   * Check if the CVMFS_EXTENDED_GC_STATS is ON or not
   *
   * @param repo_name fully qualified name of the repository
   * @return true if CVMFS_EXTENDED_GC_STATS is ON, false otherwise
   */
  static bool GcExtendedStats(const std::string &repo_name);

 private:
  static const uint32_t kDefaultDaysToKeep;
};


#endif  // CVMFS_STATISTICS_DATABASE_H_
