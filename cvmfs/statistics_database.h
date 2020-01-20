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
  StatisticsDatabase(const std::string  &filename,
                     const OpenMode      open_mode);

  std::string repo_name_;

  bool StoreEntry(const std::string &insert_statement);

 public:
  // not const - needs to be adaptable!
  static float        kLatestSchema;
  // not const - needs to be adaptable!
  static unsigned     kLatestSchemaRevision;
  static const float  kLatestCompatibleSchema;
  static bool         compacting_fails;
  static unsigned int  instances;
  unsigned int         create_empty_db_calls;
  unsigned int         check_compatibility_calls;
  unsigned int         live_upgrade_calls;
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
  bool UploadStatistics(upload::Spooler *spooler,
                        std::string local_path = "");

/**
 * Upload the statistics database into the storage backend
 * configured in uploader.
 * 
 * @return true on success, false otherwise
 */
  bool UploadStatistics(upload::AbstractUploader *uploader,
                        std::string local_path = "");

/**
  * Get the path for the database file
  * user can specify it in the server.conf
  * by default: /var/spool/cvmfs/$repo_name/stats.db
  *
  * @param repo_name Fully qualified name of the repository
  * @return path to store database file
  */
  static std::string GetDBPath(const std::string &repo_name);

  /**
  * Check if the CVMFS_EXTENDED_GC_STATS is ON or not
  *
  * @param repo_name fully qualified name of the repository
  * @return true if CVMFS_EXTENDED_GC_STATS is ON, false otherwise
  */
  static bool GcExtendedStats(const std::string &repo_name);
};


#endif  // CVMFS_STATISTICS_DATABASE_H_
