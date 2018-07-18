/**
 * This file is part of the CernVM File System.
 */

#include "statistics_database.h"

namespace {

  /**
  * Get UTC Time.
  *
  * @return a timestamp in "YYYY-MM-DD HH:MM:SS" format
  */
std::string GetGMTimestamp() {
  struct tm time_ptr;
  char date_and_time[50];
  time_t t = time(NULL);
  gmtime_r(&t, &time_ptr);      // take UTC
  // timestamp format
  strftime(date_and_time, 50, "%Y-%m-%d %H:%M:%S", &time_ptr);
  std::string timestamp(date_and_time);
  return timestamp;
}

/**
  * Build the insert statement.
  *
  * @param stats a struct with all values stored in strings
  * @return the insert statement
  */
std::string PrepareStatement(Stats stats) {
  std::string insert_statement =
    "INSERT INTO publish_statistics ("
    "timestamp,"
    "files_added,"
    "files_removed,"
    "files_changed,"
    "directories_added,"
    "directories_removed,"
    "directories_changed,"
    "sz_bytes_added,"
    "sz_bytes_removed)"
    " VALUES("
    "'"+GetGMTimestamp()+"',"+  // TEXT
    stats.files_added+"," +
    stats.files_removed +","+
    stats.files_changed + "," +
    stats.dir_added + "," +
    stats.dir_removed + "," +
    stats.dir_changed + "," +
    stats.bytes_added + "," +
    stats.bytes_removed + ");";
  return insert_statement;
}

}  // namespace


const float    StatisticsDatabase::kLatestCompatibleSchema = 1.0f;
float          StatisticsDatabase::kLatestSchema           = 1.0f;
unsigned       StatisticsDatabase::kLatestSchemaRevision   =
                                              RevisionFlags::kInitialRevision;
unsigned int   StatisticsDatabase::instances               = 0;
bool           StatisticsDatabase::compacting_fails        = false;


bool StatisticsDatabase::CreateEmptyDatabase() {
  ++create_empty_db_calls;
  return sqlite::Sql(sqlite_db(),
    "CREATE TABLE publish_statistics ("
    "timestamp TEXT,"
    "files_added INTEGER,"
    "files_removed INTEGER,"
    "files_changed INTEGER,"
    "directories_added INTEGER,"
    "directories_removed INTEGER,"
    "directories_changed INTEGER,"
    "sz_bytes_added INTEGER,"
    "sz_bytes_removed INTEGER,"
    "CONSTRAINT pk_publish_statistics PRIMARY KEY (timestamp));").Execute();
}


bool StatisticsDatabase::CheckSchemaCompatibility() {
  ++check_compatibility_calls;
  return (schema_version() > kLatestCompatibleSchema - 0.1 &&
          schema_version() < kLatestCompatibleSchema + 0.1);
}


bool StatisticsDatabase::LiveSchemaUpgradeIfNecessary() {
  ++live_upgrade_calls;
  const unsigned int revision = schema_revision();

  if (revision == RevisionFlags::kInitialRevision) {
    return true;
  }

  if (revision == RevisionFlags::kUpdatableRevision) {
    set_schema_revision(RevisionFlags::kUpdatedRevision);
    StoreSchemaRevision();
    return true;
  }

  if (revision == RevisionFlags::kFailingRevision) {
    return false;
  }

  return false;
}


bool StatisticsDatabase::CompactDatabase() const {
  ++compact_calls;
  return !compacting_fails;
}


StatisticsDatabase::~StatisticsDatabase() {
  --StatisticsDatabase::instances;
}


Stats StatisticsDatabase::GetStats(swissknife::Command *command) {
  Stats stats;
  stats.files_added = command->statistics()->
                       Lookup("Publish.n_files_added")->ToString();
  stats.files_removed = command->statistics()->
                       Lookup("Publish.n_files_removed")->ToString();
  stats.files_changed = command->statistics()->
                       Lookup("Publish.n_files_changed")->ToString();
  stats.dir_added = command->statistics()->
                       Lookup("Publish.n_directories_added")->ToString();
  stats.dir_removed = command->statistics()->
                       Lookup("Publish.n_directories_removed")->ToString();
  stats.dir_changed = command->statistics()->
                       Lookup("Publish.n_directories_changed")->ToString();
  stats.bytes_added = command->statistics()->
                       Lookup("Publish.sz_added_bytes")->ToString();
  stats.bytes_removed = command->statistics()->
                       Lookup("Publish.sz_removed_bytes")->ToString();
  return stats;
}


int StatisticsDatabase::StoreStatistics(swissknife::Command *command) {
  Stats stats = GetStats(command);

  sqlite::Sql insert(this->sqlite_db(), PrepareStatement(stats));

  if (!this->BeginTransaction()) {
    LogCvmfs(kLogCvmfs, kLogSyslogErr, "BeginTransaction failed!");
    return -1;
  }

  if (!insert.Execute()) {
    LogCvmfs(kLogCvmfs, kLogSyslogErr, "insert.Execute failed!");
    return -2;
  }

  if (!insert.Reset()) {
    LogCvmfs(kLogCvmfs, kLogSyslogErr, "insert.Reset() failed!");
    return -3;
  }

  if (!this->CommitTransaction()) {
    LogCvmfs(kLogCvmfs, kLogSyslogErr, "CommitTransaction failed!");
    return -4;
  }

  return 0;
}


std::string StatisticsDatabase::GetDBPath(std::string repo_name) {
  // default location
  const std::string db_default_path =
      "/var/spool/cvmfs/" + repo_name + "/stats.db";
  const std::string repo_config_file =
      "/etc/cvmfs/repositories.d/" + repo_name + "/server.conf";
  SimpleOptionsParser parser;

  if (!parser.TryParsePath(repo_config_file)) {
    LogCvmfs(kLogCvmfs, kLogSyslogErr,
             "Could not parse repository configuration: %s.",
             repo_config_file.c_str());
    return db_default_path;
  }

  std::string statistics_db = "";
  if (!parser.GetValue("CVMFS_STATISTICS_DB", &statistics_db)) {
    LogCvmfs(kLogCvmfs, kLogSyslogErr,
             "Missing parameter %s in repository configuration file.",
             "CVMFS_STATISTICS_DB");
    return db_default_path;
  }

  std::string dirname = GetParentPath(statistics_db);
  int mode = S_IRUSR | S_IWUSR | S_IXUSR |
             S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;  // 755
  if (!MkdirDeep(dirname, mode, true)) {
    LogCvmfs(kLogCvmfs, kLogSyslogErr,
      "Couldn't write statistics at the specified path %s.",
      statistics_db.c_str());
    return db_default_path;
  }

  return statistics_db;
}


StatisticsDatabase::StatisticsDatabase(const std::string  &filename,
              const OpenMode      open_mode) :
  sqlite::Database<StatisticsDatabase>(filename, open_mode),
  create_empty_db_calls(0),  check_compatibility_calls(0),
  live_upgrade_calls(0), compact_calls(0)
{
  ++StatisticsDatabase::instances;
}
