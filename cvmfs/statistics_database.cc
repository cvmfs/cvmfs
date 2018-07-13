/**
 * This file is part of the CernVM File System.
 */

#include "statistics_database.h"

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


void StatisticsDatabase::GetStats(swissknife::Command *command, Stats *stats) {
  stats->files_added = command->statistics()->
                       Lookup("Publish.n_files_added")->ToString();
  stats->files_removed = command->statistics()->
                   Lookup("Publish.n_files_removed")->ToString();
  stats->files_changed = command->statistics()->
                   Lookup("Publish.n_files_changed")->ToString();
  stats->dir_added = command->statistics()->
                       Lookup("Publish.n_directories_added")->ToString();
  stats->dir_removed = command->statistics()->
                   Lookup("Publish.n_directories_removed")->ToString();
  stats->dir_changed = command->statistics()->
                   Lookup("Publish.n_directories_changed")->ToString();
  stats->bytes_added = command->statistics()->
                   Lookup("Publish.sz_added_bytes")->ToString();
  stats->bytes_removed = command->statistics()->
                   Lookup("Publish.sz_removed_bytes")->ToString();
}


std::string StatisticsDatabase::GetTimestamp() {
  struct tm time_ptr;
  char date_and_time[50];
  time_t t = time(NULL);
  gmtime_r(&t, &time_ptr);      // take UTC
  // timestamp format
  strftime(date_and_time, 1000, "%Y-%m-%d %H:%M:%S", &time_ptr);
  std::string timestamp(date_and_time);
  return timestamp;
}


std::string StatisticsDatabase::PrepareStatement(std::string timestamp_value, Stats stats) {
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
    "'"+timestamp_value+"',"+  // TEXT
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


int StatisticsDatabase::StoreStatistics(swissknife::Command *command) {
  Stats stats;
  GetStats(command, &stats);
  GetTimestamp();

  sqlite::Sql insert(this->sqlite_db(),
                      PrepareStatement(GetTimestamp(), stats));

  if (!this->BeginTransaction()) {
    LogCvmfs(kLogCvmfs, kLogStdout, "BeginTransaction failed!");
    return 1;
  }

  if (!insert.Execute()) {
    LogCvmfs(kLogCvmfs, kLogStdout, "insert.Execute failed!");
    return 2;
  }

  if (!insert.Reset()) {
    LogCvmfs(kLogCvmfs, kLogStdout, "insert.Reset() failed!");
    return 3;
  }

  if (!this->CommitTransaction()) {
    LogCvmfs(kLogCvmfs, kLogStdout, "CommitTransaction failed!");
    return 4;
  }

  return 0;
}
