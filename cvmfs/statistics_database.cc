/**
 * This file is part of the CernVM File System.
 */

#include "statistics_database.h"


const float    StatisticsDatabase::kLatestCompatibleSchema = 1.0f;
float          StatisticsDatabase::kLatestSchema           = 1.0f;

// Changelog
// 1 --> 2: (Sep 4 2019)
//          * change column name `finished_time` -> `finish_time`
//            in publish_statistics table
//          * add column `revision` to publish_statistics table
//          * change column name `duplicated_files` -> `chunks_duplicated`
//            in publish_statistics table
//          * add column `chunks_added` to publish_statistics table
//          * add column `symlinks_added` to publish_statistics table
//          * add column `symlinks_removed` to publish_statistics table
//          * add column `symlinks_changed` to publish_statistics table
//          * change column name `finished_time` -> `finish_time`
//            in gc_statistics table

unsigned       StatisticsDatabase::kLatestSchemaRevision   = 2;
unsigned int   StatisticsDatabase::instances               = 0;
bool           StatisticsDatabase::compacting_fails        = false;


namespace {

struct PublishStats {
  std::string revision;
  std::string files_added;
  std::string files_removed;
  std::string files_changed;
  std::string chunks_added;
  std::string chunks_duplicated;
  std::string catalogs_added;
  std::string dirs_added;
  std::string dirs_removed;
  std::string dirs_changed;
  std::string symlinks_added;
  std::string symlinks_removed;
  std::string symlinks_changed;
  std::string bytes_added;
  std::string bytes_removed;
  std::string bytes_uploaded;
  std::string catalog_bytes_uploaded;

  explicit PublishStats(const perf::Statistics *statistics):
    revision(statistics->
                    Lookup("Publish.revision")->ToString()),
    files_added(statistics->
                    Lookup("Publish.n_files_added")->ToString()),
    files_removed(statistics->
                    Lookup("Publish.n_files_removed")->ToString()),
    files_changed(statistics->
                    Lookup("Publish.n_files_changed")->ToString()),
    chunks_added(statistics->
                    Lookup("Publish.n_chunks_added")->ToString()),
    chunks_duplicated(statistics->
                    Lookup("Publish.n_chunks_duplicated")->ToString()),
    catalogs_added(statistics->
                    Lookup("Publish.n_catalogs_added")->ToString()),
    dirs_added(statistics->
                    Lookup("Publish.n_directories_added")->ToString()),
    dirs_removed(statistics->
                    Lookup("Publish.n_directories_removed")->ToString()),
    dirs_changed(statistics->
                    Lookup("Publish.n_directories_changed")->ToString()),
    symlinks_added(statistics->
                    Lookup("Publish.n_symlinks_added")->ToString()),
    symlinks_removed(statistics->
                    Lookup("Publish.n_symlinks_removed")->ToString()),
    symlinks_changed(statistics->
                    Lookup("Publish.n_symlinks_changed")->ToString()),
    bytes_added(statistics->
                    Lookup("Publish.sz_added_bytes")->ToString()),
    bytes_removed(statistics->
                    Lookup("Publish.sz_removed_bytes")->ToString()),
    bytes_uploaded(statistics->
                    Lookup("Publish.sz_uploaded_bytes")->ToString()),
    catalog_bytes_uploaded(statistics->
                    Lookup("Publish.sz_uploaded_catalog_bytes")->ToString()) {
  }
};


struct GcStats {
  std::string n_preserved_catalogs;
  std::string n_condemned_catalogs;
  std::string n_condemned_objects;
  std::string sz_condemned_bytes;

  explicit GcStats(const perf::Statistics *statistics):
    n_preserved_catalogs(statistics->
                    Lookup("gc.n_preserved_catalogs")->ToString()),
    n_condemned_catalogs(statistics->
                    Lookup("gc.n_condemned_catalogs")->ToString()),
    n_condemned_objects(statistics->
                    Lookup("gc.n_condemned_objects")->ToString()),
    sz_condemned_bytes(statistics->
                    Lookup("gc.sz_condemned_bytes")->ToString()) {
  }
};


/**
  * Build the insert statement into publish_statistics table.
  *
  * @param stats a struct with all values stored in strings
  * @return the insert statement
  */
std::string PrepareStatementIntoPublish(const perf::Statistics *statistics,
                            const std::string &start_time,
                            const std::string &finish_time) {
  struct PublishStats stats = PublishStats(statistics);
  std::string insert_statement =
    "INSERT INTO publish_statistics ("
    "start_time,"
    "finish_time,"
    "revision,"
    "files_added,"
    "files_removed,"
    "files_changed,"
    "chunks_added,"
    "chunks_duplicated,"
    "catalogs_added,"
    "directories_added,"
    "directories_removed,"
    "directories_changed,"
    "symlinks_added,"
    "symlinks_removed,"
    "symlinks_changed,"
    "sz_bytes_added,"
    "sz_bytes_removed,"
    "sz_bytes_uploaded,"
    "sz_catalog_bytes_uploaded)"
    " VALUES("
    "'"+start_time+"',"+
    "'"+finish_time+"',"+
    stats.revision+"," +
    stats.files_added+"," +
    stats.files_removed +","+
    stats.files_changed + "," +
    stats.chunks_added + "," +
    stats.chunks_duplicated + "," +
    stats.catalogs_added + "," +
    stats.dirs_added + "," +
    stats.dirs_removed + "," +
    stats.dirs_changed + "," +
    stats.symlinks_added + "," +
    stats.symlinks_removed + "," +
    stats.symlinks_changed + "," +
    stats.bytes_added + "," +
    stats.bytes_removed + "," +
    stats.bytes_uploaded + "," +
    stats.catalog_bytes_uploaded + ");";
  return insert_statement;
}


/**
  * Build the insert statement into gc_statistics table.
  *
  * @param stats a struct with values stored in strings
  * @param start_time, finish_time to run Main() of the command
  * @param repo_name fully qualified name of the repository
  *
  * @return the insert statement
  */
std::string PrepareStatementIntoGc(const perf::Statistics *statistics,
                            const std::string &start_time,
                            const std::string &finish_time,
                            const std::string &repo_name) {
  struct GcStats stats = GcStats(statistics);
  std::string insert_statement = "";
  if (StatisticsDatabase::GcExtendedStats(repo_name)) {
    insert_statement =
      "INSERT INTO gc_statistics ("
      "start_time,"
      "finish_time,"
      "n_preserved_catalogs,"
      "n_condemned_catalogs,"
      "n_condemned_objects,"
      "sz_condemned_bytes)"
      " VALUES("
      "'" + start_time + "'," +
      "'" + finish_time + "'," +
      stats.n_preserved_catalogs + "," +
      stats.n_condemned_catalogs + ","+
      stats.n_condemned_objects + "," +
      stats.sz_condemned_bytes + ");";
  } else {
    // insert values except sz_condemned_bytes
    insert_statement =
      "INSERT INTO gc_statistics ("
      "start_time,"
      "finish_time,"
      "n_preserved_catalogs,"
      "n_condemned_catalogs,"
      "n_condemned_objects)"
      " VALUES("
      "'" + start_time + "'," +
      "'" + finish_time + "'," +
      stats.n_preserved_catalogs + "," +
      stats.n_condemned_catalogs + ","+
      stats.n_condemned_objects + ");";
  }
  return insert_statement;
}

}  // namespace


bool StatisticsDatabase::CreateEmptyDatabase() {
  ++create_empty_db_calls;
  bool ret1 = sqlite::Sql(sqlite_db(),
    "CREATE TABLE publish_statistics ("
    "publish_id INTEGER PRIMARY KEY,"
    "start_time TEXT,"
    "finish_time TEXT,"
    "revision INTEGER,"
    "files_added INTEGER,"
    "files_removed INTEGER,"
    "files_changed INTEGER,"
    "chunks_added INTEGER,"
    "chunks_duplicated INTEGER,"
    "catalogs_added INTEGER,"
    "directories_added INTEGER,"
    "directories_removed INTEGER,"
    "directories_changed INTEGER,"
    "symlinks_added INTEGER,"
    "symlinks_removed INTEGER,"
    "symlinks_changed INTEGER,"
    "sz_bytes_added INTEGER,"
    "sz_bytes_removed INTEGER,"
    "sz_bytes_uploaded INTEGER,"
    "sz_catalog_bytes_uploaded INTEGER);").Execute();
  bool ret2 = sqlite::Sql(sqlite_db(),
    "CREATE TABLE gc_statistics ("
    "gc_id INTEGER PRIMARY KEY,"
    "start_time TEXT,"
    "finish_time TEXT,"
    "n_preserved_catalogs INTEGER,"
    "n_condemned_catalogs INTEGER,"
    "n_condemned_objects INTEGER,"
    "sz_condemned_bytes INTEGER);").Execute();
  return ret1 & ret2;
}


bool StatisticsDatabase::CheckSchemaCompatibility() {
  ++check_compatibility_calls;
  return (schema_version() > kLatestCompatibleSchema - 0.1 &&
          schema_version() < kLatestCompatibleSchema + 0.1);
}


bool StatisticsDatabase::LiveSchemaUpgradeIfNecessary() {
  ++live_upgrade_calls;

  if (IsEqualSchema(schema_version(), kLatestSchema) &&
    (schema_revision() == 1)) {
    LogCvmfs(kLogCvmfs, kLogDebug, "upgrading schema revision (1 --> 2) of "
      "statistics database");

    sqlite::Sql publish_upgrade1(this->sqlite_db(), "ALTER TABLE "
    "publish_statistics RENAME COLUMN finished_time TO finish_time;");
    sqlite::Sql publish_upgrade2(this->sqlite_db(), "ALTER TABLE "
    "publish_statistics ADD revision INTEGER;");
    sqlite::Sql publish_upgrade3(this->sqlite_db(), "ALTER TABLE "
    "publish_statistics RENAME COLUMN duplicated_files TO chunks_duplicated");
    sqlite::Sql publish_upgrade4(this->sqlite_db(), "ALTER TABLE "
    "publish_statistics ADD chunks_added INTEGER;");
    sqlite::Sql publish_upgrade5(this->sqlite_db(), "ALTER TABLE "
    "publish_statistics ADD symlinks_added INTEGER;");
    sqlite::Sql publish_upgrade6(this->sqlite_db(), "ALTER TABLE "
    "publish_statistics ADD symlinks_removed INTEGER;");
    sqlite::Sql publish_upgrade7(this->sqlite_db(), "ALTER TABLE "
    "publish_statistics ADD symlinks_changed INTEGER;");
    sqlite::Sql publish_upgrade8(this->sqlite_db(), "ALTER TABLE "
    "publish_statistics ADD catalogs_added INTEGER;");
    sqlite::Sql publish_upgrade9(this->sqlite_db(), "ALTER TABLE "
    "publish_statistics ADD sz_catalog_bytes_uploaded INTEGER;");

    if (!publish_upgrade1.Execute() ||
        !publish_upgrade2.Execute() ||
        !publish_upgrade3.Execute() ||
        !publish_upgrade4.Execute() ||
        !publish_upgrade5.Execute() ||
        !publish_upgrade6.Execute() ||
        !publish_upgrade7.Execute() ||
        !publish_upgrade8.Execute() ||
        !publish_upgrade9.Execute()) {
      LogCvmfs(kLogCvmfs, kLogSyslogErr, "failed to upgrade publish_statistics"
               " table of statistics database");
      return false;
    }

    sqlite::Sql gc_upgrade1(this->sqlite_db(), "ALTER TABLE gc_statistics"
      " RENAME COLUMN finished_time TO finish_time;");

    if (!gc_upgrade1.Execute()) {
      LogCvmfs(kLogCvmfs, kLogSyslogErr, "failed to upgrade gc_statistics"
               " table of statistics database");
      return false;
    }

    set_schema_revision(2);
    if (!StoreSchemaRevision()) {
      LogCvmfs(kLogCvmfs, kLogSyslogErr, "failed to upgrade schema revision"
               " of statistics database");
      return false;
    }
  }

  return true;
}


bool StatisticsDatabase::CompactDatabase() const {
  ++compact_calls;
  return !compacting_fails;
}


StatisticsDatabase::~StatisticsDatabase() {
  --StatisticsDatabase::instances;
}


int StatisticsDatabase::StoreStatistics(const perf::Statistics *statistics,
                                        const std::string &start_time,
                                        const std::string &finish_time,
                                        const std::string &command_name,
                                        const std::string &repo_name) {
  std::string insert_statement;
  if (command_name == "ingest" || command_name == "sync") {
    insert_statement = PrepareStatementIntoPublish(statistics, start_time,
                                                               finish_time);
  } else if (command_name == "gc") {
    insert_statement = PrepareStatementIntoGc(statistics, start_time,
                                              finish_time, repo_name);
  } else {
    return -5;
  }

  sqlite::Sql insert(this->sqlite_db(), insert_statement);

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


std::string StatisticsDatabase::GetDBPath(const std::string &repo_name) {
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
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog,
             "Parameter %s was not set in the repository configuration file. "
             "Using default value: %s",
             "CVMFS_STATISTICS_DB", db_default_path.c_str());
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


/**
  * Check if the CVMFS_EXTENDED_GC_STATS is ON or not
  *
  * @param repo_name fully qualified name of the repository
  * @return true if CVMFS_EXTENDED_GC_STATS is ON
  */
bool StatisticsDatabase::GcExtendedStats(const std::string &repo_name) {
  SimpleOptionsParser parser;
  std::string param_value = "";
  const std::string repo_config_file =
      "/etc/cvmfs/repositories.d/" + repo_name + "/server.conf";

  if (!parser.TryParsePath(repo_config_file)) {
    LogCvmfs(kLogCvmfs, kLogSyslogErr,
             "Could not parse repository configuration: %s.",
             repo_config_file.c_str());
    return false;
  }
  if (!parser.GetValue("CVMFS_EXTENDED_GC_STATS", &param_value)) {
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog,
             "Parameter %s was not set in the repository configuration file. "
             "condemned_bytes were not counted.",
             "CVMFS_EXTENDED_GC_STATS");
  } else if (parser.IsOn(param_value)) {
    return true;
  }
  return false;
}


StatisticsDatabase::StatisticsDatabase(const std::string  &filename,
              const OpenMode      open_mode) :
  sqlite::Database<StatisticsDatabase>(filename, open_mode),
  create_empty_db_calls(0),  check_compatibility_calls(0),
  live_upgrade_calls(0), compact_calls(0)
{
  ++StatisticsDatabase::instances;
}
