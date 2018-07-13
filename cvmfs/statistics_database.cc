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
