#include "WritableCatalog.h"

#include <iostream>

using namespace std;

namespace cvmfs {
  
bool WritableCatalog::CreateNewCatalogDatabase(const std::string &file_path,
                                               const DirectoryEntry &root_entry,
                                               const std::string &root_entry_parent_path) {
  // create database schema for new catalog
  if (not WritableCatalog::CreateNewDatabaseSchema(file_path)) {
    pmesg(D_CATALOG, "failed to create database schema for new catalog '%s'", file_path.c_str());
    return false;
  }
  
  // open the new catalog to insert the root entry
  WritableCatalog *new_catalog = new WritableCatalog(root_entry_parent_path, NULL);
  if (not new_catalog->OpenDatabase(file_path)) {
    pmesg(D_CATALOG, "opening new catalog '%s' for the first time failed.", file_path.c_str());
    return false;
  }
  
  // configure the root entry
  // (if root_entry_parent_path == "" we assume it to be the ACTUAL root entry of the file system)
  hash::t_md5 path_hash;
  hash::t_md5 parent_hash;
  if (root_entry_parent_path == "") {
    path_hash = hash::t_md5(root_entry.name());
    parent_hash = hash::t_md5();
  } else {
    path_hash = hash::t_md5(root_entry_parent_path + "/" + root_entry.name());
    parent_hash = hash::t_md5(root_entry_parent_path);
  }
  
  // add the root entry to the new catalog
  if (not new_catalog->AddEntry(root_entry, path_hash, parent_hash)) {
    pmesg(D_CATALOG, "inserting root entry in new catalog '%s' failed", file_path.c_str());
    return false;
  }
  
  // close the newly created catalog
  delete new_catalog;
  
  return true;
}

bool WritableCatalog::CreateNewDatabaseSchema(const std::string &file_path) {
  sqlite3 *database;
  int open_flags = SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
  
  // create the new catalog file and open it
  pmesg(D_CATALOG, "creating new catalog at '%s'", file_path.c_str());
  if (SQLITE_OK != sqlite3_open_v2(file_path.c_str(), &database, open_flags, NULL)) {
    pmesg(D_CATALOG, "Cannot create and open catalog database file '%s'", file_path.c_str());
    return false;
  }
  sqlite3_extended_result_codes(database, 1);
  
  // TODO: I'm not really sure if this is the right spot to 'hide' the database schema
  //       maybe this should reside in catalog_queries
  
  // prepare schema creation queries
  if (not
    SqlStatement(database, 
                "CREATE TABLE IF NOT EXISTS catalog "
                "(md5path_1 INTEGER, md5path_2 INTEGER, parent_1 INTEGER, parent_2 INTEGER, inode INTEGER, "
                "hash BLOB, size INTEGER, mode INTEGER, mtime INTEGER, flags INTEGER, name TEXT, symlink TEXT, "
                "CONSTRAINT pk_catalog PRIMARY KEY (md5path_1, md5path_2));").Execute()) goto out_fail;
                
  if (not
    SqlStatement(database,
                "CREATE INDEX IF NOT EXISTS idx_catalog_parent "
                "ON catalog (parent_1, parent_2);").Execute()) goto out_fail;
  
  // TODO: wonder if this is actually still needed... inode field now holds stuff, not worth indexing
  if (not   
    SqlStatement(database,
                "CREATE INDEX IF NOT EXISTS idx_catalog_inode "
                "ON catalog (inode);").Execute()) goto out_fail;
                
  if (not
    SqlStatement(database,
                "CREATE TABLE IF NOT EXISTS properties "
                "(key TEXT, value TEXT, CONSTRAINT pk_properties PRIMARY KEY (key));").Execute()) goto out_fail;
                
  if (not
    SqlStatement(database,
                "CREATE TABLE IF NOT EXISTS nested_catalogs "
                "(path TEXT, sha1 TEXT, CONSTRAINT pk_nested_catalogs PRIMARY KEY (path));").Execute()) goto out_fail;
  
  if (not
    SqlStatement(database,
                "INSERT OR IGNORE INTO properties (key, value) VALUES ('revision', 0);").Execute()) goto out_fail;
  
  if (not
    SqlStatement(database,
                "INSERT OR REPLACE INTO properties (key, value) VALUES ('schema', '1.2');").Execute()) goto out_fail;

  sqlite3_close(database);
  return true;
  
out_fail:
  sqlite3_close(database);
  return false;
}

WritableCatalog::WritableCatalog(const string &path, Catalog *parent) :
  Catalog(path, parent)
{}

WritableCatalog::~WritableCatalog() {
  // CAUTION HOT!
  // (see Catalog.h - near the definition of FinalizePreparedStatements)
  FinalizePreparedStatements();
}

void WritableCatalog::InitPreparedStatements() {
  Catalog::InitPreparedStatements(); // polymorphism: up call
  
  insert_statement_                = new InsertDirectoryEntrySqlStatement(database());
  touch_statement_                 = new TouchSqlStatement(database());
  unlink_statement_                = new UnlinkSqlStatement(database());
  max_hardlink_group_id_statement_ = new GetMaximalHardlinkGroupIdStatement(database());
}

void WritableCatalog::FinalizePreparedStatements() {
  // no polymorphism: no up call (see Catalog.h - near the definition of this method)
  
  delete insert_statement_;
  delete touch_statement_;
  delete unlink_statement_;
  delete max_hardlink_group_id_statement_;
}

int WritableCatalog::GetMaximalHardlinkGroupId() const {
  LOCKED_SCOPE;
  int result = -1;
  
  if (max_hardlink_group_id_statement_->FetchRow()) {
    result = max_hardlink_group_id_statement_->GetMaximalGroupId();
  }
  max_hardlink_group_id_statement_->Reset();
  
  return result;
}

bool WritableCatalog::CheckForExistanceAndAddEntry(const DirectoryEntry &entry, 
                                                   const string &entry_path, 
                                                   const string &parent_path) {
  // check if entry already exists
  hash::t_md5 path_hash(entry_path);
  if (Lookup(path_hash)) {
    pmesg(D_CATALOG, "entry '%s' exists and thus cannot be created", entry_path.c_str());
    return false;
  }
  
  // add the entry to the catalog
  hash::t_md5 parent_hash(parent_path);
  if (not AddEntry(entry, path_hash, parent_hash)) {
    pmesg(D_CATALOG, "something went wrong while inserting new entry '%s'", entry_path.c_str());
    return false;
  }
  
  return true;
}

bool WritableCatalog::AddEntry(const DirectoryEntry &entry, 
                               const hash::t_md5 &path_hash, 
                               const hash::t_md5 &parent_hash) {
  LOCKED_SCOPE;
  SetDirty();
  
  // perform a add operation for the given directory entry
  bool result = (
    insert_statement_->BindPathHash(path_hash) &&
    insert_statement_->BindParentPathHash(parent_hash) &&
    insert_statement_->BindDirectoryEntry(entry) &&
    insert_statement_->Execute()
  );
  
  insert_statement_->Reset();
  
  return result;
}

bool WritableCatalog::TouchEntry(const string &entry_path, const time_t timestamp) {
  LOCKED_SCOPE;
  SetDirty();

  // perform a touch operation for the given path
  hash::t_md5 path_hash(entry_path);
  bool result = (
    touch_statement_->BindPathHash(path_hash) &&
    touch_statement_->BindTimestamp(timestamp) &&
    touch_statement_->Execute()
  );
  touch_statement_->Reset();

  return result;
}

bool WritableCatalog::RemoveEntry(const string &file_path) {
  LOCKED_SCOPE;
  SetDirty();
  
  // perform a delete operation for the given path
  hash::t_md5 path_hash(file_path);
  bool result = (
    unlink_statement_->BindPathHash(path_hash) &&
    unlink_statement_->Execute()
  );
  
  unlink_statement_->Reset();
  
  return result;
}

}
