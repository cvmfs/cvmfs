/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_FILESTATS_H_
#define CVMFS_SWISSKNIFE_FILESTATS_H_

#include <pthread.h>

#include <string>

#include "catalog_traversal.h"
#include "sql.h"
#include "swissknife.h"
#include "util/atomic.h"

using namespace std;  // NOLINT

namespace swissknife {

// Database class for storing repo statistics, the class design
// follows StatisticsDatabase's design
class FileStatsDatabase : public sqlite::Database<FileStatsDatabase> {
 public:
  bool CreateEmptyDatabase();

  // Following Store* functions return the primary key value of the
  // inserted entry
  int64_t StoreCatalog(int64_t num_entries, int64_t file_size);
  int64_t StoreFile(int64_t catalog_id, int64_t object_id);
  int64_t StoreObject(const void *hash, int hash_size, int64_t size);
  int64_t StoreChunkedFile(int64_t catalog_id);
  int64_t StoreChunk(const void *hash, int hash_size, int64_t size,
                     int64_t file_id);
  int64_t StoreSymlink(int64_t length);
  void InitStatements();
  void DestroyStatements();

  static float kLatestSchema;
  static unsigned kLatestSchemaRevision;

 protected:
  sqlite::Sql *query_insert_catalog;
  sqlite::Sql *query_insert_object;
  sqlite::Sql *query_insert_file;
  sqlite::Sql *query_insert_file_object;
  sqlite::Sql *query_insert_symlink;
  sqlite::Sql *query_lookup_object;
  friend class sqlite::Database<FileStatsDatabase>;
  FileStatsDatabase(const std::string &filename, const OpenMode open_mode)
      : sqlite::Database<FileStatsDatabase>(filename, open_mode) { }
};


class CommandFileStats : public Command {
 public:
  ~CommandFileStats() { }
  virtual std::string GetName() const { return "filestats"; }
  virtual std::string GetDescription() const {
    return "CernVM File System repository statistics exporter.";
  }
  virtual ParameterList GetParams() const;
  int Main(const ArgumentList &args);

 protected:
  string db_path_;
  string tmp_db_path_;
  FileStatsDatabase *db_;

  pthread_t thread_processing_;
  atomic_int32 num_downloaded_;
  atomic_int32 finished_;

  template<class ObjectFetcherT>
  bool Run(ObjectFetcherT *object_fetcher);

  void CatalogCallback(const CatalogTraversalData<catalog::Catalog> &data);

  static void *MainProcessing(void *data);

  void ProcessCatalog(string db_path);
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_FILESTATS_H_
