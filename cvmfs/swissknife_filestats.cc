/**
 * This file is part of the CernVM File System
 */

#include "swissknife_filestats.h"

#include <cassert>

#include "crypto/hash.h"
#include "util/logging.h"
#include "util/posix.h"
#include "util/string.h"

using namespace std;  // NOLINT

namespace swissknife {

ParameterList CommandFileStats::GetParams() const {
  ParameterList r;
  r.push_back(Parameter::Mandatory(
              'r', "repository URL (absolute local path or remote URL)"));
  r.push_back(Parameter::Mandatory('o', "output database file"));
  r.push_back(Parameter::Optional('n', "fully qualified repository name"));
  r.push_back(Parameter::Optional('k', "repository master key(s) / dir"));
  r.push_back(Parameter::Optional('l', "temporary directory"));
  r.push_back(Parameter::Optional('h', "root hash (other than trunk)"));
  r.push_back(Parameter::Optional('@', "proxy url"));
  return r;
}

int CommandFileStats::Main(const ArgumentList &args) {
  shash::Any manual_root_hash;
  const std::string &repo_url  = *args.find('r')->second;
  db_path_ = *args.find('o')->second;
  const std::string &repo_name =
    (args.count('n') > 0) ? *args.find('n')->second : "";
  std::string repo_keys =
    (args.count('k') > 0) ? *args.find('k')->second : "";
  if (DirectoryExists(repo_keys))
    repo_keys = JoinStrings(FindFilesBySuffix(repo_keys, ".pub"), ":");
  const std::string &tmp_dir   =
    (args.count('l') > 0) ? *args.find('l')->second : "/tmp";
  if (args.count('h') > 0) {
    manual_root_hash = shash::MkFromHexPtr(shash::HexPtr(
      *args.find('h')->second), shash::kSuffixCatalog);
  }

  tmp_db_path_ = tmp_dir + "/cvmfs_filestats/";
  atomic_init32(&num_downloaded_);

  bool success = false;
  if (IsHttpUrl(repo_url)) {
    const bool follow_redirects = false;
    const string proxy = (args.count('@') > 0) ? *args.find('@')->second : "";
    if (!this->InitDownloadManager(follow_redirects, proxy) ||
        !this->InitSignatureManager(repo_keys)) {
      LogCvmfs(kLogCatalog, kLogStderr, "Failed to init remote connection");
      return 1;
    }

    HttpObjectFetcher<catalog::Catalog,
                      history::SqliteHistory> fetcher(repo_name,
                                                      repo_url,
                                                      tmp_dir,
                                                      download_manager(),
                                                      signature_manager());
    success = Run(&fetcher);
  } else {
    LocalObjectFetcher<> fetcher(repo_url, tmp_dir);
    success = Run(&fetcher);
  }

  return (success) ? 0 : 1;
}

template <class ObjectFetcherT>
bool CommandFileStats::Run(ObjectFetcherT *object_fetcher)
{
  atomic_init32(&finished_);

  string abs_path = GetAbsolutePath(db_path_);
  unlink(abs_path.c_str());
  db_ = FileStatsDatabase::Create(db_path_);
  db_->InitStatements();

  assert(MkdirDeep(tmp_db_path_, 0755));

  typename CatalogTraversal<ObjectFetcherT>::Parameters params;
  params.object_fetcher = object_fetcher;
  CatalogTraversal<ObjectFetcherT> traversal(params);
  traversal.RegisterListener(&CommandFileStats::CatalogCallback, this);

  pthread_create(&thread_processing_, NULL, MainProcessing, this);

  bool ret = traversal.Traverse();

  atomic_inc32(&finished_);
  pthread_join(thread_processing_, NULL);

  db_->DestroyStatements();

  return ret;
}

void CommandFileStats::CatalogCallback(
  const CatalogTraversalData<catalog::Catalog> &data) {
  int32_t num = atomic_read32(&num_downloaded_);
  string out_path =  tmp_db_path_ + StringifyInt(num + 1) + ".db";
  assert(CopyPath2Path(data.catalog->database_path(), out_path));
  atomic_inc32(&num_downloaded_);
}

void *CommandFileStats::MainProcessing(void *data) {
  CommandFileStats *repo_stats = static_cast<CommandFileStats *>(data);
  int processed = 0;
  int32_t downloaded = atomic_read32(&repo_stats->num_downloaded_);
  int32_t fin = atomic_read32(&repo_stats->finished_);

  repo_stats->db_->BeginTransaction();
  while (fin == 0 || processed < downloaded) {
    if (processed < downloaded) {
      LogCvmfs(kLogCatalog, kLogStdout, "Processing catalog %d", processed);
      string db_path = repo_stats->tmp_db_path_ + "/" +
                       StringifyInt(processed + 1) + ".db";
      repo_stats->ProcessCatalog(db_path);
      ++processed;
    }
    downloaded = atomic_read32(&repo_stats->num_downloaded_);
    fin = atomic_read32(&repo_stats->finished_);
  }
  repo_stats->db_->CommitTransaction();

  return NULL;
}



void CommandFileStats::ProcessCatalog(string db_path) {
  sqlite::Database<catalog::CatalogDatabase> *cat_db;
  cat_db = sqlite::Database<catalog::CatalogDatabase>::Open(
           db_path,
           sqlite::Database<catalog::CatalogDatabase>::kOpenReadOnly);
  cat_db->TakeFileOwnership();

  int64_t file_size = GetFileSize(db_path);
  sqlite::Sql *catalog_count = new sqlite::Sql(cat_db->sqlite_db(),
                                               "SELECT count(*) FROM catalog;");
  catalog_count->Execute();
  int cur_catalog_id = db_->StoreCatalog(catalog_count->RetrieveInt64(0),
                                         file_size);
  delete catalog_count;

  sqlite::Sql *catalog_list =
    new sqlite::Sql(cat_db->sqlite_db(),
                    "SELECT hash, size, flags, symlink FROM catalog;");
  sqlite::Sql *chunks_list =
    new sqlite::Sql(cat_db->sqlite_db(),
                    "SELECT md5path_1, md5path_2, size, hash FROM chunks "
                    "ORDER BY md5path_1 ASC, md5path_2 ASC;");

  while (catalog_list->FetchRow()) {
    const void *hash = catalog_list->RetrieveBlob(0);
    int num_bytes = catalog_list->RetrieveBytes(0);
    int64_t size = catalog_list->RetrieveInt64(1);
    int flags = catalog_list->RetrieveInt(2);
    if ((flags & catalog::SqlDirent::kFlagLink) ==
                catalog::SqlDirent::kFlagLink) {
      int symlink_length = catalog_list->RetrieveBytes(3);
      db_->StoreSymlink(symlink_length);
    } else if ((flags & catalog::SqlDirent::kFlagFile) ==
               catalog::SqlDirent::kFlagFile)
    {
      if ((flags & catalog::SqlDirent::kFlagFileChunk) !=
           catalog::SqlDirent::kFlagFileChunk)
      {
        int object_id = db_->StoreObject(hash, num_bytes, size);
        db_->StoreFile(cur_catalog_id, object_id);
      } else {
        // Bulk hashes in addition to chunks
        if (hash != NULL)
          db_->StoreObject(hash, num_bytes, size);
      }
    }
  }

  int old_md5path_1 = 0, old_md5path_2 = 0;
  int md5path_1 = 0, md5path_2 = 0;
  int cur_file_id = 0;
  while (chunks_list->FetchRow()) {
    md5path_1 = chunks_list->RetrieveInt(0);
    md5path_2 = chunks_list->RetrieveInt(1);
    if (md5path_1 != old_md5path_1 || md5path_2 != old_md5path_2) {
      cur_file_id = db_->StoreChunkedFile(cur_catalog_id);
    }
    const void *hash = chunks_list->RetrieveBlob(3);
    int num_bytes = chunks_list->RetrieveBytes(3);
    int64_t size = chunks_list->RetrieveInt64(2);
    db_->StoreChunk(hash, num_bytes, size, cur_file_id);
    old_md5path_1 = md5path_1;
    old_md5path_2 = md5path_2;
  }

  delete catalog_list;
  delete chunks_list;
  delete cat_db;
}

float FileStatsDatabase::kLatestSchema = 1;
unsigned FileStatsDatabase::kLatestSchemaRevision = 1;

bool FileStatsDatabase::CreateEmptyDatabase() {
  bool ret = true;
  ret &= sqlite::Sql(sqlite_db(),
    "CREATE TABLE catalogs ("
    "catalog_id INTEGER PRIMARY KEY,"
    "num_entries INTEGER,"
    "file_size INTEGER"
    ");").Execute();
  ret &= sqlite::Sql(sqlite_db(),
    "CREATE TABLE objects ("
    "object_id INTEGER PRIMARY KEY,"
    "hash BLOB,"
    "size INTEGER"
    ");").Execute();
  ret &= sqlite::Sql(sqlite_db(),
    "CREATE INDEX idx_object_hash "
    "ON objects (hash);").Execute();
  ret &= sqlite::Sql(sqlite_db(),
    "CREATE TABLE files ("
    "file_id INTEGER PRIMARY KEY,"
    "catalog_id INTEGER,"
    "FOREIGN KEY (catalog_id) REFERENCES catalogs (catalog_id)"
    ");").Execute();
  ret &= sqlite::Sql(sqlite_db(),
    "CREATE TABLE files_objects ("
    "file_id INTEGER,"
    "object_id INTEGER,"
    "FOREIGN KEY (file_id) REFERENCES files (file_id),"
    "FOREIGN KEY (object_id) REFERENCES objects (object_id));").Execute();
  ret &= sqlite::Sql(sqlite_db(),
    "CREATE INDEX idx_file_id ON files_objects (file_id);").Execute();
  ret &= sqlite::Sql(sqlite_db(),
    "CREATE INDEX idx_object_id ON files_objects (object_id);").Execute();
  ret &= sqlite::Sql(sqlite_db(),
    "CREATE TABLE symlinks ("
    "length INTEGER);").Execute();
  return ret;
}

void FileStatsDatabase::InitStatements() {
  query_insert_catalog = new sqlite::Sql(sqlite_db(),
    "INSERT INTO catalogs (num_entries, file_size) VALUES (:num, :size);");
  query_insert_object = new sqlite::Sql(sqlite_db(),
    "INSERT INTO objects (hash, size) VALUES (:hash, :size);");
  query_insert_file = new sqlite::Sql(sqlite_db(),
    "INSERT INTO files (catalog_id) VALUES (:catalog);");
  query_insert_file_object = new sqlite::Sql(sqlite_db(),
    "INSERT INTO files_objects (file_id, object_id) VALUES (:file, :object);");
  query_insert_symlink = new sqlite::Sql(sqlite_db(),
    "INSERT INTO symlinks (length) VALUES(:length);");
  query_lookup_object = new sqlite::Sql(sqlite_db(),
    "SELECT object_id FROM objects WHERE hash = :hash;");
}

void FileStatsDatabase::DestroyStatements() {
  delete query_insert_catalog;
  delete query_insert_object;
  delete query_insert_file;
  delete query_insert_file_object;
  delete query_insert_symlink;
  delete query_lookup_object;
}

int64_t FileStatsDatabase::StoreCatalog(int64_t num_entries,
                                        int64_t file_size) {
  query_insert_catalog->Reset();
  query_insert_catalog->BindInt64(1, num_entries);
  query_insert_catalog->BindInt64(2, file_size);
  query_insert_catalog->Execute();
  return sqlite3_last_insert_rowid(sqlite_db());
}

int64_t FileStatsDatabase::StoreFile(int64_t catalog_id, int64_t object_id) {
  query_insert_file->Reset();
  query_insert_file->BindInt64(1, catalog_id);
  query_insert_file->Execute();
  int file_id = sqlite3_last_insert_rowid(sqlite_db());

  query_insert_file_object->Reset();
  query_insert_file_object->BindInt64(1, file_id);
  query_insert_file_object->BindInt64(2, object_id);
  query_insert_file_object->Execute();
  return file_id;
}

int64_t FileStatsDatabase::StoreChunkedFile(int64_t catalog_id) {
  query_insert_file->Reset();
  query_insert_file->BindInt64(1, catalog_id);
  query_insert_file->Execute();
  return sqlite3_last_insert_rowid(sqlite_db());
}

int64_t FileStatsDatabase::StoreChunk(const void *hash, int hash_size,
                                  int64_t size, int64_t file_id) {
  int object_id = StoreObject(hash, hash_size, size);

  query_insert_file_object->Reset();
  query_insert_file_object->BindInt64(1, file_id);
  query_insert_file_object->BindInt64(2, object_id);
  query_insert_file_object->Execute();
  return sqlite3_last_insert_rowid(sqlite_db());
}

int64_t FileStatsDatabase::StoreObject(const void *hash, int hash_size,
                                   int64_t size) {
  query_lookup_object->Reset();
  query_lookup_object->BindBlob(1, hash, hash_size);
  if (query_lookup_object->FetchRow()) {
    return query_lookup_object->RetrieveInt(0);
  } else {
    query_insert_object->Reset();
    query_insert_object->BindBlob(1, hash, hash_size);
    query_insert_object->BindInt64(2, size);
    query_insert_object->Execute();
    return sqlite3_last_insert_rowid(sqlite_db());
  }
}

int64_t FileStatsDatabase::StoreSymlink(int64_t length) {
  query_insert_symlink->Reset();
  query_insert_symlink->BindInt64(1, length);
  query_insert_symlink->Execute();
  return sqlite3_last_insert_rowid(sqlite_db());
}

}  // namespace swissknife
