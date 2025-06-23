#ifndef CVMFS_CATALOG_DOWNLOADER_H_
#define CVMFS_CATALOG_DOWNLOADER_H_

#include "catalog_mgr_ro.h"
#include "crypto/hash.h"
#include "ingestion/task.h"
#include "util/concurrency.h"

extern int kCatalogDownloadMultiplier;

struct CatalogDownloadResult {
  CatalogDownloadResult() { }
  explicit CatalogDownloadResult(const std::string &p, const std::string &h)
      : db_path(p), hash(h) { }
  std::string db_path;
  std::string hash;
};

class CatalogItem : SingleCopy {
 public:
  explicit CatalogItem(const shash::Any &hash);
  static CatalogItem *CreateQuitBeacon() {
    shash::Any const empty;
    return new CatalogItem(empty);
  }
  bool IsQuitBeacon() { return hash_.IsNull(); }
  shash::Any *GetHash() { return &hash_; }

 private:
  shash::Any hash_;
};

class TaskCatalogDownload : public TubeConsumer<CatalogItem>,
                            public Observable<CatalogDownloadResult> {
 public:
  TaskCatalogDownload(catalog::SimpleCatalogManager *catalog_mgr,
                      Tube<CatalogItem> *tube_in,
                      Tube<CatalogItem> *tube_counter)
      : TubeConsumer<CatalogItem>(tube_in)
      , tube_counter_(tube_counter)
      , catalog_mgr_(catalog_mgr) { }

 protected:
  virtual void Process(CatalogItem *input_hash);

 private:
  Tube<CatalogItem> *tube_counter_;
  catalog::SimpleCatalogManager *catalog_mgr_;
};

class CatalogDownloadPipeline : public Observable<CatalogDownloadResult> {
 public:
  explicit CatalogDownloadPipeline(catalog::SimpleCatalogManager *catalog_mgr);
  ~CatalogDownloadPipeline();

  void Spawn();
  void Process(const shash::Any &catalog_hash);
  void WaitFor();

  void OnFileProcessed(const CatalogDownloadResult &catalog_download_result);

 private:
  bool spawned_;
  Tube<CatalogItem> tube_input_;
  Tube<CatalogItem> tube_counter_;

  TubeConsumerGroup<CatalogItem> tasks_download_;

  catalog::SimpleCatalogManager *catalog_mgr_;
};

#endif  // CVMFS_CATALOG_DOWNLOADER_H_
