#include "catalog_downloader.h"

int kCatalogDownloadMultiplier = 32;

CatalogItem::CatalogItem(const shash::Any &hash) : hash_(hash) { }

void TaskCatalogDownload::Process(CatalogItem *input) {
  std::string const catalog_path;
  shash::Any const catalog_hash;
  // will PANIC if download failed
  // catalog_mgr_->LoadCatalog(PathString("") /* not used */, *input->GetHash(),
  // &catalog_path, &catalog_hash, NULL);
  catalog::CatalogContext context(*input->GetHash(), PathString(catalog_path));
  catalog_mgr_->LoadCatalogByHash(&context);
  NotifyListeners(
      CatalogDownloadResult(catalog_path, input->GetHash()->ToString()));
  tube_counter_->PopFront();  // pop after calling callback as callback could
                              // enqueue additional items
}

CatalogDownloadPipeline::CatalogDownloadPipeline(
    catalog::SimpleCatalogManager *catalog_mgr)
    : spawned_(false), catalog_mgr_(catalog_mgr) {
  const unsigned int nfork_base = 1;

  // spawn a bit more workers as this is just download task
  for (unsigned i = 0; i < nfork_base * kCatalogDownloadMultiplier; ++i) {
    TaskCatalogDownload *task = new TaskCatalogDownload(
        catalog_mgr_, &tube_input_, &tube_counter_);
    task->RegisterListener(&CatalogDownloadPipeline::OnFileProcessed, this);
    tasks_download_.TakeConsumer(task);
  }
}

CatalogDownloadPipeline::~CatalogDownloadPipeline() {
  if (spawned_) {
    tasks_download_.Terminate();
  }
}

void CatalogDownloadPipeline::OnFileProcessed(
    const CatalogDownloadResult &catalog_download_result) {
  NotifyListeners(catalog_download_result);
}

void CatalogDownloadPipeline::Process(const shash::Any &catalog_hash) {
  CatalogItem *catalog_item = new CatalogItem(catalog_hash);
  tube_counter_.EnqueueBack(catalog_item);
  tube_input_.EnqueueBack(catalog_item);
}

void CatalogDownloadPipeline::Spawn() {
  tasks_download_.Spawn();
  spawned_ = true;
}

void CatalogDownloadPipeline::WaitFor() { tube_counter_.Wait(); }
