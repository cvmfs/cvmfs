/**
 * This file is part of the CernVM File System.
 */


#include "pipeline.h"

#include <algorithm>
#include <cstdlib>

#include "ingestion/task_chunk.h"
#include "ingestion/task_compress.h"
#include "ingestion/task_hash.h"
#include "ingestion/task_read.h"
#include "ingestion/task_register.h"
#include "ingestion/task_write.h"
#include "upload_facility.h"
#include "upload_spooler_definition.h"
#include "util/concurrency.h"
#include "util/exception.h"
#include "util/platform.h"
#include "util/string.h"

const uint64_t IngestionPipeline::kMaxPipelineMem = 1024 * 1024 * 1024;

IngestionPipeline::IngestionPipeline(
  upload::AbstractUploader *uploader,
  const upload::SpoolerDefinition &spooler_definition)
  : compression_algorithm_(spooler_definition.compression_alg)
  , hash_algorithm_(spooler_definition.hash_algorithm)
  , generate_legacy_bulk_chunks_(spooler_definition.generate_legacy_bulk_chunks)
  , chunking_enabled_(spooler_definition.use_file_chunking)
  , minimal_chunk_size_(spooler_definition.min_file_chunk_size)
  , average_chunk_size_(spooler_definition.avg_file_chunk_size)
  , maximal_chunk_size_(spooler_definition.max_file_chunk_size)
  , spawned_(false)
  , uploader_(uploader)
  , tube_ctr_inflight_pre_(kMaxFilesInFlight)
{
  unsigned nfork_base = std::max(1U, GetNumberOfCpuCores() / 8);

  for (unsigned i = 0; i < nfork_base * kNforkRegister; ++i) {
    Tube<FileItem> *tube = new Tube<FileItem>();
    tubes_register_.TakeTube(tube);
    TaskRegister *task = new TaskRegister(tube,
      &tube_ctr_inflight_pre_, &tube_ctr_inflight_post_);
    task->RegisterListener(&IngestionPipeline::OnFileProcessed, this);
    tasks_register_.TakeConsumer(task);
  }
  tubes_register_.Activate();

  for (unsigned i = 0; i < nfork_base * kNforkWrite; ++i) {
    Tube<BlockItem> *t = new Tube<BlockItem>();
    tubes_write_.TakeTube(t);
    tasks_write_.TakeConsumer(new TaskWrite(t, &tubes_register_, uploader_));
  }
  tubes_write_.Activate();

  for (unsigned i = 0; i < nfork_base * kNforkHash; ++i) {
    Tube<BlockItem> *t = new Tube<BlockItem>();
    tubes_hash_.TakeTube(t);
    tasks_hash_.TakeConsumer(new TaskHash(t, &tubes_write_));
  }
  tubes_hash_.Activate();

  for (unsigned i = 0; i < nfork_base * kNforkCompress; ++i) {
    Tube<BlockItem> *t = new Tube<BlockItem>();
    tubes_compress_.TakeTube(t);
    tasks_compress_.TakeConsumer(
      new TaskCompress(t, &tubes_hash_, &item_allocator_));
  }
  tubes_compress_.Activate();

  for (unsigned i = 0; i < nfork_base * kNforkChunk; ++i) {
    Tube<BlockItem> *t = new Tube<BlockItem>();
    tubes_chunk_.TakeTube(t);
    tasks_chunk_.TakeConsumer(
      new TaskChunk(t, &tubes_compress_, &item_allocator_));
  }
  tubes_chunk_.Activate();

  uint64_t high = kMaxPipelineMem;
  high = std::min(high, platform_memsize() / 5);
  char *fixed_limit_mb = getenv("_CVMFS_SERVER_PIPELINE_MB");
  if (fixed_limit_mb != NULL) {
    high = String2Uint64(fixed_limit_mb) * 1024 * 1024;
  }
  uint64_t low = (high * 2) / 3;
  LogCvmfs(kLogCvmfs, kLogDebug,
           "pipeline memory thresholds %" PRIu64 "/%" PRIu64 " M",
           low / (1024 * 1024), high / (1024 * 1024));
  for (unsigned i = 0; i < nfork_base * kNforkRead; ++i) {
    TaskRead *task_read =
      new TaskRead(&tube_input_, &tubes_chunk_, &item_allocator_);
    task_read->SetWatermarks(low, high);
    tasks_read_.TakeConsumer(task_read);
  }
}


IngestionPipeline::~IngestionPipeline() {
  if (spawned_) {
    tasks_read_.Terminate();
    tasks_chunk_.Terminate();
    tasks_compress_.Terminate();
    tasks_hash_.Terminate();
    tasks_write_.Terminate();
    tasks_register_.Terminate();
  }
}


void IngestionPipeline::OnFileProcessed(
  const upload::SpoolerResult &spooler_result)
{
  NotifyListeners(spooler_result);
}


void IngestionPipeline::Process(
  IngestionSource* source,
  bool allow_chunking,
  shash::Suffix hash_suffix)
{
  FileItem *file_item = new FileItem(
    source,
    minimal_chunk_size_,
    average_chunk_size_,
    maximal_chunk_size_,
    compression_algorithm_,
    hash_algorithm_,
    hash_suffix,
    allow_chunking && chunking_enabled_,
    generate_legacy_bulk_chunks_);
  tube_ctr_inflight_post_.EnqueueBack(file_item);
  tube_ctr_inflight_pre_.EnqueueBack(file_item);
  tube_input_.EnqueueBack(file_item);
}


void IngestionPipeline::Spawn() {
  tasks_register_.Spawn();
  tasks_write_.Spawn();
  tasks_hash_.Spawn();
  tasks_compress_.Spawn();
  tasks_chunk_.Spawn();
  tasks_read_.Spawn();
  spawned_ = true;
}


void IngestionPipeline::WaitFor() {
  tube_ctr_inflight_post_.Wait();
}


//------------------------------------------------------------------------------


void TaskScrubbingCallback::Process(BlockItem *block_item) {
  FileItem *file_item = block_item->file_item();
  assert(file_item != NULL);
  assert(!file_item->path().empty());
  ChunkItem *chunk_item = block_item->chunk_item();
  assert(chunk_item != NULL);
  assert(chunk_item->is_bulk_chunk());

  switch (block_item->type()) {
    case BlockItem::kBlockData:
      delete block_item;
      break;

    case BlockItem::kBlockStop:
      assert(!chunk_item->hash_ptr()->IsNull());
      NotifyListeners(ScrubbingResult(file_item->path(),
                                      *chunk_item->hash_ptr()));
      delete block_item;
      delete chunk_item;
      delete file_item;
      tube_counter_->PopFront();
      break;

    default:
      PANIC(NULL);
  }
}


//------------------------------------------------------------------------------


ScrubbingPipeline::ScrubbingPipeline()
  : spawned_(false)
  , tube_counter_(kMaxFilesInFlight)
{
  unsigned nfork_base = std::max(1U, GetNumberOfCpuCores() / 8);

  for (unsigned i = 0; i < nfork_base * kNforkScrubbingCallback; ++i) {
    Tube<BlockItem> *tube = new Tube<BlockItem>();
    tubes_scrubbing_callback_.TakeTube(tube);
    TaskScrubbingCallback *task =
      new TaskScrubbingCallback(tube, &tube_counter_);
    task->RegisterListener(&ScrubbingPipeline::OnFileProcessed, this);
    tasks_scrubbing_callback_.TakeConsumer(task);
  }
  tubes_scrubbing_callback_.Activate();

  for (unsigned i = 0; i < nfork_base * kNforkHash; ++i) {
    Tube<BlockItem> *t = new Tube<BlockItem>();
    tubes_hash_.TakeTube(t);
    tasks_hash_.TakeConsumer(new TaskHash(t, &tubes_scrubbing_callback_));
  }
  tubes_hash_.Activate();

  for (unsigned i = 0; i < nfork_base * kNforkChunk; ++i) {
    Tube<BlockItem> *t = new Tube<BlockItem>();
    tubes_chunk_.TakeTube(t);
    tasks_chunk_.TakeConsumer(
      new TaskChunk(t, &tubes_hash_, &item_allocator_));
  }
  tubes_chunk_.Activate();

  for (unsigned i = 0; i < nfork_base * kNforkRead; ++i) {
    TaskRead *task_read =
      new TaskRead(&tube_input_, &tubes_chunk_, &item_allocator_);
    task_read->SetWatermarks(kMemLowWatermark, kMemHighWatermark);
    tasks_read_.TakeConsumer(task_read);
  }
}


ScrubbingPipeline::~ScrubbingPipeline() {
  if (spawned_) {
    tasks_read_.Terminate();
    tasks_chunk_.Terminate();
    tasks_hash_.Terminate();
    tasks_scrubbing_callback_.Terminate();
  }
}


void ScrubbingPipeline::OnFileProcessed(
  const ScrubbingResult &scrubbing_result)
{
  NotifyListeners(scrubbing_result);
}


void ScrubbingPipeline::Process(
  IngestionSource *source,
  shash::Algorithms hash_algorithm,
  shash::Suffix hash_suffix)
{
  FileItem *file_item = new FileItem(
    source,
    0, 0, 0,
    zlib::kNoCompression,
    hash_algorithm,
    hash_suffix,
    false,  /* may_have_chunks */
    true  /* hash_legacy_bulk_chunk */);
  tube_counter_.EnqueueBack(file_item);
  tube_input_.EnqueueBack(file_item);
}


void ScrubbingPipeline::Spawn() {
  tasks_scrubbing_callback_.Spawn();
  tasks_hash_.Spawn();
  tasks_chunk_.Spawn();
  tasks_read_.Spawn();
  spawned_ = true;
}


void ScrubbingPipeline::WaitFor() {
  tube_counter_.Wait();
}
