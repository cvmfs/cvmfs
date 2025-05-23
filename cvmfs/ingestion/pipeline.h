/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_INGESTION_PIPELINE_H_
#define CVMFS_INGESTION_PIPELINE_H_

#include <string>

#include "compression/compression.h"
#include "crypto/hash.h"
#include "ingestion/item.h"
#include "ingestion/item_mem.h"
#include "ingestion/task.h"
#include "upload_spooler_result.h"
#include "util/concurrency.h"
#include "util/tube.h"

namespace upload {
class AbstractUploader;
struct SpoolerDefinition;
}  // namespace upload

class IngestionPipeline : public Observable<upload::SpoolerResult> {
 public:
  explicit IngestionPipeline(
      upload::AbstractUploader *uploader,
      const upload::SpoolerDefinition &spooler_definition);
  ~IngestionPipeline();

  void Spawn();
  void Process(IngestionSource *source, bool allow_chunking,
               shash::Suffix hash_suffix = shash::kSuffixNone);
  void WaitFor();

  void OnFileProcessed(const upload::SpoolerResult &spooler_result);

 private:
  static const uint64_t kMaxPipelineMem;  // 1G
  static const unsigned kMaxFilesInFlight = 8000;
  static const unsigned kNforkRegister = 1;
  static const unsigned kNforkWrite = 1;
  static const unsigned kNforkHash = 2;
  static const unsigned kNforkCompress = 4;
  static const unsigned kNforkChunk = 1;
  static const unsigned kNforkRead = 8;

  const zlib::Algorithms compression_algorithm_;
  const shash::Algorithms hash_algorithm_;
  const bool generate_legacy_bulk_chunks_;
  const bool chunking_enabled_;
  const size_t minimal_chunk_size_;
  const size_t average_chunk_size_;
  const size_t maximal_chunk_size_;

  bool spawned_;
  upload::AbstractUploader *uploader_;
  // TODO(jblomer): a semaphore would be faster!
  // We need to have two in-flight counters: the pre-counter decreases
  // before the final NotifyListeners() call, so that the callback can schedule
  // a new job into the pipeline.  This happens when finished child catalogs
  // trigger uploading the parent catalog.
  // The pre-counter sets the kMaxFilesInFlight limit.
  Tube<FileItem> tube_ctr_inflight_pre_;
  // The post counter is set after the final callback.  It is used to wait
  // for the pipeline to finish.
  Tube<FileItem> tube_ctr_inflight_post_;

  Tube<FileItem> tube_input_;

  TubeConsumerGroup<FileItem> tasks_read_;

  TubeGroup<BlockItem> tubes_chunk_;
  TubeConsumerGroup<BlockItem> tasks_chunk_;

  TubeGroup<BlockItem> tubes_compress_;
  TubeConsumerGroup<BlockItem> tasks_compress_;

  TubeGroup<BlockItem> tubes_hash_;
  TubeConsumerGroup<BlockItem> tasks_hash_;

  TubeGroup<BlockItem> tubes_write_;
  TubeConsumerGroup<BlockItem> tasks_write_;

  TubeGroup<FileItem> tubes_register_;
  TubeConsumerGroup<FileItem> tasks_register_;

  ItemAllocator item_allocator_;
};  // class IngestionPipeline


struct ScrubbingResult {
  ScrubbingResult() { }
  ScrubbingResult(const std::string &p, const shash::Any &h)
      : path(p), hash(h) { }
  std::string path;
  shash::Any hash;
};


class TaskScrubbingCallback : public TubeConsumer<BlockItem>,
                              public Observable<ScrubbingResult> {
 public:
  TaskScrubbingCallback(Tube<BlockItem> *tube_in, Tube<FileItem> *tube_counter)
      : TubeConsumer<BlockItem>(tube_in), tube_counter_(tube_counter) { }

 protected:
  virtual void Process(BlockItem *block_item);

 private:
  Tube<FileItem> *tube_counter_;
};


class ScrubbingPipeline : public Observable<ScrubbingResult> {
 public:
  ScrubbingPipeline();
  ~ScrubbingPipeline();

  void Spawn();
  void Process(IngestionSource *source,
               shash::Algorithms hash_algorithm,
               shash::Suffix hash_suffix);
  void WaitFor();

  void OnFileProcessed(const ScrubbingResult &scrubbing_result);

 private:
  static const uint64_t kMemLowWatermark = 384 * 1024 * 1024;
  static const uint64_t kMemHighWatermark = 512 * 1024 * 1024;
  static const unsigned kMaxFilesInFlight = 8000;
  static const unsigned kNforkScrubbingCallback = 1;
  static const unsigned kNforkHash = 2;
  static const unsigned kNforkChunk = 1;
  static const unsigned kNforkRead = 8;

  bool spawned_;
  Tube<FileItem> tube_input_;
  // TODO(jblomer): a semaphore would be faster!
  Tube<FileItem> tube_counter_;

  TubeConsumerGroup<FileItem> tasks_read_;

  TubeGroup<BlockItem> tubes_chunk_;
  TubeConsumerGroup<BlockItem> tasks_chunk_;

  TubeGroup<BlockItem> tubes_hash_;
  TubeConsumerGroup<BlockItem> tasks_hash_;

  TubeGroup<BlockItem> tubes_scrubbing_callback_;
  TubeConsumerGroup<BlockItem> tasks_scrubbing_callback_;

  ItemAllocator item_allocator_;
};

#endif  // CVMFS_INGESTION_PIPELINE_H_
