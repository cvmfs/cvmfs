/**
 * This file is part of the CernVM File System.
 */

#include <fcntl.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>

#include "c_mock_uploader.h"
#include "compression/compression.h"
#include "crypto/hash.h"
#include "gtest/gtest.h"
#include "ingestion/item.h"
#include "ingestion/item_mem.h"
#include "ingestion/pipeline.h"
#include "ingestion/task.h"
#include "ingestion/task_chunk.h"
#include "ingestion/task_compress.h"
#include "ingestion/task_hash.h"
#include "ingestion/task_read.h"
#include "ingestion/task_write.h"
#include "testutil.h"
#include "upload_facility.h"
#include "util/atomic.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/smalloc.h"

using namespace std;  // NOLINT

namespace {
class DummyItem {
 public:
  static DummyItem *CreateQuitBeacon() { return new DummyItem(-1); }
  bool IsQuitBeacon() { return summand == -1; }
  explicit DummyItem(int s) : summand(s) { }
  int summand;
  static atomic_int32 sum;
};
atomic_int32 DummyItem::sum = 0;


class TestTask : public TubeConsumer<DummyItem> {
 public:
  static atomic_int32 cnt_terminate;
  static atomic_int32 cnt_process;
  explicit TestTask(Tube<DummyItem> *tube) : TubeConsumer<DummyItem>(tube) { }

 protected:
  virtual void Process(DummyItem *item) {
    atomic_xadd32(&item->sum, item->summand);
    atomic_inc32(&cnt_process);
  }
  virtual void OnTerminate() { atomic_inc32(&cnt_terminate); }
};
atomic_int32 TestTask::cnt_terminate = 0;
atomic_int32 TestTask::cnt_process = 0;
}  // anonymous namespace


/**
 * Collect results from the pipeline, what would normally enter entries in the
 * file catalog.
 */
struct FnFileProcessed {
  FnFileProcessed() { atomic_init64(&ncall); }

  void OnFileProcessed(const upload::SpoolerResult &spooler_result) {
    atomic_inc64(&ncall);
  }

  atomic_int64 ncall;
};


struct FnFileHashed {
  FnFileHashed() { atomic_init64(&ncall); }

  void OnFileProcessed(const ScrubbingResult &scrubbing_result) {
    last_result = scrubbing_result;
    atomic_inc64(&ncall);
  }

  ScrubbingResult last_result;
  atomic_int64 ncall;
};


class T_Ingestion : public ::testing::Test {
 protected:
  static const unsigned kNumTasks = 32;

  virtual void SetUp() {
    DummyItem::sum = 0;
    TestTask::cnt_terminate = 0;
    TestTask::cnt_process = 0;
    for (unsigned i = 0; i < kNumTasks; ++i)
      task_group_.TakeConsumer(new TestTask(&tube_));
    uploader_ = IngestionMockUploader::MockConstruct();
    ASSERT_TRUE(uploader_ != NULL);
    EXPECT_EQ(0U, BlockItem::managed_bytes());
  }

  virtual void TearDown() {
    uploader_->TearDown();
    delete uploader_;
    EXPECT_EQ(0U, BlockItem::managed_bytes());
  }

  Tube<DummyItem> tube_;
  TubeConsumerGroup<DummyItem> task_group_;
  IngestionMockUploader *uploader_;
  ItemAllocator allocator_;
};


//------------------------------------------------------------------------------


TEST_F(T_Ingestion, TaskBasic) {
  DummyItem i1(1);
  DummyItem i2(2);
  DummyItem i3(3);

  task_group_.Spawn();
  EXPECT_EQ(0, atomic_read32(&TestTask::cnt_terminate));
  EXPECT_EQ(0, atomic_read32(&TestTask::cnt_process));
  EXPECT_EQ(0, atomic_read32(&DummyItem::sum));

  tube_.EnqueueBack(&i1);
  tube_.EnqueueBack(&i2);
  tube_.EnqueueBack(&i3);

  tube_.Wait();
  task_group_.Terminate();
  EXPECT_EQ(static_cast<int>(kNumTasks),
            atomic_read32(&TestTask::cnt_terminate));

  EXPECT_EQ(6, atomic_read32(&DummyItem::sum));
  EXPECT_EQ(3, atomic_read32(&TestTask::cnt_process));
}


TEST_F(T_Ingestion, TaskStress) {
  DummyItem i1(1);
  DummyItem i2(2);
  DummyItem i3(3);

  task_group_.Spawn();
  EXPECT_EQ(0, atomic_read32(&TestTask::cnt_terminate));
  EXPECT_EQ(0, atomic_read32(&TestTask::cnt_process));
  EXPECT_EQ(0, atomic_read32(&DummyItem::sum));

  for (unsigned i = 0; i < 10000; ++i) {
    tube_.EnqueueBack(&i1);
    tube_.EnqueueBack(&i2);
    tube_.EnqueueBack(&i3);
  }

  tube_.Wait();
  task_group_.Terminate();
  EXPECT_EQ(static_cast<int>(kNumTasks),
            atomic_read32(&TestTask::cnt_terminate));

  EXPECT_EQ(10000 * 6, atomic_read32(&DummyItem::sum));
  EXPECT_EQ(10000 * 3, atomic_read32(&TestTask::cnt_process));
}


TEST_F(T_Ingestion, TaskRead) {
  Tube<FileItem> tube_in;
  Tube<BlockItem> *tube_out = new Tube<BlockItem>();
  TubeGroup<BlockItem> tube_group_out;
  tube_group_out.TakeTube(tube_out);
  tube_group_out.Activate();

  TubeConsumerGroup<FileItem> task_group;
  task_group.TakeConsumer(new TaskRead(&tube_in, &tube_group_out, &allocator_));
  task_group.Spawn();

  FileItem file_null(new FileIngestionSource(std::string("/dev/null")));
  EXPECT_TRUE(file_null.may_have_chunks());
  tube_in.EnqueueBack(&file_null);
  BlockItem *item_stop = tube_out->PopFront();
  EXPECT_EQ(0U, file_null.size());
  EXPECT_FALSE(file_null.may_have_chunks());
  EXPECT_EQ(BlockItem::kBlockStop, item_stop->type());
  EXPECT_EQ(&file_null, item_stop->file_item());
  delete item_stop;

  string str_abc = "abc";
  EXPECT_TRUE(SafeWriteToFile(str_abc, "./abc", 0600));

  FileItem file_abc(new FileIngestionSource(std::string("./abc")));
  tube_in.EnqueueBack(&file_abc);
  BlockItem *item_data = tube_out->PopFront();
  EXPECT_EQ(3U, file_abc.size());
  EXPECT_EQ(BlockItem::kBlockData, item_data->type());
  EXPECT_EQ(str_abc, string(reinterpret_cast<char *>(item_data->data()),
                            item_data->size()));
  delete item_data;
  item_stop = tube_out->PopFront();
  EXPECT_EQ(BlockItem::kBlockStop, item_stop->type());
  delete item_stop;
  unlink("./abc");

  unsigned nblocks = 10;
  int fd_tmp = open("./large", O_CREAT | O_TRUNC | O_WRONLY, 0600);
  EXPECT_GT(fd_tmp, 0);
  for (unsigned i = 0; i < nblocks; ++i) {
    string str_block(TaskRead::kBlockSize, static_cast<char>(i));
    EXPECT_TRUE(SafeWrite(fd_tmp, str_block.data(), str_block.size()));
  }
  close(fd_tmp);

  unsigned size = nblocks * TaskRead::kBlockSize;

  FileItem file_large(new FileIngestionSource(std::string("./large")), size - 1,
                      size, size + 1);
  tube_in.EnqueueBack(&file_large);
  for (unsigned i = 0; i < nblocks; ++i) {
    item_data = tube_out->PopFront();
    if (i == 0) {
      EXPECT_GT(BlockItem::managed_bytes(), 0U);
    }
    EXPECT_EQ(BlockItem::kBlockData, item_data->type());
    EXPECT_EQ(
        string(TaskRead::kBlockSize, static_cast<char>(i)),
        string(reinterpret_cast<char *>(item_data->data()), item_data->size()));
    delete item_data;
  }
  EXPECT_EQ(size, file_large.size());
  EXPECT_TRUE(file_large.may_have_chunks());
  item_stop = tube_out->PopFront();
  EXPECT_EQ(BlockItem::kBlockStop, item_stop->type());
  delete item_stop;
  unlink("./large");

  task_group.Terminate();
}


TEST_F(T_Ingestion, TaskReadThrottle) {
  Tube<FileItem> tube_in;
  Tube<BlockItem> *tube_out = new Tube<BlockItem>();
  TubeGroup<BlockItem> tube_group_out;
  tube_group_out.TakeTube(tube_out);
  tube_group_out.Activate();

  TubeConsumerGroup<FileItem> task_group;
  TaskRead *task_read = new TaskRead(&tube_in, &tube_group_out, &allocator_);
  task_read->SetWatermarks(1, 2);
  task_group.TakeConsumer(task_read);
  task_group.Spawn();
  EXPECT_EQ(0U, task_read->n_block());

  string str_abc = "abc";
  EXPECT_TRUE(SafeWriteToFile(str_abc, "./abc", 0600));

  FileItem file_abc(new FileIngestionSource(std::string("./abc")));
  tube_in.EnqueueBack(&file_abc);

  FileItem file_null(new FileIngestionSource(std::string("/dev/null")));
  tube_in.EnqueueBack(&file_null);

  BlockItem *item_data = tube_out->PopFront();
  EXPECT_EQ(BlockItem::kBlockData, item_data->type());
  BlockItem *item_stop = tube_out->PopFront();
  EXPECT_EQ(BlockItem::kBlockStop, item_stop->type());

  if (task_read->n_block() == 0) {
    SafeSleepMs(2 * TaskRead::kThrottleMaxMs);
  }
  EXPECT_EQ(1U, task_read->n_block());

  delete item_data;
  delete item_stop;
  unlink("./abc");

  item_stop = tube_out->PopFront();
  EXPECT_EQ(BlockItem::kBlockStop, item_stop->type());
  delete item_stop;

  task_group.Terminate();
}


TEST_F(T_Ingestion, TaskChunkDispatch) {
  Tube<BlockItem> tube_in;
  Tube<BlockItem> *tube_out = new Tube<BlockItem>();
  TubeGroup<BlockItem> tube_group_out;
  tube_group_out.TakeTube(tube_out);
  tube_group_out.Activate();

  TubeConsumerGroup<BlockItem> task_group;
  task_group.TakeConsumer(
      new TaskChunk(&tube_in, &tube_group_out, &allocator_));
  task_group.Spawn();

  FileItem file_null(new FileIngestionSource(std::string("/dev/null")));
  file_null.set_size(0);
  EXPECT_FALSE(file_null.is_fully_chunked());
  EXPECT_EQ(0U, file_null.nchunks_in_fly());
  BlockItem *b1 = new BlockItem(1, &allocator_);
  b1->SetFileItem(&file_null);
  b1->MakeStop();
  tube_in.EnqueueBack(b1);
  BlockItem *item_stop = tube_out->PopFront();
  EXPECT_EQ(0U, tube_out->size());
  EXPECT_EQ(BlockItem::kBlockStop, item_stop->type());
  EXPECT_GE(item_stop->tag(), 2 << 28);
  EXPECT_EQ(&file_null, item_stop->file_item());
  EXPECT_EQ(&file_null, item_stop->chunk_item()->file_item());
  EXPECT_EQ(0U, item_stop->chunk_item()->size());
  EXPECT_FALSE(item_stop->chunk_item()->is_bulk_chunk());
  EXPECT_TRUE(item_stop->chunk_item()->IsSolePiece());
  EXPECT_EQ(shash::kSuffixPartial, item_stop->chunk_item()->hash_ptr()->suffix);
  EXPECT_TRUE(file_null.is_fully_chunked());
  EXPECT_EQ(1U, file_null.nchunks_in_fly());
  delete item_stop->chunk_item();
  delete item_stop;

  file_null.set_may_have_chunks(false);
  BlockItem *b2 = new BlockItem(2, &allocator_);
  b2->SetFileItem(&file_null);
  b2->MakeStop();
  tube_in.EnqueueBack(b2);
  item_stop = tube_out->PopFront();
  EXPECT_EQ(0U, item_stop->chunk_item()->size());
  EXPECT_TRUE(item_stop->chunk_item()->is_bulk_chunk());
  EXPECT_FALSE(item_stop->chunk_item()->IsSolePiece());
  EXPECT_EQ(shash::kSuffixNone, item_stop->chunk_item()->hash_ptr()->suffix);
  delete item_stop->chunk_item();
  delete item_stop;

  FileItem file_null_legacy(new FileIngestionSource(std::string("/dev/null")),
                            1024, 2048, 4096, zlib::kZlibDefault, shash::kSha1,
                            shash::kSuffixNone, true, true);
  file_null_legacy.set_size(0);
  BlockItem *b3 = new BlockItem(3, &allocator_);
  b3->SetFileItem(&file_null_legacy);
  b3->MakeStop();
  tube_in.EnqueueBack(b3);
  BlockItem *item_stop_chunk = tube_out->PopFront();
  EXPECT_FALSE(item_stop_chunk->chunk_item()->is_bulk_chunk());
  EXPECT_TRUE(item_stop_chunk->chunk_item()->IsSolePiece());
  delete item_stop_chunk->chunk_item();
  delete item_stop_chunk;
  BlockItem *item_stop_bulk = tube_out->PopFront();
  EXPECT_TRUE(item_stop_bulk->chunk_item()->is_bulk_chunk());
  EXPECT_FALSE(item_stop_bulk->chunk_item()->IsSolePiece());
  EXPECT_TRUE(file_null_legacy.is_fully_chunked());
  EXPECT_EQ(2U, file_null_legacy.nchunks_in_fly());
  delete item_stop_bulk->chunk_item();
  delete item_stop_bulk;

  task_group.Terminate();
}


TEST_F(T_Ingestion, TaskChunk) {
  Tube<BlockItem> tube_in;
  Tube<BlockItem> *tube_out = new Tube<BlockItem>();
  TubeGroup<BlockItem> tube_group_out;
  tube_group_out.TakeTube(tube_out);
  tube_group_out.Activate();

  TubeConsumerGroup<BlockItem> task_group;
  task_group.TakeConsumer(
      new TaskChunk(&tube_in, &tube_group_out, &allocator_));
  task_group.Spawn();

  // Tuned for ~100ms test with many blocks
  unsigned nblocks = 10000;
  unsigned size = nblocks * TaskRead::kBlockSize;
  unsigned avg_chunk_size = 4 * TaskRead::kBlockSize;
  // File does not exist
  FileItem file_large(new FileIngestionSource(std::string("./large")),
                      avg_chunk_size / 2, avg_chunk_size, avg_chunk_size * 2);

  EXPECT_FALSE(file_large.is_fully_chunked());
  for (unsigned i = 0; i < nblocks; ++i) {
    string str_content(TaskRead::kBlockSize, static_cast<char>(i));
    BlockItem *b = new BlockItem(1, &allocator_);
    b->SetFileItem(&file_large);
    b->MakeDataCopy(reinterpret_cast<const unsigned char *>(str_content.data()),
                    TaskRead::kBlockSize);
    tube_in.EnqueueBack(b);
  }
  BlockItem *b_stop = new BlockItem(1, &allocator_);
  b_stop->SetFileItem(&file_large);
  b_stop->MakeStop();
  tube_in.EnqueueBack(b_stop);

  unsigned consumed = 0;
  unsigned chunk_size = 0;
  unsigned n_recv_blocks = 0;
  int64_t tag = -1;
  uint64_t last_offset = 0;
  while (consumed < size) {
    BlockItem *b = tube_out->PopFront();
    EXPECT_FALSE(b->chunk_item()->is_bulk_chunk());
    EXPECT_FALSE(b->chunk_item()->IsSolePiece());
    if (tag == -1) {
      n_recv_blocks++;
      tag = b->tag();
    } else {
      EXPECT_EQ(tag, b->tag());
    }

    if (b->size() == 0) {
      EXPECT_EQ(BlockItem::kBlockStop, b->type());
      EXPECT_GE(chunk_size, avg_chunk_size / 2);
      EXPECT_LE(chunk_size, avg_chunk_size * 2);
      EXPECT_EQ(consumed, last_offset + chunk_size);
      EXPECT_EQ(chunk_size, b->chunk_item()->size());
      chunk_size = 0;
      tag = -1;
      delete b->chunk_item();
    } else {
      EXPECT_EQ(BlockItem::kBlockData, b->type());
      chunk_size += b->size();
      last_offset = b->chunk_item()->offset();
    }

    consumed += b->size();
    delete b;
  }
  b_stop = tube_out->PopFront();
  EXPECT_EQ(BlockItem::kBlockStop, b_stop->type());
  delete b_stop->chunk_item();
  delete b_stop;
  EXPECT_EQ(0U, tube_out->size());

  EXPECT_EQ(size, consumed);
  EXPECT_TRUE(file_large.is_fully_chunked());
  EXPECT_EQ(n_recv_blocks, file_large.nchunks_in_fly());

  task_group.Terminate();
}


TEST_F(T_Ingestion, TaskChunkCornerCases) {
  Tube<BlockItem> tube_in;
  Tube<BlockItem> *tube_out = new Tube<BlockItem>();
  TubeGroup<BlockItem> tube_group_out;
  tube_group_out.TakeTube(tube_out);
  tube_group_out.Activate();

  TubeConsumerGroup<BlockItem> task_group;
  task_group.TakeConsumer(
      new TaskChunk(&tube_in, &tube_group_out, &allocator_));
  task_group.Spawn();

  // File does not exist
  FileItem file_large(new FileIngestionSource(std::string("./large")), 1024,
                      2048, 4096);

  file_large.set_size(8192);
  // Ensure there is a chunking cut mark at EOF
  unsigned block_size = 512;
  assert((file_large.size() % block_size) == 0);
  assert(((file_large.size() / 2) % block_size) == 0);
  for (unsigned i = 0; i < (file_large.size() / block_size); ++i) {
    unsigned char *buf = reinterpret_cast<unsigned char *>(
        scalloc(1, block_size));
    BlockItem *b = new BlockItem(1, &allocator_);
    b->SetFileItem(&file_large);
    b->MakeDataCopy(buf, block_size);
    free(buf);
    tube_in.EnqueueBack(b);
  }
  BlockItem *b_stop = new BlockItem(1, &allocator_);
  b_stop->SetFileItem(&file_large);
  b_stop->MakeStop();
  tube_in.EnqueueBack(b_stop);

  // Expect exactly two chunks of the same size
  ChunkItem *chunk_item;
  for (unsigned i = 0; i < 2; ++i) {
    chunk_item = NULL;
    for (unsigned j = 0; j < ((file_large.size() / 2) / block_size); ++j) {
      BlockItem *b = tube_out->PopFront();
      EXPECT_FALSE(b->chunk_item()->is_bulk_chunk());
      if (chunk_item == NULL)
        chunk_item = b->chunk_item();
      EXPECT_EQ(chunk_item, b->chunk_item());
      delete b;
    }
    b_stop = tube_out->PopFront();
    EXPECT_EQ(BlockItem::kBlockStop, b_stop->type());
    EXPECT_EQ(file_large.size() / 2, chunk_item->size());
    delete b_stop;
    delete chunk_item;
  }
  EXPECT_TRUE(tube_out->IsEmpty());
  EXPECT_TRUE(file_large.is_fully_chunked());
  EXPECT_EQ(2U, file_large.nchunks_in_fly());

  task_group.Terminate();
}


TEST_F(T_Ingestion, TaskCompressNull) {
  Tube<BlockItem> tube_in;
  Tube<BlockItem> *tube_out = new Tube<BlockItem>();
  TubeGroup<BlockItem> tube_group_out;
  tube_group_out.TakeTube(tube_out);
  tube_group_out.Activate();

  TubeConsumerGroup<BlockItem> task_group;
  task_group.TakeConsumer(
      new TaskCompress(&tube_in, &tube_group_out, &allocator_));
  task_group.Spawn();

  FileItem file_null(new FileIngestionSource(std::string("/dev/null")));
  ChunkItem chunk_null(&file_null, 0);
  BlockItem *b1 = new BlockItem(1, &allocator_);
  b1->SetFileItem(&file_null);
  b1->SetChunkItem(&chunk_null);
  b1->MakeStop();
  tube_in.EnqueueBack(b1);

  void *ptr_zlib_null;
  uint64_t sz_zlib_null;
  EXPECT_TRUE(zlib::CompressMem2Mem(NULL, 0, &ptr_zlib_null, &sz_zlib_null));

  BlockItem *item_data = tube_out->PopFront();
  EXPECT_EQ(BlockItem::kBlockData, item_data->type());
  EXPECT_EQ(sz_zlib_null, item_data->size());
  EXPECT_EQ(0, memcmp(item_data->data(), ptr_zlib_null, sz_zlib_null));
  free(ptr_zlib_null);
  EXPECT_EQ(1, item_data->tag());
  EXPECT_EQ(&file_null, item_data->file_item());
  EXPECT_EQ(&chunk_null, item_data->chunk_item());
  delete item_data;
  BlockItem *item_stop = tube_out->PopFront();
  EXPECT_EQ(BlockItem::kBlockStop, item_stop->type());
  EXPECT_EQ(1, item_stop->tag());
  EXPECT_EQ(&file_null, item_stop->file_item());
  EXPECT_EQ(&chunk_null, item_stop->chunk_item());
  delete item_stop;
  EXPECT_EQ(0U, tube_out->size());

  task_group.Terminate();
}


TEST_F(T_Ingestion, TaskCompress) {
  Tube<BlockItem> tube_in;
  Tube<BlockItem> *tube_out = new Tube<BlockItem>();
  TubeGroup<BlockItem> tube_group_out;
  tube_group_out.TakeTube(tube_out);
  tube_group_out.Activate();

  TubeConsumerGroup<BlockItem> task_group;
  task_group.TakeConsumer(
      new TaskCompress(&tube_in, &tube_group_out, &allocator_));
  task_group.Spawn();

  unsigned size = 16 * 1024 * 1024;
  unsigned block_size = 32 * 1024;
  unsigned nblocks = size / block_size;
  EXPECT_EQ(0U, size % block_size);
  BlockItem block_raw(42, &allocator_);
  block_raw.MakeData(size);
  unsigned char *buf = reinterpret_cast<unsigned char *>(smalloc(size));

  // File does not exist
  FileItem file_large(new FileIngestionSource(std::string("./large")));
  ChunkItem chunk_large(&file_large, 0);
  for (unsigned i = 0; i < nblocks; ++i) {
    string str_content(block_size, static_cast<char>(i));
    for (unsigned j = 1; j < block_size; ++j)
      str_content[j] = i * str_content[j - 1] + j;

    BlockItem *b = new BlockItem(1, &allocator_);
    b->SetFileItem(&file_large);
    b->SetChunkItem(&chunk_large);
    b->MakeDataCopy(reinterpret_cast<const unsigned char *>(str_content.data()),
                    block_size);
    EXPECT_EQ(block_size, block_raw.Write(b->data(), b->size()));
    tube_in.EnqueueBack(b);
  }
  EXPECT_EQ(size, block_raw.size());
  BlockItem *b_stop = new BlockItem(1, &allocator_);
  b_stop->SetFileItem(&file_large);
  b_stop->SetChunkItem(&chunk_large);
  b_stop->MakeStop();
  tube_in.EnqueueBack(b_stop);

  void *ptr_zlib_large = NULL;
  uint64_t sz_zlib_large = 0;
  EXPECT_TRUE(zlib::CompressMem2Mem(block_raw.data(), block_raw.size(),
                                    &ptr_zlib_large, &sz_zlib_large));
  free(buf);

  unsigned char *ptr_read_large = reinterpret_cast<unsigned char *>(
      smalloc(sz_zlib_large));
  unsigned read_pos = 0;

  BlockItem *b = NULL;
  do {
    delete b;
    b = tube_out->PopFront();
    EXPECT_EQ(1, b->tag());
    EXPECT_EQ(&file_large, b->file_item());
    EXPECT_EQ(&chunk_large, b->chunk_item());
    EXPECT_LE(read_pos + b->size(), sz_zlib_large);
    if (b->size() > 0) {
      memcpy(ptr_read_large + read_pos, b->data(), b->size());
      read_pos += b->size();
    }
  } while (b->type() == BlockItem::kBlockData);
  EXPECT_EQ(BlockItem::kBlockStop, b->type());
  delete b;
  EXPECT_EQ(0U, tube_out->size());

  EXPECT_EQ(sz_zlib_large, read_pos);
  EXPECT_EQ(0, memcmp(ptr_zlib_large, ptr_read_large, sz_zlib_large));

  free(ptr_read_large);
  free(ptr_zlib_large);
  task_group.Terminate();
}


TEST_F(T_Ingestion, TaskHash) {
  Tube<BlockItem> tube_in;
  Tube<BlockItem> *tube_out = new Tube<BlockItem>();
  TubeGroup<BlockItem> tube_group_out;
  tube_group_out.TakeTube(tube_out);
  tube_group_out.Activate();

  TubeConsumerGroup<BlockItem> task_group;
  task_group.TakeConsumer(new TaskHash(&tube_in, &tube_group_out));
  task_group.Spawn();

  FileItem file_null(new FileIngestionSource(std::string("/dev/null")));
  ChunkItem chunk_null(&file_null, 0);
  BlockItem b1(1, &allocator_);
  b1.SetFileItem(&file_null);
  b1.SetChunkItem(&chunk_null);
  b1.MakeStop();
  tube_in.EnqueueBack(&b1);

  BlockItem *item_stop = tube_out->PopFront();
  EXPECT_EQ(&b1, item_stop);
  EXPECT_EQ("da39a3ee5e6b4b0d3255bfef95601890afd80709",
            chunk_null.hash_ptr()->ToString());
  EXPECT_EQ(0U, tube_out->size());

  string str_abc = "abc";
  EXPECT_TRUE(SafeWriteToFile(str_abc, "./abc", 0600));

  FileItem file_abc(new FileIngestionSource(std::string("./abc")));
  ChunkItem chunk_abc(&file_abc, 0);
  BlockItem b2_a(2, &allocator_);
  b2_a.SetFileItem(&file_null);
  b2_a.SetChunkItem(&chunk_abc);
  b2_a.MakeDataCopy(reinterpret_cast<const unsigned char *>(str_abc.data()),
                    str_abc.size());
  BlockItem b2_b(2, &allocator_);
  b2_b.SetFileItem(&file_null);
  b2_b.SetChunkItem(&chunk_abc);
  b2_b.MakeStop();
  tube_in.EnqueueBack(&b2_a);
  tube_in.EnqueueBack(&b2_b);

  BlockItem *item_data = tube_out->PopFront();
  EXPECT_EQ(&b2_a, item_data);
  item_stop = tube_out->PopFront();
  EXPECT_EQ(&b2_b, item_stop);
  EXPECT_EQ("a9993e364706816aba3e25717850c26c9cd0d89d",
            chunk_abc.hash_ptr()->ToString());
  EXPECT_EQ(0U, tube_out->size());

  unlink("./abc");

  task_group.Terminate();
}


TEST_F(T_Ingestion, TaskWriteNull) {
  Tube<BlockItem> tube_in;
  Tube<FileItem> *tube_out = new Tube<FileItem>();
  TubeGroup<FileItem> tube_group_out;
  tube_group_out.TakeTube(tube_out);
  tube_group_out.Activate();

  TubeConsumerGroup<BlockItem> task_group;
  task_group.TakeConsumer(new TaskWrite(&tube_in, &tube_group_out, uploader_));
  task_group.Spawn();

  FileItem file_null(new FileIngestionSource(std::string("/dev/null")));
  file_null.set_size(0);
  file_null.set_is_fully_chunked();
  ChunkItem *chunk_null = new ChunkItem(&file_null, 0);
  shash::Any hash_empty(shash::kSha1);
  hash_empty.suffix = shash::kSuffixPartial;
  shash::HashString("", &hash_empty);
  *chunk_null->hash_ptr() = hash_empty;
  BlockItem *b1 = new BlockItem(1, &allocator_);
  b1->SetFileItem(&file_null);
  b1->SetChunkItem(chunk_null);
  b1->MakeStop();
  tube_in.EnqueueBack(b1);
  FileItem *file_processed = tube_out->PopFront();
  EXPECT_EQ(&file_null, file_processed);
  EXPECT_EQ(0U, file_processed->GetNumChunks());
  EXPECT_EQ(hash_empty, file_processed->bulk_hash());
  EXPECT_EQ(1U, uploader_->results.size());
  EXPECT_EQ(hash_empty, uploader_->results[0].computed_hash);

  task_group.Terminate();
}


TEST_F(T_Ingestion, TaskWriteLarge) {
  Tube<BlockItem> tube_in;
  Tube<FileItem> *tube_out = new Tube<FileItem>();
  TubeGroup<FileItem> tube_group_out;
  tube_group_out.TakeTube(tube_out);
  tube_group_out.Activate();

  TubeConsumerGroup<BlockItem> task_group;
  task_group.TakeConsumer(new TaskWrite(&tube_in, &tube_group_out, uploader_));
  task_group.Spawn();

  // File does not exist
  FileItem file_large(new FileIngestionSource(std::string("./large")));
  unsigned nchunks = 32;
  unsigned chunk_size = 1024 * 1024;
  unsigned block_size = 1024;
  ASSERT_EQ(0U, chunk_size % block_size);
  file_large.set_size(nchunks * chunk_size);

  shash::Any hash_zeros(shash::kSha1);
  hash_zeros.suffix = shash::kSuffixPartial;
  unsigned char *dummy_buffer = reinterpret_cast<unsigned char *>(
      scalloc(1, chunk_size));
  shash::HashMem(dummy_buffer, chunk_size, &hash_zeros);
  free(dummy_buffer);

  for (unsigned i = 0; i < nchunks; ++i) {
    ChunkItem *chunk_item = new ChunkItem(&file_large, i * chunk_size);
    unsigned nblocks = chunk_size / block_size;
    unsigned char block_buffer[block_size];
    memset(block_buffer, 0, block_size);
    for (unsigned j = 0; j < nblocks; ++j) {
      BlockItem *b = new BlockItem(i, &allocator_);
      b->SetFileItem(&file_large);
      b->SetChunkItem(chunk_item);
      b->MakeDataCopy(block_buffer, block_size);
      tube_in.EnqueueBack(b);
    }

    *chunk_item->hash_ptr() = hash_zeros;

    BlockItem *b_stop = new BlockItem(i, &allocator_);
    b_stop->SetFileItem(&file_large);
    b_stop->SetChunkItem(chunk_item);
    b_stop->MakeStop();
    tube_in.EnqueueBack(b_stop);

    if (i == (nchunks - 1)) {
      file_large.set_is_fully_chunked();
    }
  }

  FileItem *file_processed = tube_out->PopFront();
  EXPECT_EQ(&file_large, file_processed);
  EXPECT_EQ(nchunks, file_processed->GetNumChunks());
  EXPECT_EQ(nchunks, uploader_->results.size());
  EXPECT_EQ(shash::Any(hash_zeros.algorithm), file_processed->bulk_hash());

  task_group.Terminate();
}


TEST_F(T_Ingestion, PipelineNull) {
  upload::SpoolerDefinition spooler_definition = MockSpoolerDefinition();
  spooler_definition.compression_alg = zlib::kNoCompression;

  UniquePtr<IngestionPipeline> pipeline_straight(
      new IngestionPipeline(uploader_, spooler_definition));
  FnFileProcessed fn_processed;
  pipeline_straight->RegisterListener(&FnFileProcessed::OnFileProcessed,
                                      &fn_processed);
  pipeline_straight->Spawn();

  pipeline_straight->Process(new FileIngestionSource(std::string("/dev/null")),
                             true);
  pipeline_straight->WaitFor();
  EXPECT_EQ(1, atomic_read64(&fn_processed.ncall));
  EXPECT_EQ(1U, uploader_->results.size());
  shash::Any hash_null(spooler_definition.hash_algorithm);
  shash::HashString("", &hash_null);
  EXPECT_EQ(hash_null, uploader_->results[0].computed_hash);

  uploader_->ClearResults();

  spooler_definition.compression_alg = zlib::kZlibDefault;
  spooler_definition.hash_algorithm = shash::kShake128;
  UniquePtr<IngestionPipeline> pipeline_zlib(
      new IngestionPipeline(uploader_, spooler_definition));
  pipeline_zlib->Spawn();

  pipeline_zlib->Process(new FileIngestionSource(std::string("/dev/null")),
                         true);
  pipeline_zlib->WaitFor();
  EXPECT_EQ(1U, uploader_->results.size());
  void *compressed_null = NULL;
  uint64_t sz_compressed_null;
  EXPECT_TRUE(
      zlib::CompressMem2Mem(NULL, 0, &compressed_null, &sz_compressed_null));
  shash::Any hash_compressed_null(spooler_definition.hash_algorithm);
  shash::HashMem(reinterpret_cast<unsigned char *>(compressed_null),
                 sz_compressed_null, &hash_compressed_null);
  free(compressed_null);
  EXPECT_EQ(hash_compressed_null, uploader_->results[0].computed_hash);
}


TEST_F(T_Ingestion, Scrubbing) {
  UniquePtr<ScrubbingPipeline> pipeline_scrubbing(new ScrubbingPipeline());
  FnFileHashed fn_hashed;
  pipeline_scrubbing->RegisterListener(&FnFileHashed::OnFileProcessed,
                                       &fn_hashed);
  pipeline_scrubbing->Spawn();

  shash::Any null_hash(shash::kShake128);
  HashString("", &null_hash);

  pipeline_scrubbing->Process(new FileIngestionSource(std::string("/dev/null")),
                              shash::kShake128, shash::kSuffixNone);
  pipeline_scrubbing->WaitFor();
  EXPECT_EQ(1, atomic_read64(&fn_hashed.ncall));
  EXPECT_EQ("/dev/null", fn_hashed.last_result.path);
  EXPECT_EQ(null_hash, fn_hashed.last_result.hash);
}
