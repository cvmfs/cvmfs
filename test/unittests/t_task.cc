/**
 * This file is part of the CernVM File System.
 */

#include "gtest/gtest.h"

#include <fcntl.h>
#include <unistd.h>

#include "atomic.h"
#include "ingestion/item.h"
#include "ingestion/task.h"
#include "ingestion/task_read.h"
#include "smalloc.h"
#include "util/posix.h"

using namespace std;  // NOLINT

namespace {
class DummyItem {
 public:
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
}


class T_Task : public ::testing::Test {
 protected:
  static const unsigned kNumTasks = 32;

  virtual void SetUp() {
    DummyItem::sum = 0;
    TestTask::cnt_terminate = 0;
    TestTask::cnt_process = 0;
    for (unsigned i = 0; i < kNumTasks; ++i)
      task_group_.TakeConsumer(new TestTask(&tube_));
  }

  virtual void TearDown() {
  }

  Tube<DummyItem> tube_;
  TubeConsumerGroup<DummyItem> task_group_;
};


TEST_F(T_Task, Basic) {
  DummyItem i1(1);
  DummyItem i2(2);
  DummyItem i3(3);

  task_group_.Spawn();
  EXPECT_EQ(0, atomic_read32(&TestTask::cnt_terminate));
  EXPECT_EQ(0, atomic_read32(&TestTask::cnt_process));
  EXPECT_EQ(0, atomic_read32(&DummyItem::sum));

  tube_.Enqueue(&i1);
  tube_.Enqueue(&i2);
  tube_.Enqueue(&i3);

  tube_.Wait();
  task_group_.Terminate();
  EXPECT_EQ(static_cast<int>(kNumTasks),
            atomic_read32(&TestTask::cnt_terminate));

  EXPECT_EQ(6, atomic_read32(&DummyItem::sum));
  EXPECT_EQ(3, atomic_read32(&TestTask::cnt_process));
}


TEST_F(T_Task, Stress) {
  DummyItem i1(1);
  DummyItem i2(2);
  DummyItem i3(3);

  task_group_.Spawn();
  EXPECT_EQ(0, atomic_read32(&TestTask::cnt_terminate));
  EXPECT_EQ(0, atomic_read32(&TestTask::cnt_process));
  EXPECT_EQ(0, atomic_read32(&DummyItem::sum));

  for (unsigned i = 0; i < 10000; ++i) {
    tube_.Enqueue(&i1);
    tube_.Enqueue(&i2);
    tube_.Enqueue(&i3);
  }

  tube_.Wait();
  task_group_.Terminate();
  EXPECT_EQ(static_cast<int>(kNumTasks),
            atomic_read32(&TestTask::cnt_terminate));

  EXPECT_EQ(10000 * 6, atomic_read32(&DummyItem::sum));
  EXPECT_EQ(10000 * 3, atomic_read32(&TestTask::cnt_process));
}


TEST_F(T_Task, Read) {
  Tube<FileItem> tube_in;
  Tube<BlockItem> tube_out;
  tube_in.set_nstage(0);
  tube_out.set_nstage(1);

  TubeConsumerGroup<FileItem> task_group;
  task_group.TakeConsumer(new TaskRead(&tube_in, &tube_out));
  task_group.Spawn();

  FileItem file_null("/dev/null");
  tube_in.Enqueue(&file_null);
  BlockItem *item_stop = tube_out.Pop();
  EXPECT_EQ(BlockItem::kBlockStop, item_stop->type());
  delete item_stop;

  string str_abc = "abc";
  EXPECT_TRUE(SafeWriteToFile(str_abc, "./abc", 0600));
  FileItem file_abc("./abc");
  tube_in.Enqueue(&file_abc);
  BlockItem *item_data = tube_out.Pop();
  EXPECT_EQ(BlockItem::kBlockData, item_data->type());
  EXPECT_EQ(str_abc, string(reinterpret_cast<char *>(item_data->data()),
                            item_data->size()));
  delete item_data;
  item_stop = tube_out.Pop();
  EXPECT_EQ(BlockItem::kBlockStop, item_stop->type());
  delete item_stop;
  unlink("./abc");

  unsigned nblocks = 10;
  int fd_tmp = open("./large", O_CREAT | O_TRUNC | O_WRONLY, 0600);
  EXPECT_GT(fd_tmp, 0);
  for (unsigned i = 0; i < nblocks; ++i) {
    string str_block(TaskRead::kBlockSize, i);
    EXPECT_TRUE(SafeWrite(fd_tmp, str_block.data(), str_block.size()));
  }
  close(fd_tmp);

  FileItem file_large("./large");
  tube_in.Enqueue(&file_large);
  for (unsigned i = 0; i < nblocks; ++i) {
    item_data = tube_out.Pop();
    EXPECT_EQ(BlockItem::kBlockData, item_data->type());
    EXPECT_EQ(string(TaskRead::kBlockSize, i),
              string(reinterpret_cast<char *>(item_data->data()),
                                              item_data->size()));
    delete item_data;
  }
  item_stop = tube_out.Pop();
  EXPECT_EQ(BlockItem::kBlockStop, item_stop->type());
  delete item_stop;
  unlink("./large");

  task_group.Terminate();
}
