/**
 * This file is part of the CernVM File System.
 */

#include "gtest/gtest.h"

#include "atomic.h"
#include "ingestion/item.h"
#include "ingestion/task.h"
#include "ingestion/task_read.h"

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
  explicit TestTask(Tube<DummyItem> *tube) : TubeConsumer<DummyItem>(tube) { }

 protected:
  virtual void Process(DummyItem *item) {
    atomic_xadd32(&item->sum, item->summand);
  }
  virtual void OnTerminate() { atomic_inc32(&cnt_terminate); }
};
atomic_int32 TestTask::cnt_terminate = 0;
}


class T_Task : public ::testing::Test {
 protected:
  static const unsigned kNumTasks = 3;

  virtual void SetUp() {
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
  EXPECT_EQ(0, atomic_read32(&DummyItem::sum));

  tube_.Enqueue(&i1);
  tube_.Enqueue(&i2);
  tube_.Enqueue(&i3);
  tube_.Wait();
  EXPECT_EQ(6, atomic_read32(&DummyItem::sum));

  task_group_.Terminate();
  EXPECT_EQ(static_cast<int>(kNumTasks),
            atomic_read32(&TestTask::cnt_terminate));
}


TEST_F(T_Task, Read) {
  Tube<FileItem> tube_in;
  Tube<BlockItem> tube_out;

  TubeConsumerGroup<FileItem> task_group;
  task_group.TakeConsumer(new TaskRead(&tube_in, &tube_out));
  task_group.Spawn();

  FileItem file_null("/dev/null");
  tube_in.Enqueue(&file_null);
  BlockItem *item_stop = tube_out.Pop();
  EXPECT_EQ(BlockItem::kBlockStop, item_stop->type());
  delete item_stop;

  task_group.Terminate();
}
