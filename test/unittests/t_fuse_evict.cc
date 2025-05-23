/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <vector>

#include "fuse_evict.h"
#include "glue_buffer.h"
#include "util/string.h"

class T_FuseInvalidator : public ::testing::Test {
 protected:
  virtual void SetUp() {
    invalidator_ = new FuseInvalidator(
        &inode_tracker_, &dentry_tracker_, NULL, true);
    invalidator_->Spawn();
  }

  virtual void TearDown() { delete invalidator_; }

 protected:
  glue::InodeTracker inode_tracker_;
  glue::DentryTracker dentry_tracker_;
  FuseInvalidator *invalidator_;
};


TEST_F(T_FuseInvalidator, StartStop) {
  FuseInvalidator *idle_invalidator = new FuseInvalidator(
      &inode_tracker_, &dentry_tracker_, NULL, true);
  EXPECT_FALSE(idle_invalidator->spawned_);
  delete idle_invalidator;

  FuseInvalidator *noop_invalidator = new FuseInvalidator(
      &inode_tracker_, &dentry_tracker_, NULL, true);
  noop_invalidator->Spawn();
  EXPECT_TRUE(noop_invalidator->spawned_);
  delete noop_invalidator;
}


TEST_F(T_FuseInvalidator, InvalidateTimeout) {
  FuseInvalidator::Handle handle(0);
  EXPECT_FALSE(handle.IsDone());
  invalidator_->InvalidateInodes(&handle);
  handle.WaitFor();
  EXPECT_TRUE(handle.IsDone());

  invalidator_->terminated_ = 1;
  FuseInvalidator::Handle handle2(1000000);
  EXPECT_FALSE(handle2.IsDone());
  invalidator_->InvalidateInodes(&handle2);
  handle2.WaitFor();
  EXPECT_TRUE(handle2.IsDone());
}


TEST_F(T_FuseInvalidator, InvalidateOps) {
  invalidator_->fuse_channel_or_session_ = reinterpret_cast<void **>(this);
  inode_tracker_.VfsGet(glue::InodeEx(1, glue::InodeEx::kDirectory),
                        PathString(""));
  for (unsigned i = 2; i <= 1024; ++i) {
    inode_tracker_.VfsGet(glue::InodeEx(i, glue::InodeEx::kRegular),
                          PathString("/" + StringifyInt(i)));
  }
  for (unsigned i = 0; i < 1024; ++i) {
    dentry_tracker_.Add(i, "404", 100000);
  }

  FuseInvalidator::Handle handle(0);
  EXPECT_FALSE(handle.IsDone());
  invalidator_->InvalidateInodes(&handle);
  handle.WaitFor();
  EXPECT_TRUE(handle.IsDone());
  EXPECT_EQ(FuseInvalidator::kCheckTimeoutFreqOps,
            fuse_lowlevel_notify_inval_inode_cnt);
  EXPECT_EQ(1024U, fuse_lowlevel_notify_inval_entry_cnt);

  FuseInvalidator::Handle handle2(1000000);
  EXPECT_FALSE(handle2.IsDone());
  invalidator_->InvalidateInodes(&handle2);
  handle2.WaitFor();
  EXPECT_TRUE(handle2.IsDone());
  EXPECT_EQ(FuseInvalidator::kCheckTimeoutFreqOps + 1024,
            fuse_lowlevel_notify_inval_inode_cnt);
  EXPECT_EQ(1024U, fuse_lowlevel_notify_inval_entry_cnt);

  for (unsigned i = 0; i < 1024; ++i) {
    dentry_tracker_.Add(i, "404", 100000);
  }
  invalidator_->terminated_ = 1;
  handle2.Reset();
  EXPECT_FALSE(handle2.IsDone());
  invalidator_->InvalidateInodes(&handle2);
  handle2.WaitFor();
  EXPECT_TRUE(handle2.IsDone());
  EXPECT_EQ((2 * FuseInvalidator::kCheckTimeoutFreqOps) + 1024,
            fuse_lowlevel_notify_inval_inode_cnt);
  EXPECT_EQ(FuseInvalidator::kCheckTimeoutFreqOps + 1024,
            fuse_lowlevel_notify_inval_entry_cnt);
}
