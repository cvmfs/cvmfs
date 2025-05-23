/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <map>

#include "fd_table.h"
#include "util/prng.h"

using namespace std;  // NOLINT

class T_FdTable : public ::testing::Test {
 protected:
  virtual void SetUp() { fd_table_ = new FdTable<int>(5, -1); }

  virtual void TearDown() { delete fd_table_; }

  FdTable<int> *fd_table_;
};


TEST_F(T_FdTable, Basics) {
  EXPECT_GE(fd_table_->OpenFd(0), 0);
  EXPECT_GE(fd_table_->OpenFd(1), 0);
  EXPECT_GE(fd_table_->OpenFd(2), 0);
  EXPECT_GE(fd_table_->OpenFd(3), 0);
  EXPECT_GE(fd_table_->OpenFd(4), 0);

  EXPECT_EQ(-ENFILE, fd_table_->OpenFd(5));

  EXPECT_EQ(0, fd_table_->CloseFd(0));
  int fd = fd_table_->OpenFd(5);
  EXPECT_GE(fd, 0);
  EXPECT_EQ(-1, fd_table_->GetHandle(5));
  EXPECT_EQ(-1, fd_table_->GetHandle(-1));
  EXPECT_EQ(5, fd_table_->GetHandle(fd));
  EXPECT_EQ(-EBADF, fd_table_->CloseFd(5));
  EXPECT_EQ(-EBADF, fd_table_->CloseFd(-1));
  EXPECT_EQ(0, fd_table_->CloseFd(fd));
  EXPECT_EQ(-EBADF, fd_table_->CloseFd(fd));
  EXPECT_EQ(-1, fd_table_->GetHandle(fd));
  EXPECT_GE(fd_table_->OpenFd(6), 0);
}


TEST_F(T_FdTable, Stress) {
  map<int, int> open_fds;
  Prng prng;
  prng.InitLocaltime();
  for (unsigned i = 0; i < 1000000; ++i) {
    int op = prng.Next(2);
    if (op % 2 == 0) {
      // Open
      int fd = fd_table_->OpenFd(i);
      if (open_fds.size() < 5) {
        EXPECT_GE(fd, 0);
        open_fds[fd] = i;
      } else {
        EXPECT_EQ(-ENFILE, fd);
      }
    } else {
      // Access & Close
      if (open_fds.size() == 0) {
        int fd = prng.Next(5);
        EXPECT_EQ(-1, fd_table_->GetHandle(fd));
        EXPECT_EQ(-EBADF, fd_table_->CloseFd(fd));
      } else {
        unsigned fd_idx = (open_fds.size() > 1) ? prng.Next(open_fds.size())
                                                : 0;
        map<int, int>::const_iterator iter = open_fds.begin();
        for (unsigned i = 0; i < fd_idx; ++i, ++iter) {
        }
        EXPECT_EQ(iter->second, fd_table_->GetHandle(iter->first));
        EXPECT_EQ(0, fd_table_->CloseFd(iter->first));
        open_fds.erase(iter->first);
      }
    }
  }
}
