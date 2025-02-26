/**
 * This file is part of the CernVM File System.
 */
#define __STDC_FORMAT_MACROS
#include <benchmark/benchmark.h>

#include <cassert>
#include <string>
#include <vector>

#include "bm_util.h"
#include "glue_buffer.h"
#include "util/algorithm.h"
#include "util/string.h"

using namespace std;  // NOLINT

class BM_InodeTracker : public benchmark::Fixture {
 protected:
  virtual void SetUp(const benchmark::State &st) {
    Prng prng;
    prng.InitLocaltime();
    vector<uint64_t> sorted;
    for (unsigned i = 0; i < kNumInodes; ++i)
      sorted.push_back(i + 1);
    inodes_ = Shuffle(sorted, &prng);

    paths_.push_back(PathString("/", 1));
    for (unsigned i = 0; i < 10; ++i) {
      string path_1st = "/" + StringifyInt(i);
      paths_.push_back(PathString(path_1st));
      // First level
      for (unsigned j = 0; j < 10; ++j) {
        // Second level, 100 elements
        string path_2nd = path_1st + "/" + StringifyInt(j);
        paths_.push_back(PathString(path_2nd));
        for (unsigned k = 0; k < 10; ++k) {
          // Third level, 1000 elements
          string path_3rd = path_2nd + "/" + StringifyInt(k);
          paths_.push_back(PathString(path_3rd));
          for (unsigned l = 0; l < 10; ++l) {
            // Forth level, 10000 elements
            string path_4th = path_3rd + "/" + StringifyInt(l);
            paths_.push_back(PathString(path_4th));
          }
        }
      }
    }

    assert(inodes_.size() == paths_.size());

    inode_tracker_ = new glue::InodeTracker();
    inode_tracker_->VfsGet(glue::InodeEx(kNumInodes + 1, 0),
                           PathString("/", 1));
  }

  virtual void TearDown(const benchmark::State &st) {
    delete inode_tracker_;
    inodes_.clear();
    paths_.clear();
  }

  // Construction of paths_ needs to be changed if this number changes
  static const unsigned kNumInodes = 11111;

  vector<uint64_t> inodes_;
  vector<PathString> paths_;
  glue::InodeTracker *inode_tracker_;
};


BENCHMARK_DEFINE_F(BM_InodeTracker, Get)(benchmark::State &st) {
  unsigned i = 0;
  while (st.KeepRunning()) {
    unsigned idx = i % kNumInodes;
    inode_tracker_->VfsGet(glue::InodeEx(inodes_[idx], 0), paths_[idx]);
    ++i;
  }
  st.SetItemsProcessed(st.iterations());
}
BENCHMARK_REGISTER_F(BM_InodeTracker, Get)->Repetitions(3);


// Combined cost of adding and removing an element from the tracker
// Needs to run at least 10,000 times
BENCHMARK_DEFINE_F(BM_InodeTracker, GetPut)(benchmark::State &st) {
  unsigned i = 0;
  while (st.KeepRunning()) {
    unsigned idx = i % 5000;
    if (((i / 5000) % 2) == 0) {
      inode_tracker_->VfsGet(glue::InodeEx(inodes_[idx], 0), paths_[idx]);
    } else {
      inode_tracker_->GetVfsPutRaii().VfsPut(inodes_[idx], 1);
    }
    ++i;
  }
  st.SetItemsProcessed(st.iterations());
}
BENCHMARK_REGISTER_F(BM_InodeTracker, GetPut)->Repetitions(3);


BENCHMARK_DEFINE_F(BM_InodeTracker, FindPath)(benchmark::State &st) {
  unsigned size = st.range(0);
  for (unsigned i = 0; i < size; ++i)
    inode_tracker_->VfsGet(glue::InodeEx(inodes_[i], 0), paths_[i]);

  unsigned i = 0;
  PathString path;
  while (st.KeepRunning()) {
    unsigned idx = i % size;
    glue::InodeEx inode_ex(inodes_[idx], 0);
    const bool retval = inode_tracker_->FindPath(&inode_ex, &path);
    assert(retval == true);
    Escape(&path);
    ++i;
  }
  st.SetItemsProcessed(st.iterations());
}
BENCHMARK_REGISTER_F(BM_InodeTracker, FindPath)->Repetitions(3)->Arg(10000);


BENCHMARK_DEFINE_F(BM_InodeTracker, FindInode)(benchmark::State &st) {
  unsigned size = st.range(0);
  for (unsigned i = 0; i < size; ++i)
    inode_tracker_->VfsGet(glue::InodeEx(inodes_[i], 0), paths_[i]);

  unsigned i = 0;
  uint64_t inode;
  while (st.KeepRunning()) {
    unsigned idx = i % size;
    inode = inode_tracker_->FindInode(paths_[idx]);
    Escape(&inode);
    ++i;
  }
  st.SetItemsProcessed(st.iterations());
}
BENCHMARK_REGISTER_F(BM_InodeTracker, FindInode)->Repetitions(3)->Arg(10000);


BENCHMARK_DEFINE_F(BM_InodeTracker, Nadd)(benchmark::State &st) {
  unsigned size = st.range(0);
  while (st.KeepRunning()) {
    glue::DentryTracker tracker;
    for (unsigned i = 0; i < size; ++i)
      tracker.Add(0, "libCore.so", 1000);
  }
  st.SetItemsProcessed(st.iterations() * size);
}
BENCHMARK_REGISTER_F(BM_InodeTracker, Nadd)->Repetitions(3)->Arg(100000);
