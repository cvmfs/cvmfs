/**
 * This file is part of the CernVM File System.
 */

#include <errno.h>
#include <gtest/gtest.h>
#include <pthread.h>
#include <unistd.h>
#include <vector>
#include <string>
#include <algorithm>

#include "clientctx.h"
#include "interrupt.h"
#include "util/platform.h"
#include "util/concurrency.h"

namespace {
class MockInterruptCue : public InterruptCue {
 public:
  virtual bool IsCanceled() { return false; }
};

struct ThreadData {
  std::string name;
  unsigned delay_ms;
  bool should_unset;
};

void *MainTestThread(void *data) {
  ThreadData *td = static_cast<ThreadData *>(data);
  MockInterruptCue ic;
  
  ClientCtx::GetInstance()->Set(100, 200, getpid(), &ic, td->name);
  if (td->delay_ms > 0) {
    SafeSleepMs(td->delay_ms);
  }
  if (td->should_unset) {
    ClientCtx::GetInstance()->Unset();
  }
  return NULL;
}
}

TEST(T_MonitorComplex, MultiThreadedBreadcrumbs) {
  ClientCtx::CleanupInstance();
  ClientCtx *ctx = ClientCtx::GetInstance();
  
  ThreadData td1 = {"stuck_op", 1000, false};
  ThreadData td2 = {"quick_op", 10, true};
  ThreadData td3 = {"another_stuck", 1000, false};
  
  pthread_t threads[3];
  pthread_create(&threads[0], NULL, MainTestThread, &td1);
  pthread_create(&threads[2], NULL, MainTestThread, &td3);
  
  // Wait for 1 and 3 to start
  SafeSleepMs(100);
  
  // Start 2 and let it finish
  pthread_create(&threads[1], NULL, MainTestThread, &td2);
  pthread_join(threads[1], NULL);
  
  pthread_mutex_t *lock = ctx->GetLockTlsBlocks();
  pthread_mutex_lock(lock);
  const std::vector<ClientCtx::ThreadLocalStorage *> &blocks = ctx->GetTlsBlocks();
  
  // We expect 3 blocks in total (TLS blocks are preserved even after Unset)
  EXPECT_EQ(3U, blocks.size());
  
  int active_count = 0;
  int inactive_count = 0;
  bool found_stuck_op = false;
  bool found_another_stuck = false;
  
  for (unsigned i = 0; i < blocks.size(); ++i) {
    if (blocks[i]->is_set) {
      active_count++;
      if (blocks[i]->name == "stuck_op") found_stuck_op = true;
      if (blocks[i]->name == "another_stuck") found_another_stuck = true;
      EXPECT_GT(blocks[i]->start_time, 0U);
    } else {
      inactive_count++;
      EXPECT_EQ("", blocks[i]->name);
      EXPECT_EQ(0U, blocks[i]->start_time);
    }
  }
  
  pthread_mutex_unlock(lock);
  
  EXPECT_EQ(2, active_count);
  EXPECT_EQ(1, inactive_count);
  EXPECT_TRUE(found_stuck_op);
  EXPECT_TRUE(found_another_stuck);
  
  // Clean up remaining threads
  pthread_join(threads[0], NULL);
  pthread_join(threads[2], NULL);
  
  ClientCtx::CleanupInstance();
}
