/**
 * This file is part of the CernVM File System.
 */

#include "statistics.h"

#include <cassert>

#include "smalloc.h"
#include "util_concurrency.h"

using namespace std;  // NOLINT

namespace perf {

Counter *Statistics::Lookup(const std::string &name) {
  MutexLockGuard lock_guard(lock_);
  map<string, CounterInfo *>::const_iterator i = counters_.find(name);
  if (i != counters_.end())
    return &i->second->counter;
  return NULL;
}


std::string Statistics::LookupDesc(const std::string &name) {
  MutexLockGuard lock_guard(lock_);
  map<string, CounterInfo *>::const_iterator i = counters_.find(name);
  if (i != counters_.end())
    return i->second->desc;
  return "";
}


Counter *Statistics::Register(const string &name, const string &desc) {
  MutexLockGuard lock_guard(lock_);
  if (counters_.find(name) != counters_.end())
    return NULL;
  CounterInfo *counter_info = new CounterInfo(desc);
  counters_[name] = counter_info;
  return &counter_info->counter;
}


Statistics::Statistics() {
  lock_ =
    reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock_, NULL);
  assert(retval == 0);
}


Statistics::~Statistics() {
  for (map<string, CounterInfo *>::iterator i = counters_.begin(),
       iEnd = counters_.end(); i != iEnd; ++i)
  {
    delete i->second;
  }
  pthread_mutex_destroy(lock_);
  free(lock_);
}

}  // namespace perf
