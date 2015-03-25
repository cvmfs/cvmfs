/**
 * This file is part of the CernVM File System.
 */

#include "statistics.h"

#include <cassert>

#include "smalloc.h"
#include "util.h"
#include "util_concurrency.h"

using namespace std;  // NOLINT

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

namespace perf {

Counter::Counter() { atomic_init64(&counter_); }
void Counter::Inc() { atomic_inc64(&counter_); }
void Counter::Dec() { atomic_dec64(&counter_); }
int64_t Counter::Get() { return atomic_read64(&counter_); }
void Counter::Set(const int64_t val) { atomic_write64(&counter_, val); }
int64_t Counter::Xadd(const int64_t delta) {
  return atomic_xadd64(&counter_, delta);
}

std::string Counter::Print() { return StringifyInt(Get()); }
std::string Counter::PrintK() { return StringifyInt(Get() / 1000); }
std::string Counter::PrintKi() { return StringifyInt(Get() / 1024); }
std::string Counter::PrintM() { return StringifyInt(Get() / (1000 * 1000)); }
std::string Counter::PrintMi() { return StringifyInt(Get() / (1024 * 1024)); }
std::string Counter::PrintRatio(Counter divider) {
  double enumerator_value = Get();
  double divider_value = divider.Get();
  return StringifyDouble(enumerator_value / divider_value);
}


//-----------------------------------------------------------------------------

Counter *Statistics::Lookup(const std::string &name) {
  MutexLockGuard lock_guard(lock_);
  map<string, CounterInfo *>::const_iterator i = counters_.find(name);
  if (i != counters_.end())
    return &i->second->counter;
  return NULL;
}


string Statistics::LookupDesc(const std::string &name) {
  MutexLockGuard lock_guard(lock_);
  map<string, CounterInfo *>::const_iterator i = counters_.find(name);
  if (i != counters_.end())
    return i->second->desc;
  return "";
}


string Statistics::PrintList(const PrintOptions print_options) {
  string result;
  if (print_options == kPrintHeader)
    result += "Name|Value|Description\n";

  MutexLockGuard lock_guard(lock_);
  for (map<string, CounterInfo *>::const_iterator i = counters_.begin(),
       iEnd = counters_.end(); i != iEnd; ++i)
  {
    result += i->first + "|" + StringifyInt(i->second->counter.Get()) +
              "|" + i->second->desc + "\n";
  }
  return result;
}


Counter *Statistics::Register(const string &name, const string &desc) {
  MutexLockGuard lock_guard(lock_);
  assert(counters_.find(name) == counters_.end());
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


#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif
