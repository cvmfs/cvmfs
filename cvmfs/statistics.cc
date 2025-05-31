/**
 * This file is part of the CernVM File System.
 */

#include "statistics.h"

#include <algorithm>
#include <cassert>

#include "json_document_write.h"
#include "util/concurrency.h"
#include "util/platform.h"
#include "util/smalloc.h"
#include "util/string.h"

using namespace std;  // NOLINT

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

namespace perf {

std::string Counter::ToString() { return StringifyInt(Get()); }
std::string Counter::Print() { return StringifyInt(Get()); }
std::string Counter::PrintK() { return StringifyInt(Get() / 1000); }
std::string Counter::PrintKi() { return StringifyInt(Get() / 1024); }
std::string Counter::PrintM() { return StringifyInt(Get() / (1000 * 1000)); }
std::string Counter::PrintMi() { return StringifyInt(Get() / (1024 * 1024)); }
std::string Counter::PrintRatio(Counter divider) {
  const double enumerator_value = Get();
  const double divider_value = divider.Get();
  return StringifyDouble(enumerator_value / divider_value);
}


//-----------------------------------------------------------------------------


/**
 * Creates a new Statistics binder which maintains the same Counters as the
 * existing one.  Changes to those counters are visible in both Statistics
 * objects. The child can then independently add more counters. CounterInfo
 * objects are reference counted and deleted when all the statistics objects
 * dealing with it are destroyed.
 */
Statistics *Statistics::Fork() {
  Statistics *child = new Statistics();

  const MutexLockGuard lock_guard(lock_);
  for (map<string, CounterInfo *>::iterator i = counters_.begin(),
                                            iEnd = counters_.end();
       i != iEnd;
       ++i) {
    atomic_inc32(&i->second->refcnt);
  }
  child->counters_ = counters_;

  return child;
}


Counter *Statistics::Lookup(const std::string &name) const {
  const MutexLockGuard lock_guard(lock_);
  const map<string, CounterInfo *>::const_iterator i = counters_.find(name);
  if (i != counters_.end())
    return &i->second->counter;
  return NULL;
}


string Statistics::LookupDesc(const std::string &name) {
  const MutexLockGuard lock_guard(lock_);
  const map<string, CounterInfo *>::const_iterator i = counters_.find(name);
  if (i != counters_.end())
    return i->second->desc;
  return "";
}

string Statistics::PrintList(const PrintOptions print_options) {
  string result;
  if (print_options == kPrintHeader)
    result += "Name|Value|Description\n";

  const MutexLockGuard lock_guard(lock_);
  for (map<string, CounterInfo *>::const_iterator i = counters_.begin(),
                                                  iEnd = counters_.end();
       i != iEnd;
       ++i) {
    result += i->first + "|" + i->second->counter.ToString() + "|"
              + i->second->desc + "\n";
  }
  return result;
}

/**
 * Converts statistics counters into JSON string in following format
 * {
 *   "name_major1": {
 *     "counter1": val1,
 *     "counter2": val2
 *   },
 *   "name_major2": {
 *     "counter3": val3
 *   }
 * }
 */
string Statistics::PrintJSON() {
  const MutexLockGuard lock_guard(lock_);

  JsonStringGenerator json_statistics;

  // Make use of std::map key ordering and add counters namespace by namespace
  JsonStringGenerator json_statistics_namespace;
  std::string last_namespace = "";
  for (map<string, CounterInfo *>::const_iterator i = counters_.begin(),
                                                  iEnd = counters_.end();
       i != iEnd;
       ++i) {
    std::vector<std::string> tokens = SplitString(i->first, '.');

    if (tokens[0] != last_namespace) {
      if (last_namespace != "") {
        json_statistics.AddJsonObject(
            last_namespace, json_statistics_namespace.GenerateString());
      }
      json_statistics_namespace.Clear();
    }

    json_statistics_namespace.Add(tokens[1], i->second->counter.Get());

    last_namespace = tokens[0];
  }
  if (last_namespace != "") {
    json_statistics.AddJsonObject(last_namespace,
                                  json_statistics_namespace.GenerateString());
  }

  return json_statistics.GenerateString();
}

/**
 * Snapshot current state of the counters.
 * Elements will either be updated or inserted into the map.
 *
 * Note: This function does NOT clear previous elements part of the map.
 *
 * Returns map of the updated counters and the timestamp of the snapshot.
 */
void Statistics::SnapshotCounters(std::map<std::string, int64_t> *counters,
                                  uint64_t *timestamp_ns) {
  const MutexLockGuard lock_guard(lock_);
  *timestamp_ns = platform_realtime_ns();
  for (map<string, CounterInfo *>::const_iterator i = counters_.begin(),
                                                  iEnd = counters_.end();
       i != iEnd;
       ++i) {
    // modify or insert
    (*counters)[i->first] = (*i->second).counter.Get();
  }
}

Counter *Statistics::Register(const string &name, const string &desc) {
  const MutexLockGuard lock_guard(lock_);
  assert(counters_.find(name) == counters_.end());
  CounterInfo *counter_info = new CounterInfo(desc);
  counters_[name] = counter_info;
  return &counter_info->counter;
}


Statistics::Statistics() {
  lock_ = reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  const int retval = pthread_mutex_init(lock_, NULL);
  assert(retval == 0);
}


Statistics::~Statistics() {
  for (map<string, CounterInfo *>::iterator i = counters_.begin(),
                                            iEnd = counters_.end();
       i != iEnd;
       ++i) {
    const int32_t old_value = atomic_xadd32(&i->second->refcnt, -1);
    if (old_value == 1)
      delete i->second;
  }
  pthread_mutex_destroy(lock_);
  free(lock_);
}


//------------------------------------------------------------------------------


/**
 * If necessary, capacity_s is extended to be a multiple of resolution_s
 */
Recorder::Recorder(uint32_t resolution_s, uint32_t capacity_s)
    : last_timestamp_(0), capacity_s_(capacity_s), resolution_s_(resolution_s) {
  assert((resolution_s > 0) && (capacity_s > resolution_s));
  const bool has_remainder = (capacity_s_ % resolution_s_) != 0;
  if (has_remainder) {
    capacity_s_ += resolution_s_ - (capacity_s_ % resolution_s_);
  }
  no_bins_ = capacity_s_ / resolution_s_;
  bins_.reserve(no_bins_);
  for (unsigned i = 0; i < no_bins_; ++i)
    bins_.push_back(0);
}


void Recorder::Tick() { TickAt(platform_monotonic_time()); }


void Recorder::TickAt(uint64_t timestamp) {
  const uint64_t bin_abs = timestamp / resolution_s_;
  const uint64_t last_bin_abs = last_timestamp_ / resolution_s_;

  // timestamp in the past: don't update last_timestamp_
  if (bin_abs < last_bin_abs) {
    // Do we still remember this event?
    if ((last_bin_abs - bin_abs) < no_bins_)
      bins_[bin_abs % no_bins_]++;
    return;
  }

  if (last_bin_abs == bin_abs) {
    bins_[bin_abs % no_bins_]++;
  } else {
    // When clearing bins between last_timestamp_ and now, avoid cycling the
    // ring buffer multiple times.
    const unsigned max_bins_clear =
        std::min(bin_abs, last_bin_abs + no_bins_ + 1);
    for (uint64_t i = last_bin_abs + 1; i < max_bins_clear; ++i)
      bins_[i % no_bins_] = 0;
    bins_[bin_abs % no_bins_] = 1;
  }

  last_timestamp_ = timestamp;
}


uint64_t Recorder::GetNoTicks(uint32_t retrospect_s) const {
  const uint64_t now = platform_monotonic_time();
  if (retrospect_s > now)
    retrospect_s = now;

  const uint64_t last_bin_abs = last_timestamp_ / resolution_s_;
  const uint64_t past_bin_abs = (now - retrospect_s) / resolution_s_;
  const int64_t min_bin_abs =
      std::max(past_bin_abs,
               (last_bin_abs < no_bins_) ? 0 : (last_bin_abs - (no_bins_ - 1)));
  uint64_t result = 0;
  for (int64_t i = last_bin_abs; i >= min_bin_abs; --i) {
    result += bins_[i % no_bins_];
  }

  return result;
}


//------------------------------------------------------------------------------


void MultiRecorder::AddRecorder(uint32_t resolution_s, uint32_t capacity_s) {
  recorders_.push_back(Recorder(resolution_s, capacity_s));
}


uint64_t MultiRecorder::GetNoTicks(uint32_t retrospect_s) const {
  const unsigned N = recorders_.size();
  for (unsigned i = 0; i < N; ++i) {
    if ((recorders_[i].capacity_s() >= retrospect_s) || (i == (N - 1))) {
      return recorders_[i].GetNoTicks(retrospect_s);
    }
  }
  return 0;
}


void MultiRecorder::Tick() {
  const uint64_t now = platform_monotonic_time();
  for (unsigned i = 0; i < recorders_.size(); ++i)
    recorders_[i].TickAt(now);
}


void MultiRecorder::TickAt(uint64_t timestamp) {
  for (unsigned i = 0; i < recorders_.size(); ++i)
    recorders_[i].TickAt(timestamp);
}

}  // namespace perf


#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif
