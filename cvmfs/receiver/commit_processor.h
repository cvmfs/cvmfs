/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_RECEIVER_COMMIT_PROCESSOR_H_
#define CVMFS_RECEIVER_COMMIT_PROCESSOR_H_

#include <ctime>
#include <string>

#include "repository_tag.h"
#include "server_tool.h"
#include <memory>

namespace receiver {

/**
 * Resolve the gateway-local CVMFS_AUTO_TAG_TIMESPAN configuration value into a
 * Unix timestamp threshold, relative to `now`, in-process and without spawning
 * `date` or any other subprocess. Only the documented relative form
 * "<N> <unit> ago" is supported (e.g. "30 days ago", "1 month ago"); the unit
 * may be sec(ond), min(ute), hour, day, week, month or year, singular or
 * plural. Month and year arithmetic is calendar-aware (via mktime), matching
 * the GNU `date` semantics the publisher uses. Anything else (absolute dates,
 * "N days" without "ago", garbage, ...) returns 0 so the caller skips cleanup.
 *
 * Exposed in the header so it can be unit tested with a fixed `now`.
 */
time_t ParseRelativeTimespan(const std::string &timespan, time_t now);

/**
 * This class is used in the `cvmfs_receiver` tool, on repository gateway
 * machines. The receiver::Reactor class, implementing the event loop of the
 * `cvmfs_receiver` tool, dispatches the handling of the kCommit events
 * to this class.
 *
 * Its responsibility is updating the repository (sub-)catalogs with the changes
 * introduced during the lease. After all the catalogs have been updated, the
 * repository manifest is also updated and resigned.
 */
class CommitProcessor {
 public:
  enum Result {
    kSuccess,
    kError,
    kMergeFailure,
    kMissingReflog,
    kLeaseExpired
  };

  CommitProcessor();
  virtual ~CommitProcessor();

  // Process the committed lease.
  //
  // lease_expiration is the Unix timestamp (seconds) of the commit deadline:
  // the lease expiration minus the gateway's configured safety margin. It is
  // re-checked just before the repository is modified: if the deadline has
  // passed, the commit is not published and kLeaseExpired is returned.
  //
  // virtual so the reactor's MakeCommitProcessor() factory can inject a mock
  // in unit tests.
  //
  // When direct_graft is false (the default) the standard CatalogMergeTool /
  // DiffRec path is used -- identical behaviour to before this parameter was
  // added.
  //
  // When direct_graft is true the experimental fast path is used:
  // new_root_hash is grafted directly into the parent catalog at lease_path via
  // TryGraftNestedCatalog, bypassing DiffRec entirely.  This is only valid when
  // lease_path points to a brand-new directory subtree with no pre-existing
  // entry in the parent catalog.  The dedicated kCommitGraft reactor request is
  // the only caller that sets this to true.
  virtual Result Process(const std::string &lease_path,
                         const shash::Any &old_root_hash,
                         const shash::Any &new_root_hash,
                         const RepositoryTag &tag, int64_t lease_expiration,
                         uint64_t *final_revision, bool direct_graft = false);

  int GetNumErrors() const { return num_errors_; }

  void SetStatistics(perf::Statistics *st, const std::string &start_time);

 private:
  int num_errors_;
  perf::Statistics *statistics_;
  std::string start_time_;
};

}  // namespace receiver

#endif  // CVMFS_RECEIVER_COMMIT_PROCESSOR_H_
