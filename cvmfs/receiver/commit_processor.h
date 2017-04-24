/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_RECEIVER_COMMIT_PROCESSOR_H_
#define CVMFS_RECEIVER_COMMIT_PROCESSOR_H_

#include <string>

namespace receiver {

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
  enum Result { kSuccess, kPathViolation };

  CommitProcessor();
  virtual ~CommitProcessor();

  Result Process(const std::string& lease_path);

  int GetNumErrors() const { return num_errors_; }

 private:
  int num_errors_;
};

}  // namespace receiver

#endif  // CVMFS_RECEIVER_COMMIT_PROCESSOR_H_