/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_H_
#define CVMFS_UPLOAD_H_

#include <pthread.h>
#include <stdint.h>
#include <unistd.h>

#include <cstdio>
#include <string>

#include "atomic.h"

namespace upload {


class BackendStat {
 public:
  BackendStat(const std::string &base_path) { base_path_ = base_path; }
  virtual ~BackendStat() { }
  virtual bool Stat(const std::string &path) = 0;
 protected:
  std::string base_path_;
};


class LocalStat : public BackendStat {
 public:
  LocalStat(const std::string &base_path) : BackendStat(base_path) { }
  bool Stat(const std::string &path);
};


/**
 * Encapsulates the callback function that handles responses from the external
 * Spooler.
 */
class SpoolerCallback {
 public:
  virtual void Callback(const std::string &path, int retval,
                        const std::string &digest) = 0;
  virtual ~SpoolerCallback() { }
};


/**
 * Notifies the external spooler about files that are supposed to be uploaded to
 * the upstream storage.
 * Collects the results.  Files should only be sent once to the spooler,
 * otherwise there is no way to distinguish the results.
 * Internally, input and output work with two named pipes.
 * The callback function is called from a parallel thread.
 */
class Spooler {
 public:
  Spooler(const std::string &fifo_paths, const std::string &fifo_digests);
  ~Spooler();
  bool Connect();
  void SetCallback(SpoolerCallback *value) { spooler_callback_ = value; }
  void UnsetCallback() { delete spooler_callback_; spooler_callback_ = NULL; }
  SpoolerCallback *spooler_callback() { return spooler_callback_; }
  void SpoolProcess(const std::string &local_path,
                    const std::string &remote_dir,
                    const std::string &file_postfix);
  void SpoolCopy(const std::string &local_path, const std::string &remote_path);
  void EndOfTransaction();

  void set_move_mode(const bool mode) { move_mode_ = mode; }

  bool IsIdle() { return atomic_read64(&num_pending_) == 0; }
  void WaitFor();
  uint64_t num_errors() { return atomic_read64(&num_errors_); }

 private:
  static void *MainReceive(void *caller);

  atomic_int64 num_pending_;
  atomic_int64 num_errors_;
  std::string fifo_paths_;
  std::string fifo_digests_;
  SpoolerCallback *spooler_callback_;
  bool connected_;
  bool move_mode_;
  int fd_paths_;
  int fd_digests_;
  FILE *fdigests_;
  pthread_t thread_receive_;
};


int MainLocalSpooler(const std::string &fifo_paths,
                     const std::string &fifo_digests,
                     const std::string &upstream_basedir);

int MainRiakSpooler(const std::string &fifo_paths,
                    const std::string &fifo_digests);

/**
 * Starts a spooler process and the creates the corresponding object
 * from a definition string like "local:/dir/to/repo,/path/pipe,/digest/pipe"
 */
Spooler *MakeSpoolerEnsemble(const std::string &spooler_definition);
BackendStat *GetBackendStat(const std::string &spooler_definition);

}  // namespace upload

#endif  // CVMFS_UPLOAD_H_
