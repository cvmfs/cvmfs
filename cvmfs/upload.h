/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_H_
#define CVMFS_UPLOAD_H_

#include <pthread.h>
#include <stdint.h>

#include <cstdio>
#include <string>
#include "atomic.h"

namespace upload {
  
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
  typedef void (*SpoolerCallback)(const std::string &path, int retval,
                                  const std::string &digest);
  
  Spooler(const std::string &fifo_paths, const std::string &fifo_digests);
  ~Spooler();
  bool Connect();
  void SetCallback(SpoolerCallback *value) { spooler_callback_ = value; }
  SpoolerCallback *spooler_callback() { return spooler_callback_; }
  void SpoolProcess(const std::string &local_path, 
                    const std::string &remote_dir,
                    const std::string &file_postfix);
  void SpoolCopy(const std::string &local_path, const std::string &remote_path);
  
  bool IsIdle() { return atomic_read64(&num_pending_) == 0; }
  uint64_t num_errors() { return atomic_read64(&num_errors_); }

 private:
  static void *MainReceive(void *caller);
  
  atomic_int64 num_pending_;
  atomic_int64 num_errors_;
  std::string fifo_paths_;
  std::string fifo_digests_;
  SpoolerCallback *spooler_callback_;
  bool connected_;
  int fd_paths_;
  int fd_digests_;
  FILE *fdigests_;
  pthread_t thread_receive_;
};


int MainLocalSpooler(const std::string &fifo_paths, 
                     const std::string &fifo_digests,
                     const std::string &upstream_basedir);
  
  
class Forklift {
 public:
  Forklift(const std::string &entry_point) : 
    entry_point_(entry_point), last_error_("OK") { }
  virtual ~Forklift() { }
  
  virtual bool Connect() = 0;
  virtual bool Put(const std::string &local_path, 
                   const std::string &remote_path) const = 0;
  virtual bool Move(const std::string &local_path, 
                    const std::string &remote_path) const = 0;
  virtual std::string GetLastError() const { return last_error_; };
 protected:  
  std::string entry_point_;
  mutable std::string last_error_;
};  // class Forklift
   

class ForkliftLocal : public Forklift {
 public:
  ForkliftLocal(const std::string &entry_point) : 
    Forklift(entry_point) { }
  virtual ~ForkliftLocal() { }

  bool Connect();
  bool Put(const std::string &local_path, const std::string &remote_path) const;
  bool Move(const std::string &local_path, 
            const std::string &remote_path) const;
};  // class ForkliftLocal
  
  
/**
 * Hands path names over to a local pipe.  The other end takes care of further
 * processing.
 */
class ForkliftPathPipe : public Forklift {
 public:
  ForkliftPathPipe(const std::string &entry_point) : 
    Forklift(entry_point) { pipe_fd_ = -1; }
  virtual ~ForkliftPathPipe() { if (pipe_fd_ >= 0) close(pipe_fd_); }
  
  bool Connect();
  bool Put(const std::string &local_path, const std::string &remote_path) const;
  bool Move(const std::string &local_path, 
            const std::string &remote_path) const;

 private:
  int pipe_fd_; 
};  // class ForkliftPathPipe
  
  
Forklift *CreateForklift(const std::string &upstream);
   
}  // namespace upload

#endif  // CVMFS_UPLOAD_H_
