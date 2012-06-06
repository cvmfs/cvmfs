/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_H_
#define CVMFS_UPLOAD_H_

#include <string>

namespace upload {

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
};  // class Forklift
  
  
Forklift *CreateForklift(const std::string &upstream);
   
}  // namespace upload

#endif  // CVMFS_UPLOAD_H_
