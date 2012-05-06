/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_H_
#define CVMFS_UPLOAD_H_

#include <string>

namespace upload {

class Forklift {
 public:
  Forklift(const std::string &base_directory) : 
    base_directory_(base_directory), last_error_("OK") { }
  virtual ~Forklift() { }
  
  virtual bool Connect() = 0;
  virtual bool Put(const std::string &local_path, 
                   const std::string &remote_path) const = 0;
  virtual bool Move(const std::string &local_path, 
                    const std::string &remote_path) const = 0;
  virtual std::string GetLastError() const { return last_error_; };
 protected:  
  std::string base_directory_;
  mutable std::string last_error_;
};  // class Forklift
   

class ForkliftLocal : public Forklift {
 public:
  ForkliftLocal(const std::string &base_directory) : 
    Forklift(base_directory) { }
  virtual ~ForkliftLocal() { }

  bool Connect();
  bool Put(const std::string &local_path, const std::string &remote_path) const;
  bool Move(const std::string &local_path, 
            const std::string &remote_path) const;
};  // class Forklift
  
  
Forklift *CreateForklift(const std::string &upstream);
   
}  // namespace upload

#endif  // CVMFS_UPLOAD_H_
