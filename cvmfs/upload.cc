/**
 * This file is part of the CernVM File System.
 *
 * The Forlift classes provide an interface to push files to a storage 
 * component.
 */

#include "upload.h"

#include <errno.h>
#include <cstdio>

#include "compression.h"
#include "util.h"

using namespace std;  // NOLINT

namespace upload {
  
Forklift *CreateForklift(const std::string &upstream) {
  if (upstream.find("local:") == 0) {
    return new upload::ForkliftLocal(upstream.substr(6 /* length "local:" */));
  }
  return NULL;
}
   
  
bool ForkliftLocal::Connect() {
  bool retval = DirectoryExists(base_directory_);
  if (!retval)
    last_error_ = "directory " + base_directory_ + " does not exist";
  else
    last_error_ = "OK";
  return retval;
}
  
  
bool ForkliftLocal::Put(const string &local_path, 
                        const string &remote_path) const 
{
  bool retval = CopyPath2Path(local_path, base_directory_+remote_path);
  if (retval)
    last_error_ = "OK";
  else
    last_error_ = "copy failure";
  return retval;
}
  
  
bool ForkliftLocal::Move(const string &local_path, 
                         const string &remote_path) const 
{
  int retval = rename(local_path.c_str(), 
                      (base_directory_+remote_path).c_str());
  if (retval == 0) {
    last_error_ = "OK";
    return true;
  }
  
  if (errno == EXDEV) {
    retval = Put(local_path, remote_path);
    if (retval)
      unlink(local_path.c_str());
    return retval;
  }
  
  last_error_ = "failed to rename (" + StringifyInt(errno) + ")";
  return false;
}   
   
}  // namespace upload
