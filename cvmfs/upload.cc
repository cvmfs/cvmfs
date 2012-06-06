/**
 * This file is part of the CernVM File System.
 *
 * The Forlift classes provide an interface to push files to a storage 
 * component.
 */

#include "upload.h"

#include <fcntl.h>
#include <errno.h>

#include <cstdio>

#include "compression.h"
#include "util.h"

using namespace std;  // NOLINT

namespace upload {
  
Forklift *CreateForklift(const std::string &upstream) {
  if (upstream.find("local:") == 0) {
    return new upload::ForkliftLocal(upstream.substr(6 /* length "local:" */));
  } else if (upstream.find("pipe:") == 0) {
    return new upload::ForkliftPathPipe(upstream.substr(5 /* cut "pipe:" */));
  }
  return NULL;
}
   
  
bool ForkliftLocal::Connect() {
  bool retval = DirectoryExists(entry_point_);
  if (!retval)
    last_error_ = "directory " + entry_point_ + " does not exist";
  else
    last_error_ = "OK";
  return retval;
}
  
  
bool ForkliftLocal::Put(const string &local_path, 
                        const string &remote_path) const 
{
  bool retval = CopyPath2Path(local_path, entry_point_+remote_path);
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
                      (entry_point_+remote_path).c_str());
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
  
  
bool ForkliftPathPipe::Connect() {
  pipe_fd_ = open(entry_point_.c_str(), O_WRONLY);
  
  if (pipe_fd_ == -1) {
    last_error_ = "failed to connect to pipe (" + StringifyInt(errno) + ")";
    return false;
  } else {
    last_error_ = "OK";
    return true;
  }
}


bool ForkliftPathPipe::Put(const string &local_path, 
                           const string &remote_path) const 
{
  const string command = local_path + ", " + remote_path + "\n";
  int written = write(pipe_fd_, command.data(), command.length());
  if ((written == -1) || (static_cast<unsigned>(written) != command.length())) {
    last_error_ = "failed to upload file (" + StringifyInt(errno) + ")";
    return false;
  } else {
    last_error_ = "OK";
    return true;
  }
}


bool ForkliftPathPipe::Move(const string &local_path, 
                            const string &remote_path) const 
{
  int retval = Put(local_path, remote_path);
  // TODO: When delete?
  //if (retval)
  //  unlink(local_path.c_str());
  return retval;
}   

   
}  // namespace upload
