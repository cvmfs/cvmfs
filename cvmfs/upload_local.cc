/**
 * This file is part of the CernVM File System.
 */

#include "upload_local.h"

#include <errno.h>

#include "logging.h"
#include "compression.h"
#include "util.h"

using namespace upload;


LocalUploader::LocalUploader(const SpoolerDefinition &spooler_definition) :
  AbstractUploader(spooler_definition),
  upstream_path_(spooler_definition.spooler_configuration)
{
  assert (spooler_definition.IsValid() &&
          spooler_definition.driver_type == SpoolerDefinition::Local);

  atomic_init32(&copy_errors_);
}


bool LocalUploader::WillHandle(const SpoolerDefinition &spooler_definition) {
  return spooler_definition.driver_type == SpoolerDefinition::Local;
}


unsigned int LocalUploader::GetNumberOfErrors() const {
  return atomic_read32(&copy_errors_);
}


void LocalUploader::Upload(const std::string &local_path,
                           const std::string &remote_path,
                           const callback_t   *callback) {
  const int retcode = Copy(local_path, remote_path);
  Respond(callback, retcode, local_path);
}


void LocalUploader::Upload(const std::string  &local_path,
                           const hash::Any    &content_hash,
                           const std::string  &hash_suffix,
                           const callback_t   *callback) {
  const int retcode = Move(local_path,
                           "data" + content_hash.MakePath(1,2) + hash_suffix);
  Respond(callback, retcode, local_path);
}


bool LocalUploader::Peek(const std::string& path) const {
  return FileExists(upstream_path_ + "/" + path);
}


int LocalUploader::Copy(const std::string &local_path,
                        const std::string &remote_path) const {
  const std::string destination_path = upstream_path_ + "/" + remote_path;

  int retval  = CopyPath2Path(local_path, destination_path);
  int retcode = retval ? 0 : 100;

  if (retcode != 0) {
    atomic_inc32(&copy_errors_);
  }

  return retcode;
}


int LocalUploader::Move(const std::string &local_path,
                        const std::string &remote_path) const {
  const std::string destination_path = upstream_path_ + "/" + remote_path;

  int retval  = rename(local_path.c_str(), destination_path.c_str());
  int retcode = (retval == 0) ? 0 : errno;
  if (retcode != 0) {
    atomic_inc32(&copy_errors_);
  }

  return retcode;
}
