/**
 * This file is part of the CernVM File System.
 */

#include "upload_local.h"

#include <errno.h>

#include "logging.h"
#include "compression.h"
#include "util.h"

using namespace upload;


LocalSpooler::LocalSpooler(const SpoolerDefinition &spooler_definition) :
  AbstractSpooler(spooler_definition),
  upstream_path_(spooler_definition.spooler_description)
{
  assert (spooler_definition.IsValid() &&
          spooler_definition.driver_type == SpoolerDefinition::Local);

  atomic_init32(&copy_errors_);
}


unsigned int LocalSpooler::GetNumberOfErrors() const {
  return AbstractSpooler::GetNumberOfErrors() + atomic_read32(&copy_errors_);
}


void LocalSpooler::Copy(const std::string &local_path,
                        const std::string &remote_path) {
  const std::string destination_path = upstream_path_ + "/" + remote_path;

  int retcode = 0;
  if (move()) {
    int retval = rename(local_path.c_str(), destination_path.c_str());
    retcode    = (retval == 0) ? 0 : errno;
  } else {
    int retval = CopyPath2Path(local_path, destination_path);
    retcode    = retval ? 0 : 100;
  }

  if (retcode != 0) {
    atomic_inc32(&copy_errors_);
  }

  const SpoolerResult result(retcode, local_path);
  JobDone(result);
}
