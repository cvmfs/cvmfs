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
  upstream_path_(spooler_definition.spooler_configuration)
{
  assert (spooler_definition.IsValid() &&
          spooler_definition.driver_type == SpoolerDefinition::Local);

  atomic_init32(&copy_errors_);
}


bool LocalSpooler::WillHandle(const SpoolerDefinition &spooler_definition) {
  return spooler_definition.driver_type == SpoolerDefinition::Local;
}


unsigned int LocalSpooler::GetNumberOfErrors() const {
  return AbstractSpooler::GetNumberOfErrors() + atomic_read32(&copy_errors_);
}


void LocalSpooler::Upload(const FileProcessor::Results &data) {
  int retcode = 0;
  int move_retcode = 0;

  FileChunks uploaded_chunks;

  if (data.IsChunked()) {
    TemporaryFileChunks::const_iterator i    = data.file_chunks.begin();
    TemporaryFileChunks::const_iterator iend = data.file_chunks.end();
    for (; i != iend; ++i) {
      move_retcode = Move(i->temporary_path(),
                          "data" + i->content_hash().MakePath(1,2) + FileChunk::kChecksumSuffix);
      if (move_retcode != 0) {
        retcode = move_retcode;
        break;
      }

      uploaded_chunks.push_back(static_cast<FileChunk>(*i));
    }
  }

  if (retcode == 0) {
    retcode = Move(data.bulk_file.temporary_path(),
                   "data" + data.bulk_file.content_hash().MakePath(1,2));
  }

  const SpoolerResult result(retcode,
                             data.local_path,
                             data.bulk_file.content_hash(),
                             uploaded_chunks);
  JobDone(result);
}

void LocalSpooler::Upload(const std::string &local_path,
                          const std::string &remote_path) {
  const int retcode = Copy(local_path, remote_path);

  const SpoolerResult result(retcode, local_path);
  JobDone(result);
}

int LocalSpooler::Copy(const std::string &local_path,
                       const std::string &remote_path) const {
  const std::string destination_path = upstream_path_ + "/" + remote_path;

  int retval  = CopyPath2Path(local_path, destination_path);
  int retcode = retval ? 0 : 100;

  if (retcode != 0) {
    atomic_inc32(&copy_errors_);
  }

  return retcode;
}

int LocalSpooler::Move(const std::string &local_path,
                       const std::string &remote_path) const {
  const std::string destination_path = upstream_path_ + "/" + remote_path;

  int retval  = rename(local_path.c_str(), destination_path.c_str());
  int retcode = (retval == 0) ? 0 : errno;
  if (retcode != 0) {
    atomic_inc32(&copy_errors_);
  }

  return retcode;
}
