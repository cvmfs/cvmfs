#include "upload_local.h"

#include <errno.h>

#include "logging.h"
#include "compression.h"
#include "util.h"

using namespace upload;

LocalSpoolerBackend::LocalSpoolerBackend(const std::string &upstream_path) :
  upstream_path_(upstream_path),
  initialized_(false)
{}


bool LocalSpoolerBackend::Initialize() {
  bool retval = AbstractSpoolerBackend::Initialize();
  if (!retval)
    return false;

  if (upstream_path_.empty()) {
    LogCvmfs(kLogSpooler, kLogWarning, "upstream path not configured");
    return false;
  }

  LogCvmfs(kLogSpooler, kLogVerboseMsg, "successfully initialized local "
                                        "spooler backend");

  initialized_ = true;
  return true;
}


void LocalSpoolerBackend::Copy(const std::string &local_path,
                               const std::string &remote_path,
                               const bool move) {
  const std::string destination_path = upstream_path_ + "/" + remote_path;

  int retcode = 0;

  if (move) {
    int retval = rename(local_path.c_str(), destination_path.c_str());
    retcode    = (retval == 0) ? 0 : errno;
  } else {
    int retval = CopyPath2Path(local_path, destination_path);
    retcode    = retval ? 0 : 100;
  }

  SendResult(retcode, local_path);
}


void LocalSpoolerBackend::Process(const std::string &local_path,
                                  const std::string &remote_dir,
                                  const std::string &file_suffix,
                                  const bool move) {
  hash::Any compressed_hash(hash::kSha1);
  const std::string remote_path = upstream_path_ + "/" + remote_dir;

  std::string tmp_path;
  FILE *fcas = CreateTempFile(remote_path + "/cvmfs", 0777, "w",
                              &tmp_path);

  int retcode = 0;

  if (fcas == NULL) {
    retcode = 103;
  } else {
    int retval = zlib::CompressPath2File(local_path, fcas,
                                         &compressed_hash);
    retcode = retval ? 0 : 103;
    fclose(fcas);
    if (retval) {
      const std::string cas_path = remote_path + compressed_hash.MakePath(1, 2)
                              + file_suffix;
      retval = rename(tmp_path.c_str(), cas_path.c_str());
      if (retval != 0) {
        unlink(tmp_path.c_str());
        retcode = 104;
      }
    }
  }
  if (move) {
    if (unlink(local_path.c_str()) != 0)
      retcode = 105;
  }

  SendResult(retcode, local_path, compressed_hash);
}


bool LocalSpoolerBackend::IsReady() const {
  const bool ready = AbstractSpoolerBackend::IsReady();
  return ready && initialized_;
}
