/**
 * This file is part of the CernVM File System.
 */

#include "payload_processor.h"

#include <fcntl.h>
#include <unistd.h>
#include <vector>

#include "logging.h"
#include "util/posix.h"
#include "util/string.h"

namespace receiver {

PayloadProcessor::PayloadProcessor()
    : pending_files_(), current_repo_(), num_errors_(0) {}

PayloadProcessor::~PayloadProcessor() {}

PayloadProcessor::Result PayloadProcessor::Process(
    int fdin, const std::string& digest_base64, const std::string& path,
    uint64_t header_size) {
  const size_t first_slash_idx = path.find('/', 0);

  current_repo_ = path.substr(0, first_slash_idx);

  std::string header_digest;
  if (!Debase64(digest_base64, &header_digest)) {
    return kOtherError;
  }

  // Set up object pack deserialization
  shash::Any digest = shash::MkFromHexPtr(shash::HexPtr(header_digest));
  ObjectPackConsumer deserializer(digest, header_size);
  deserializer.RegisterListener(&PayloadProcessor::ConsumerEventCallback, this);

  int nb = 0;
  ObjectPackBuild::State consumer_state = ObjectPackBuild::kStateContinue;
  do {
    std::vector<unsigned char> buffer(4096, 0);

    nb = read(fdin, &buffer[0], buffer.size());
    consumer_state = deserializer.ConsumeNext(nb, &buffer[0]);
    if (consumer_state != ObjectPackBuild::kStateContinue &&
        consumer_state != ObjectPackBuild::kStateDone) {
      LogCvmfs(kLogReceiver, kLogCustom1,
               "Error %d encountered when consuming object pack.",
               consumer_state);
      break;
    }
  } while (nb > 0 && consumer_state != ObjectPackBuild::kStateDone);

  if (GetNumErrors() > 0) {
    return kOtherError;
  }

  assert(pending_files_.empty());

  return kSuccess;
}

void PayloadProcessor::ConsumerEventCallback(
    const ObjectPackBuild::Event& event) {
  std::string path("");

  if (event.object_type == ObjectPack::kCas) {
    path = event.id.MakePath();
  } else if (event.object_type == ObjectPack::kNamed) {
    path = event.object_name;
  } else {
    // kEmpty - this is an error.
    LogCvmfs(kLogReceiver, kLogCustom1, "Event received with unknown object.");
    num_errors_++;
    return;
  }

  FileIterator it = pending_files_.find(event.id);
  if (it == pending_files_.end()) {
    // New file to unpack
    // Create a temporary path
    std::string temp_dir = "/srv/cvmfs/" + current_repo_ + "/data/txn";
    const std::string tmp_path = CreateTempPath(temp_dir, 0666);
    if (tmp_path.empty()) {
      LogCvmfs(kLogReceiver, kLogCustom1, "Unable to create temporary path.");
      num_errors_++;
      return;
    }

    FileInfo info;
    info.temp_path = tmp_path;
    info.total_size = event.size;
    info.current_size = 0;

    pending_files_[event.id] = info;
  }

  FileInfo& info = pending_files_[event.id];

  int fdout = open(info.temp_path.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0600);
  if (fdout == -1) {
    LogCvmfs(kLogReceiver, kLogCustom1,
             "Unable to open temporary output file: %s",
             info.temp_path.c_str());
    return;
  }

  if (!WriteFile(fdout, event.buf, event.buf_size)) {
    LogCvmfs(kLogReceiver, kLogCustom1, "Unable to write %s",
             info.temp_path.c_str());
    num_errors_++;
    unlink(info.temp_path.c_str());
    close(fdout);
    return;
  }
  close(fdout);

  info.current_size += event.buf_size;

  if (info.current_size == info.total_size) {
    const std::string dest = "/srv/cvmfs/" + current_repo_ + "/data/" + path;

    // Atomically move to final destination
    // TODO(radu): It would be nice to hook this into the spooler/uploader
    // components, allowing, for instance to upload from the gateway to S3
    if (FileExists(dest)) {
      unlink(dest.c_str());
    }
    if (RenameFile(info.temp_path.c_str(), dest.c_str())) {
      LogCvmfs(kLogReceiver, kLogCustom1,
               "Unable to move file to final destination: %s", dest.c_str());
      num_errors_++;
      return;
    }

    pending_files_.erase(event.id);

    shash::Any file_hash(shash::kSha1);
    shash::HashFile(dest, &file_hash);

    if (file_hash != event.id) {
      LogCvmfs(kLogReceiver, kLogCustom0,
               "PayloadProcessor - Hash mismatch for unpacked file: event "
               "size: %ld, file size: %ld, event hash: %s, file hash: %s",
               event.size, GetFileSize(dest), event.id.ToString(true).c_str(),
               file_hash.ToString(true).c_str());
      num_errors_++;
      return;
    }
  }
}

bool PayloadProcessor::WriteFile(int fd, const void* const buf,
                                 size_t buf_size) {
  return SafeWrite(fd, buf, buf_size);
}

int PayloadProcessor::RenameFile(const std::string& old_name,
                                 const std::string& new_name) {
  return rename(old_name.c_str(), new_name.c_str());
}

}  // namespace receiver
