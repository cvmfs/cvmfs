/**
 * This file is part of the CernVM File System.
 */

#include "payload_processor.h"

#include <fcntl.h>
#include <unistd.h>
#include <vector>

#include "logging.h"
#include "params.h"
#include "util/posix.h"
#include "util/string.h"

namespace receiver {

PayloadProcessor::PayloadProcessor()
    : pending_files_(),
      current_repo_(),
      spooler_(),
      temp_dir_(),
      num_errors_(0) {}

PayloadProcessor::~PayloadProcessor() {}

PayloadProcessor::Result PayloadProcessor::Process(
    int fdin, const std::string& header_digest, const std::string& path,
    uint64_t header_size) {
  LogCvmfs(kLogReceiver, kLogSyslog,
           "PayloadProcessor - receiving path: %s, header digest: %s, header "
           "size: %ld",
           path.c_str(), header_digest.c_str(), header_size);

  const size_t first_slash_idx = path.find('/', 0);

  current_repo_ = path.substr(0, first_slash_idx);

  Result init_result = Initialize();
  if (init_result != kSuccess) {
    return init_result;
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
      LogCvmfs(kLogReceiver, kLogSyslogErr,
               "Error %d encountered when consuming object pack.",
               consumer_state);
      break;
    }
  } while (nb > 0 && consumer_state != ObjectPackBuild::kStateDone);

  if (GetNumErrors() > 0) {
    return kOtherError;
  }

  assert(pending_files_.empty());

  Finalize();

  deserializer.UnregisterListeners();

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
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "Event received with unknown object.");
    num_errors_++;
    return;
  }

  FileIterator it = pending_files_.find(event.id);
  if (it == pending_files_.end()) {
    // New file to unpack
    const std::string tmp_path =
        CreateTempPath(temp_dir_->dir() + "/payload", 0666);
    if (tmp_path.empty()) {
      LogCvmfs(kLogReceiver, kLogSyslogErr, "Unable to create temporary path.");
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
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "Unable to open temporary output file: %s",
             info.temp_path.c_str());
    return;
  }

  if (!WriteFile(fdout, event.buf, event.buf_size)) {
    LogCvmfs(kLogReceiver, kLogSyslogErr, "Unable to write %s",
             info.temp_path.c_str());
    num_errors_++;
    unlink(info.temp_path.c_str());
    close(fdout);
    return;
  }
  close(fdout);

  info.current_size += event.buf_size;

  if (info.current_size == info.total_size) {
    shash::Any file_hash(shash::kSha1);
    shash::HashFile(info.temp_path, &file_hash);

    if (file_hash != event.id) {
      LogCvmfs(kLogReceiver, kLogSyslogErr,
               "PayloadProcessor - Hash mismatch for unpacked file: event "
               "size: %ld, file size: %ld, event hash: %s, file hash: %s",
               event.size, GetFileSize(info.temp_path),
               event.id.ToString(true).c_str(),
               file_hash.ToString(true).c_str());
      num_errors_++;
      return;
    }

    Upload(info.temp_path, "data/" + path);

    pending_files_.erase(event.id);
  }
}

PayloadProcessor::Result PayloadProcessor::Initialize() {
  Params params;
  if (!GetParamsFromFile(current_repo_, &params)) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "Error: Could not get configuration parameters.");
    return kOtherError;
  }

  const std::string spooler_temp_dir =
      GetSpoolerTempDir(params.spooler_configuration);
  assert(!spooler_temp_dir.empty());
  temp_dir_ = RaiiTempDir::Create(spooler_temp_dir + "/payload_processor");

  upload::SpoolerDefinition definition(
      params.spooler_configuration, params.hash_alg, params.compression_alg,
      params.generate_legacy_bulk_chunks, params.use_file_chunking,
      params.min_chunk_size, params.avg_chunk_size, params.max_chunk_size,
      "dummy_token", "dummy_key");

  spooler_.Destroy();
  spooler_ = upload::Spooler::Construct(definition);

  return kSuccess;
}

void PayloadProcessor::Finalize() {
  spooler_->WaitForUpload();
  temp_dir_.Destroy();
}

void PayloadProcessor::Upload(const std::string& source,
                              const std::string& dest) {
  spooler_->Upload(source, dest);
}

bool PayloadProcessor::WriteFile(int fd, const void* const buf,
                                 size_t buf_size) {
  return SafeWrite(fd, buf, buf_size);
}

}  // namespace receiver
