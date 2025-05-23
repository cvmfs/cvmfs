/**
 * This file is part of the CernVM File System.
 */

#include "payload_processor.h"

#include <fcntl.h>
#include <unistd.h>

#include <vector>

#include "params.h"
#include "util/logging.h"
#include "util/posix.h"
#include "util/string.h"

namespace {

const size_t kConsumerBuffer = 10 * 1024 * 1024;  // 10 MB

}

namespace receiver {

FileInfo::FileInfo()
    : handle(NULL)
    , total_size(0)
    , current_size(0)
    , hash_context()
    , hash_buffer() { }

FileInfo::FileInfo(const ObjectPackBuild::Event &event)
    : handle(NULL)
    , total_size(event.size)
    , current_size(0)
    , hash_context(shash::ContextPtr(event.id.algorithm))
    , hash_buffer(hash_context.size, 0) {
  hash_context.buffer = &hash_buffer[0];
  shash::Init(hash_context);
}

FileInfo::FileInfo(const FileInfo &other)
    : handle(other.handle)
    , total_size(other.total_size)
    , current_size(other.current_size)
    , hash_context(other.hash_context)
    , hash_buffer(other.hash_buffer) {
  hash_context.buffer = &hash_buffer[0];
}

FileInfo &FileInfo::operator=(const FileInfo &other) {
  handle = other.handle;
  total_size = other.total_size;
  current_size = other.current_size;
  hash_context = other.hash_context;
  hash_buffer = other.hash_buffer;
  hash_context.buffer = &hash_buffer[0];

  return *this;
}

PayloadProcessor::PayloadProcessor()
    : pending_files_()
    , current_repo_()
    , uploader_()
    , temp_dir_()
    , num_errors_(0)
    , statistics_(NULL) { }

PayloadProcessor::~PayloadProcessor() { }

PayloadProcessor::Result PayloadProcessor::Process(
    int fdin, const std::string &header_digest, const std::string &path,
    uint64_t header_size) {
  LogCvmfs(kLogReceiver, kLogSyslog,
           "PayloadProcessor - lease_path: %s, header digest: %s, header "
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
  std::vector<unsigned char> buffer(kConsumerBuffer, 0);
  do {
    nb = read(fdin, &buffer[0], buffer.size());
    consumer_state = deserializer.ConsumeNext(nb, &buffer[0]);
    if (consumer_state != ObjectPackBuild::kStateContinue
        && consumer_state != ObjectPackBuild::kStateDone) {
      LogCvmfs(kLogReceiver, kLogSyslogErr,
               "PayloadProcessor - error: %d encountered when consuming object "
               "pack.",
               consumer_state);
      break;
    }
  } while (nb > 0 && consumer_state != ObjectPackBuild::kStateDone);

  assert(pending_files_.empty());

  Result res = Finalize();

  deserializer.UnregisterListeners();

  return res;
}

void PayloadProcessor::ConsumerEventCallback(
    const ObjectPackBuild::Event &event) {
  std::string path("");

  if (event.object_type == ObjectPack::kCas) {
    path = event.id.MakePath();
  } else if (event.object_type == ObjectPack::kNamed) {
    path = event.object_name;
  } else {
    // kEmpty - this is an error.
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "PayloadProcessor - error: Event received with unknown object.");
    num_errors_++;
    return;
  }

  FileIterator it = pending_files_.find(event.id);
  if (it == pending_files_.end()) {
    // Schedule file upload if it's not being uploaded yet.
    // Uploaders later check if the file is already present
    // in the upstream storage and will not upload it twice.
    FileInfo info(event);
    // info.handle is later deleted by FinalizeStreamedUpload
    info.handle = uploader_->InitStreamedUpload(NULL);
    pending_files_[event.id] = info;
  }

  FileInfo &info = pending_files_[event.id];

  void *buf_copied = smalloc(event.buf_size);
  memcpy(buf_copied, event.buf, event.buf_size);
  upload::AbstractUploader::UploadBuffer buf(event.buf_size, buf_copied);
  uploader_->ScheduleUpload(
      info.handle, buf,
      upload::AbstractUploader::MakeClosure(
          &PayloadProcessor::OnUploadJobComplete, this, buf_copied));

  shash::Update(static_cast<const unsigned char *>(event.buf),
                event.buf_size,
                info.hash_context);

  info.current_size += event.buf_size;

  if (info.current_size == info.total_size) {
    shash::Any file_hash(event.id.algorithm);
    shash::Final(info.hash_context, &file_hash);

    if (file_hash != event.id) {
      LogCvmfs(
          kLogReceiver, kLogSyslogErr,
          "PayloadProcessor - error: Hash mismatch for unpacked file: event "
          "size: %ld, file size: %ld, event hash: %s, file hash: %s",
          event.size, info.current_size, event.id.ToString(true).c_str(),
          file_hash.ToString(true).c_str());
      num_errors_++;
      return;
    }
    // override final remote path if not CAS object
    if (event.object_type == ObjectPack::kNamed) {
      info.handle->remote_path = path;
    }
    uploader_->ScheduleCommit(info.handle, event.id);

    pending_files_.erase(event.id);
  }
}

void PayloadProcessor::OnUploadJobComplete(
    const upload::UploaderResults &results, void *buffer) {
  free(buffer);
}

void PayloadProcessor::SetStatistics(perf::Statistics *st) {
  statistics_ = new perf::StatisticsTemplate("publish", st);
}

PayloadProcessor::Result PayloadProcessor::Initialize() {
  Params params;
  if (!GetParamsFromFile(current_repo_, &params)) {
    LogCvmfs(
        kLogReceiver, kLogSyslogErr,
        "PayloadProcessor - error: Could not get configuration parameters.");
    return kOtherError;
  }

  const std::string spooler_temp_dir = GetSpoolerTempDir(
      params.spooler_configuration);
  assert(!spooler_temp_dir.empty());
  assert(MkdirDeep(spooler_temp_dir + "/receiver", 0770, true));
  temp_dir_ = RaiiTempDir::Create(spooler_temp_dir
                                  + "/receiver/payload_processor");

  upload::SpoolerDefinition definition(
      params.spooler_configuration, params.hash_alg, params.compression_alg,
      params.generate_legacy_bulk_chunks, params.use_file_chunking,
      params.min_chunk_size, params.avg_chunk_size, params.max_chunk_size,
      "dummy_token", "dummy_key");

  uploader_.Destroy();

  // configure the uploader environment
  uploader_ = upload::AbstractUploader::Construct(definition);
  if (!uploader_.IsValid()) {
    LogCvmfs(kLogSpooler, kLogWarning,
             "Failed to initialize backend upload "
             "facility in PayloadProcessor.");
    return kUploaderError;
  }

  if (statistics_.IsValid()) {
    uploader_->InitCounters(statistics_.weak_ref());
  }

  return kSuccess;
}

PayloadProcessor::Result PayloadProcessor::Finalize() {
  uploader_->WaitForUpload();
  temp_dir_.Destroy();

  const unsigned num_uploader_errors = uploader_->GetNumberOfErrors();
  uploader_->TearDown();
  if (num_uploader_errors > 0) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "PayloadProcessor - error: Uploader - %d upload(s) failed.",
             num_uploader_errors);
    return kUploaderError;
  }

  if (GetNumErrors() > 0) {
    LogCvmfs(kLogReceiver, kLogSyslogErr,
             "PayloadProcessor - error: %d unpacking error(s).",
             GetNumErrors());
    return kOtherError;
  }

  return kSuccess;
}

}  // namespace receiver
