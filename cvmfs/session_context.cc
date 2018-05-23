/**
 * This file is part of the CernVM File System.
 */

#include "session_context.h"

#include <algorithm>

#include "curl/curl.h"
#include "cvmfs_config.h"
#include "gateway_util.h"
#include "json_document.h"
#include "swissknife_lease_curl.h"
#include "util/string.h"

namespace {
const size_t kMaxResultQueueSize = 1000000;
}

namespace upload {

size_t SendCB(void* ptr, size_t size, size_t nmemb, void* userp) {
  CurlSendPayload* payload = static_cast<CurlSendPayload*>(userp);

  size_t max_chunk_size = size * nmemb;
  if (max_chunk_size < 1) {
    return 0;
  }

  size_t current_chunk_size = 0;
  while (current_chunk_size < max_chunk_size) {
    if (payload->index < payload->json_message->size()) {
      // Can add a chunk from the JSON message
      const size_t read_size =
          std::min(max_chunk_size - current_chunk_size,
                   payload->json_message->size() - payload->index);
      current_chunk_size += read_size;
      std::memcpy(ptr, payload->json_message->data() + payload->index,
                  read_size);
      payload->index += read_size;
    } else {
      // Can add a chunk from the payload
      const size_t max_read_size = max_chunk_size - current_chunk_size;
      const unsigned nbytes = payload->pack_serializer->ProduceNext(
          max_read_size, static_cast<unsigned char*>(ptr) + current_chunk_size);
      current_chunk_size += nbytes;

      if (!nbytes) {
        break;
      }
    }
  }

  return current_chunk_size;
}

size_t RecvCB(void* buffer, size_t size, size_t nmemb, void* userp) {
  std::string* my_buffer = static_cast<std::string*>(userp);

  if (size * nmemb < 1) {
    return 0;
  }

  *my_buffer = static_cast<char*>(buffer);

  return my_buffer->size();
}

SessionContextBase::SessionContextBase()
    : upload_results_(kMaxResultQueueSize, kMaxResultQueueSize),
      api_url_(),
      session_token_(),
      key_id_(),
      secret_(),
      queue_was_flushed_(1, 1),
      max_pack_size_(ObjectPack::kDefaultLimit),
      active_handles_(),
      current_pack_(NULL),
      current_pack_mtx_(),
      objects_dispatched_(0),
      bytes_committed_(0),
      bytes_dispatched_(0) {}

SessionContextBase::~SessionContextBase() {}

bool SessionContextBase::Initialize(const std::string& api_url,
                                    const std::string& session_token,
                                    const std::string& key_id,
                                    const std::string& secret,
                                    uint64_t max_pack_size) {
  bool ret = true;

  // Initialize session context lock
  pthread_mutexattr_t attr;
  if (pthread_mutexattr_init(&attr) ||
      pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE) ||
      pthread_mutex_init(&current_pack_mtx_, &attr) ||
      pthread_mutexattr_destroy(&attr)) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "Could not initialize SessionContext lock.");
    return false;
  }

  // Set upstream URL and session token
  api_url_ = api_url;
  session_token_ = session_token;
  key_id_ = key_id;
  secret_ = secret;
  max_pack_size_ = max_pack_size;

  atomic_init64(&objects_dispatched_);
  bytes_committed_ = 0u;
  bytes_dispatched_ = 0u;

  // Ensure that the upload job and result queues are empty
  upload_results_.Drop();

  queue_was_flushed_.Drop();
  queue_was_flushed_.Enqueue(true);

  // Ensure that there are not open object packs
  if (current_pack_) {
    LogCvmfs(
        kLogUploadGateway, kLogStderr,
        "Could not initialize SessionContext - Existing open object packs.");
    ret = false;
  }

  ret = InitializeDerived() && ret;

  return ret;
}

bool SessionContextBase::Finalize(bool commit, const std::string& old_root_hash,
                                  const std::string& new_root_hash,
                                  const RepositoryTag& tag) {
  assert(active_handles_.empty());
  {
    MutexLockGuard lock(current_pack_mtx_);

    if (current_pack_ && current_pack_->GetNoObjects() > 0) {
      Dispatch();
      current_pack_ = NULL;
    }
  }

  bool results = true;
  int64_t jobs_finished = 0;
  while (!upload_results_.IsEmpty() || (jobs_finished < NumJobsSubmitted())) {
    Future<bool>* future = upload_results_.Dequeue();
    results = future->Get() && results;
    delete future;
    jobs_finished++;
  }

  if (commit) {
    if (old_root_hash.empty() || new_root_hash.empty()) {
      return false;
    }
    bool commit_result = Commit(old_root_hash, new_root_hash, tag);
    if (!commit_result) {
      LogCvmfs(kLogUploadGateway, kLogStderr,
               "SessionContext: could not commit session. Aborting.");
      return false;
    }
  }

  results &= FinalizeDerived() && (bytes_committed_ == bytes_dispatched_);

  pthread_mutex_destroy(&current_pack_mtx_);
  return results;
}

void SessionContextBase::WaitForUpload() {
  if (!upload_results_.IsEmpty()) {
    queue_was_flushed_.Dequeue();
  }
}

ObjectPack::BucketHandle SessionContextBase::NewBucket() {
  MutexLockGuard lock(current_pack_mtx_);
  if (!current_pack_) {
    current_pack_ = new ObjectPack(max_pack_size_);
  }
  ObjectPack::BucketHandle hd = current_pack_->NewBucket();
  active_handles_.push_back(hd);
  return hd;
}

bool SessionContextBase::CommitBucket(const ObjectPack::BucketContentType type,
                                      const shash::Any& id,
                                      const ObjectPack::BucketHandle handle,
                                      const std::string& name,
                                      const bool force_dispatch) {
  MutexLockGuard lock(current_pack_mtx_);

  if (!current_pack_) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "Error: Called SessionBaseContext::CommitBucket without an open "
             "ObjectPack.");
    return false;
  }

  uint64_t size0 = current_pack_->size();
  bool committed = current_pack_->CommitBucket(type, id, handle, name);

  if (committed) {  // Current pack is still not full
    active_handles_.erase(
        std::remove(active_handles_.begin(), active_handles_.end(), handle),
        active_handles_.end());
    uint64_t size1 = current_pack_->size();
    bytes_committed_ += size1 - size0;
    if (force_dispatch) {
      Dispatch();
      current_pack_ = NULL;
    }
  } else {  // Current pack is full and can be dispatched
    uint64_t new_size = 0;
    if (handle->capacity > max_pack_size_) {
      new_size = handle->capacity + 1;
    } else {
      new_size = max_pack_size_;
    }
    ObjectPack* new_pack = new ObjectPack(new_size);
    for (size_t i = 0u; i < active_handles_.size(); ++i) {
      current_pack_->TransferBucket(active_handles_[i], new_pack);
    }

    if (current_pack_->GetNoObjects() > 0) {
      Dispatch();
    }
    current_pack_ = new_pack;

    CommitBucket(type, id, handle, name, false);
  }

  return true;
}

int64_t SessionContextBase::NumJobsSubmitted() const {
  return atomic_read64(&objects_dispatched_);
}

void SessionContextBase::Dispatch() {
  MutexLockGuard lock(current_pack_mtx_);

  if (!current_pack_) {
    return;
  }

  atomic_inc64(&objects_dispatched_);
  bytes_dispatched_ += current_pack_->size();
  upload_results_.Enqueue(DispatchObjectPack(current_pack_));
}

SessionContext::SessionContext()
    : SessionContextBase(),
      upload_jobs_(1000000, 1000000),
      worker_terminate_(),
      worker_() {}

bool SessionContext::InitializeDerived() {
  // Start worker thread
  atomic_init32(&worker_terminate_);
  atomic_write32(&worker_terminate_, 0);

  upload_jobs_.Drop();

  int retval =
      pthread_create(&worker_, NULL, UploadLoop, reinterpret_cast<void*>(this));

  return !retval;
}

bool SessionContext::FinalizeDerived() {
  atomic_write32(&worker_terminate_, 1);

  pthread_join(worker_, NULL);

  return true;
}

bool SessionContext::Commit(const std::string& old_root_hash,
                            const std::string& new_root_hash,
                            const RepositoryTag& tag) {
  std::string request;
  JsonStringInput request_input;
  request_input.push_back(
      std::make_pair("old_root_hash", old_root_hash.c_str()));
  request_input.push_back(
      std::make_pair("new_root_hash", new_root_hash.c_str()));
  request_input.push_back(std::make_pair("tag_name",
                                         tag.name_.c_str()));
  request_input.push_back(std::make_pair("tag_channel",
                                         tag.channel_.c_str()));
  request_input.push_back(std::make_pair("tag_description",
                                         tag.description_.c_str()));
  ToJsonString(request_input, &request);
  CurlBuffer buffer;
  return MakeEndRequest("POST", key_id_, secret_, session_token_, api_url_,
                        request, &buffer);
}

Future<bool>* SessionContext::DispatchObjectPack(ObjectPack* pack) {
  UploadJob* job = new UploadJob;
  job->pack = pack;
  job->result = new Future<bool>();
  upload_jobs_.Enqueue(job);
  return job->result;
}

bool SessionContext::DoUpload(const SessionContext::UploadJob* job) {
  // Set up the object pack serializer
  ObjectPackProducer serializer(job->pack);

  shash::Any payload_digest(shash::kSha1);
  serializer.GetDigest(&payload_digest);
  const std::string json_msg =
      "{\"session_token\" : \"" + session_token_ +
      "\", \"payload_digest\" : \"" + payload_digest.ToString(false) +
      "\", \"header_size\" : \"" + StringifyInt(serializer.GetHeaderSize()) +
      "\", \"api_version\" : \"" + StringifyInt(gateway::APIVersion()) + "\"}";

  // Compute HMAC
  shash::Any hmac(shash::kSha1);
  shash::HmacString(secret_, json_msg, &hmac);

  CurlSendPayload payload;
  payload.json_message = &json_msg;
  payload.pack_serializer = &serializer;
  payload.index = 0;

  const size_t payload_size =
      json_msg.size() + serializer.GetHeaderSize() + job->pack->size();

  // Prepare the Curl POST request
  CURL* h_curl = curl_easy_init();

  if (!h_curl) {
    return false;
  }

  // Set HTTP headers (Authorization and Message-Size)
  std::string header_str = std::string("Authorization: ") + key_id_ + " " +
                           Base64(hmac.ToString(false));
  struct curl_slist* auth_header = NULL;
  auth_header = curl_slist_append(auth_header, header_str.c_str());
  header_str = std::string("Message-Size: ") + StringifyInt(json_msg.size());
  auth_header = curl_slist_append(auth_header, header_str.c_str());
  curl_easy_setopt(h_curl, CURLOPT_HTTPHEADER, auth_header);

  std::string reply;
  curl_easy_setopt(h_curl, CURLOPT_NOPROGRESS, 1L);
  curl_easy_setopt(h_curl, CURLOPT_USERAGENT, "cvmfs/" VERSION);
  curl_easy_setopt(h_curl, CURLOPT_MAXREDIRS, 50L);
  curl_easy_setopt(h_curl, CURLOPT_CUSTOMREQUEST, "POST");
  curl_easy_setopt(h_curl, CURLOPT_URL, (api_url_ + "/payloads").c_str());
  curl_easy_setopt(h_curl, CURLOPT_POSTFIELDS, NULL);
  curl_easy_setopt(h_curl, CURLOPT_POSTFIELDSIZE_LARGE,
                   static_cast<curl_off_t>(payload_size));
  curl_easy_setopt(h_curl, CURLOPT_READDATA, &payload);
  curl_easy_setopt(h_curl, CURLOPT_READFUNCTION, SendCB);
  curl_easy_setopt(h_curl, CURLOPT_WRITEFUNCTION, RecvCB);
  curl_easy_setopt(h_curl, CURLOPT_WRITEDATA, &reply);

  // Perform the Curl POST request
  CURLcode ret = curl_easy_perform(h_curl);
  if (ret) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "SessionContext::DoUpload - curl_easy_perform failed: %d",
             ret);
  }

  const bool ok = (reply == "{\"status\":\"ok\"}");
  if (!ok) {
    LogCvmfs(kLogUploadGateway, kLogStderr,
             "SessionContext::DoUpload - error reply: %s",
             reply.c_str());
  }

  curl_easy_cleanup(h_curl);
  h_curl = NULL;

  return ok && !ret;
}

void* SessionContext::UploadLoop(void* data) {
  SessionContext* ctx = reinterpret_cast<SessionContext*>(data);

  int64_t jobs_processed = 0;
  while (!ctx->ShouldTerminate()) {
    while (jobs_processed < ctx->NumJobsSubmitted()) {
      UploadJob* job = ctx->upload_jobs_.Dequeue();
      if (!ctx->DoUpload(job)) {
        LogCvmfs(kLogUploadGateway, kLogStderr,
                 "SessionContext: could not submit payload. Aborting.");
        abort();
      }
      job->result->Set(true);
      delete job->pack;
      delete job;
      jobs_processed++;
    }
    if (ctx->queue_was_flushed_.IsEmpty()) {
      ctx->queue_was_flushed_.Enqueue(true);
    }
  }

  return NULL;
}

bool SessionContext::ShouldTerminate() {
  return atomic_read32(&worker_terminate_);
}

}  // namespace upload
