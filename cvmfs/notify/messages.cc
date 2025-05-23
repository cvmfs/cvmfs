/**
 * This file is part of the CernVM File System.
 */

#include "messages.h"

#include <cassert>

#include "json.h"
#include "json_document.h"
#include "util/logging.h"
#include "util/pointer.h"
#include "util/string.h"

namespace {

const LogFacilities &kLogError = DefaultLogging::error;

}  // namespace

namespace notify {

namespace msg {

Activity::Activity() : version_(0), timestamp_(), repository_(), manifest_() { }

Activity::~Activity() { }

bool Activity::operator==(const Activity &other) const {
  return (this->version_ == other.version_)
         && (this->timestamp_ == other.timestamp_)
         && (this->repository_ == other.repository_)
         && (this->manifest_ == other.manifest_);
}

void Activity::ToJSONString(std::string *s) {
  assert(s);

  *s = "{ \"version\" : " + StringifyInt(version_) + ", \"timestamp\" : \""
       + timestamp_ + "\", \"type\" : \"activity\", \"repository\" : \""
       + repository_ + "\", \"manifest\" : \"" + Base64(manifest_) + "\"}";
}

bool Activity::FromJSONString(const std::string &s) {
  const UniquePtr<JsonDocument> m(JsonDocument::Create(s));
  if (!m.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogError, "Could not create JSON document.");
    return false;
  }

  std::string message_type;
  if (!GetFromJSON(m->root(), "type", &message_type)) {
    LogCvmfs(kLogCvmfs, kLogError, "Could not read message type.");
    return false;
  }
  if (message_type != "activity") {
    LogCvmfs(kLogCvmfs, kLogError, "Invalid message type: %s.",
             message_type.c_str());
    return false;
  }

  if (!GetFromJSON(m->root(), "version", &version_)) {
    LogCvmfs(kLogCvmfs, kLogError, "Could not read version.");
    return false;
  }

  if (!GetFromJSON(m->root(), "timestamp", &timestamp_)) {
    LogCvmfs(kLogCvmfs, kLogError, "Could not read timestamp.");
    return false;
  }
  if (!GetFromJSON(m->root(), "repository", &repository_)) {
    LogCvmfs(kLogCvmfs, kLogError, "Could not read repository.");
    return false;
  }
  std::string manifest_b64;
  if (!GetFromJSON(m->root(), "manifest", &manifest_b64)) {
    LogCvmfs(kLogCvmfs, kLogError, "Could not read manifest.");
    return false;
  }
  if (!Debase64(manifest_b64, &manifest_)) {
    LogCvmfs(kLogCvmfs, kLogError, "Could not debase64 manifest.");
    return false;
  }

  return true;
}

}  // namespace msg

}  // namespace notify
