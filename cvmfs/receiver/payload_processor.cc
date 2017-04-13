/**
 * This file is part of the CernVM File System.
 */

#include "payload_processor.h"

#include <fcntl.h>
#include <unistd.h>
#include <vector>

#include "../logging.h"
#include "util/posix.h"
#include "util/string.h"

namespace receiver {

PayloadProcessor::PayloadProcessor() : current_repo_(), num_errors_(0) {}

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
      LogCvmfs(kLogCvmfs, kLogStderr,
               "Error %d encountered when consuming object pack.",
               consumer_state);
      break;
    }
  } while (nb > 0 && consumer_state != ObjectPackBuild::kStateDone);

  if (GetNumErrors() > 0) {
    return kOtherError;
  }

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
    LogCvmfs(kLogCvmfs, kLogStderr, "Event received with unknown object.");
    num_errors_++;
  }

  const std::string hash_string = event.id.ToString(true);
  const char suffix = hash_string[hash_string.size() - 1];

  if (suffix == 'C') {
    // Catalog
    // Extract path names which are updated in this change set and return the
    // list to the caller, to be used in a later call to rebuild catalogs
  } else {
    // Normal file
    const std::string dest = "/srv/cvmfs/" + current_repo_ + "/data/" + path;
    const std::string dest_dir = GetParentPath(dest);

    int fdout = open(dest.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0600);
    int nb = WriteFile(fdout, event.buf, event.buf_size);
    if (static_cast<unsigned int>(nb) != event.buf_size) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Unable to write %s", dest.c_str());
      num_errors_++;
    }
  }
}

int PayloadProcessor::WriteFile(int fd, const void* const buf,
                                size_t buf_size) {
  return write(fd, buf, buf_size);
}

}  // namespace receiver
