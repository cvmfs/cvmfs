/**
 * This file is part of the CernVM File System.
 */

#include "payload_processor.h"

#include "unistd.h"

#include "../logging.h"
#include "util/string.h"

namespace receiver {

PayloadProcessor::PayloadProcessor() {}

PayloadProcessor::~PayloadProcessor() {}

PayloadProcessor::Result PayloadProcessor::Process(
    int fdin, const std::string& digest_base64, const std::string& /*path*/,
    uint64_t header_size) {
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

  return kSuccess;
}

void PayloadProcessor::ConsumerEventCallback(
    const ObjectPackBuild::Event& event) {
  const std::string path = event.id.MakePath();
  if (event.object_type == ObjectPack::kCas) {
    // CAS Blob
    LogCvmfs(
        kLogCvmfs, kLogStderr,
        "Event received: kCas, path: %s, size: %ld, buf_size: %d, buf: %lu\n",
        path.c_str(), event.size, event.buf_size, event.buf);
  } else if (event.object_type == ObjectPack::kNamed) {
    // Named file
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Event received: kNamed, path: %s, size: %ld, buf_size: %d, buf: "
             "%lu, object_name: %s\n",
             path.c_str(), event.size, event.buf_size, event.buf,
             event.object_name.c_str());
  } else {
    // kEmpty - this is an error.
    LogCvmfs(kLogCvmfs, kLogStderr, "Event received with unknown object.");
  }
}

}  // namespace receiver
