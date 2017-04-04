/**
 * This file is part of the CernVM File System.
 */

#include "reactor.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <vector>

#include "../json_document.h"
#include "../logging.h"

namespace receiver {

receiver::Request Reactor::ReadRequest(int fd, std::string* data) {
  using namespace receiver;  // NOLINT

  // First, read the command identifier
  int32_t req_id = 0;
  int nb = read(fd, &req_id, 4);

  if (nb != 4) {
    return kError;
  }

  // Then, read message size
  int32_t msg_size = 0;
  nb = read(fd, &msg_size, 4);

  if (req_id == kError || nb != 4) {
    return kError;
  }

  // Finally read the message body
  if (msg_size > 0) {
    std::vector<char> buffer(msg_size);
    nb = read(fd, &buffer[0], msg_size);

    if (nb != msg_size) {
      return kError;
    }

    *data = std::string(&buffer[0]);
    return static_cast<Request>(req_id);
  }

  return kQuit;
}

bool Reactor::WriteRequest(int fd, receiver::Request req,
                           const std::string& data) {
  const int32_t msg_size = data.size();
  const int32_t total_size = 8 + data.size();  // req + msg_size + data

  std::vector<char> buffer(total_size);

  memcpy(&buffer[0], &req, 4);
  memcpy(&buffer[4], &msg_size, 4);

  if (!data.empty()) {
    memcpy(&buffer[8], &data[0], data.size());
  }

  int nb = write(fd, &buffer[0], total_size);

  return nb == total_size;
}

bool Reactor::ReadReply(int fd, std::string* data) {
  int32_t msg_size(0);
  int nb = read(fd, &msg_size, 4);

  if (nb != 4) {
    return false;
  }

  std::vector<char> buffer(msg_size);
  nb = read(fd, &buffer[0], msg_size);

  if (nb != msg_size) {
    return false;
  }

  *data = std::string(&buffer[0]);

  return true;
}

bool Reactor::WriteReply(int fd, const std::string& data) {
  const int32_t msg_size = data.size();
  const int32_t total_size = 4 + data.size();

  std::vector<char> buffer(total_size);

  memcpy(&buffer[0], &msg_size, 4);

  if (!data.empty()) {
    memcpy(&buffer[4], &data[0], data.size());
  }

  int nb = write(fd, &buffer[0], total_size);

  return nb == total_size;
}

Reactor::Reactor(int fdin, int fdout) : fdin_(fdin), fdout_(fdout) {}

Reactor::~Reactor() {}

bool Reactor::run() {
  // Testing: Just echo the character.
  std::string msg_body;
  Request req = kQuit;
  do {
    req = ReadRequest(fdin_, &msg_body);
    if (!HandleRequest(fdout_, req, msg_body)) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "Reactor: could not handle request. Exiting");
      abort();
    }
  } while (req != kQuit);

  return true;
}

bool Reactor::HandleRequest(int fdout, Request req, const std::string& data) {
  bool ok = true;
  switch (req) {
    case kQuit:
      ok = WriteReply(fdout, "ok");
      break;
    case kEcho:
      ok = WriteReply(fdout, data);
      break;
    case kGenerateToken:
      break;
    case kGetTokenId:
      break;
    case kSubmitPayload:
      break;
    case kError:
      LogCvmfs(kLogCvmfs, kLogStderr, "Reactor: unknown command received.");
      ok = false;
      break;
    default:
      break;
  }

  return ok;
}

}  // namespace receiver
