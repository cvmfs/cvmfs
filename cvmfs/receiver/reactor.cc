/**
 * This file is part of the CernVM File System.
 */

#include "reactor.h"

#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <vector>

#include "../json_document.h"
#include "../logging.h"

namespace {

enum Command {
  kError = 0,
  kEcho = 1,
  kGenerateToken = 2,
  kGetTokenId = 3,
  kSubmitPayload = 4,
  kQuit = 5
};

Command ReadCmd(int fd, JsonDocument** /*cmd*/) {
  // First, read the command identifier
  int32_t cmd_id = 0;
  int nb = read(fd, &cmd_id, 4);

  if (cmd_id == kError) {
    return kError;
  }

  // Then, read message size
  int32_t msg_size = 0;
  nb = read(fd, &msg_size, 4);

  // Finally read the message body
  if (nb == 4 && msg_size > 0) {
    std::string buffer(msg_size, ' ');
    nb = read(fd, &buffer[0], msg_size);
  }

  return kQuit;
}

}  // namespace

namespace receiver {

Reactor::Reactor(int fdin, int fdout) : fdin_(fdin), fdout_(fdout) {}

Reactor::~Reactor() {}

bool Reactor::run() {
  // Testing: Just echo the character.
  char buffer;
  int nb = read(fdin_, &buffer, 1);
  nb = write(fdout_, &buffer, 1);

  JsonDocument* msg_body = NULL;
  Command cmd = kQuit;
  do {
    cmd = ReadCmd(fdin_, &msg_body);
    if (cmd == kError) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "Reactor: unknown command received. Exiting");
      abort();
    }
  } while (cmd != kQuit);

  return true;
}  // namespace receiver

}  // namespace receiver
