/**
 * This file is part of the CernVM File System.
 */

#include "reactor.h"

#include <stdint.h>
#include <unistd.h>
#include <vector>

#include "json_document.h"

namespace {

enum Command { kGenerateToken, kGetTokenId, kSubmitPayload, kQuit, kError };

Command ReadCmd(int fd, JsonDocument** cmd) {
  // First, read message size
  uint64_t msg_size = 0;
  int nb = read(fd, &msg_size, 8);

  if (nb == 8 && msg_size > 0) {
    std::vector<char> buffer(msg_size);
    int nb = read(fd, &buffer[0], msg_size);
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
  while (ReadCmd(fdin_, &msg_body) != kQuit) {
  }

  return true;
}

}  // namespace receiver
