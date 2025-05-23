/**
 * This file is part of the CernVM File System.
 */


#include "repository_util.h"

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#include <cstdio>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "crypto/hash.h"
#include "publish/except.h"
#include "util/posix.h"
#include "util/string.h"

namespace publish {

CheckoutMarker *CheckoutMarker::CreateFrom(const std::string &path) {
  if (!FileExists(path))
    return NULL;

  FILE *f = fopen(path.c_str(), "r");
  if (f == NULL)
    throw publish::EPublish("cannot open checkout marker");
  std::string line;
  bool retval = GetLineFile(f, &line);
  fclose(f);
  if (!retval)
    throw publish::EPublish("empty checkout marker");
  line = Trim(line, true /* trim_newline */);
  std::vector<std::string> tokens = SplitString(line, ' ');
  std::string previous_branch;
  if (tokens.size() == 4)
    previous_branch = tokens[3];
  if (tokens.size() < 3 || tokens.size() > 4)
    throw publish::EPublish("checkout marker not parsable: " + line);

  CheckoutMarker *marker = new CheckoutMarker(
      tokens[0], tokens[2],
      shash::MkFromHexPtr(shash::HexPtr(tokens[1]), shash::kSuffixCatalog),
      previous_branch);
  return marker;
}

void CheckoutMarker::SaveAs(const std::string &path) const {
  std::string marker = tag_ + " " + hash_.ToString(false /* with_suffix */)
                       + " " + branch_;
  if (!previous_branch_.empty())
    marker += " " + previous_branch_;
  marker += "\n";
  SafeWriteToFile(marker, path, kDefaultFileMode);
}


//------------------------------------------------------------------------------


void ServerLockFile::Lock() {
  if (!TryLock()) {
    throw EPublish("Could not acquire lock " + path_,
                   EPublish::kFailTransactionState);
  }
}


bool ServerLockFile::TryLock() {
  int new_fd = TryLockFile(path_);
  if (new_fd >= 0) {
    assert(fd_ < 0);
    fd_ = new_fd;
    return true;
  } else if (new_fd == -1) {
    throw EPublish("Error while attempting to acquire lock " + path_);
  } else {
    return false;
  }
}


void ServerLockFile::Unlock() {
  int old_fd = fd_;
  assert(old_fd >= 0);
  fd_ = -1;
  unlink(path_.c_str());
  close(old_fd);
}


//------------------------------------------------------------------------------


void ServerFlagFile::Set() {
  int fd = open(path_.c_str(), O_CREAT | O_RDWR, 0600);
  if (fd < 0)
    throw EPublish("cannot create flag file " + path_);
  close(fd);
}


void ServerFlagFile::Clear() { unlink(path_.c_str()); }


bool ServerFlagFile::IsSet() const { return FileExists(path_); }


//------------------------------------------------------------------------------


void RunSuidHelper(const std::string &verb, const std::string &fqrn) {
  std::vector<std::string> cmd_line;
  cmd_line.push_back("/usr/bin/cvmfs_suid_helper");
  cmd_line.push_back(verb);
  cmd_line.push_back(fqrn);
  std::set<int> preserved_fds;
  preserved_fds.insert(1);
  preserved_fds.insert(2);
  pid_t child_pid;
  bool retval = ManagedExec(cmd_line, preserved_fds, std::map<int, int>(),
                            false /* drop_credentials */, true /* clear_env */,
                            false /* double_fork */, &child_pid);
  if (!retval)
    throw EPublish("cannot spawn suid helper");
  int exit_code = WaitForChild(child_pid);
  if (exit_code != 0)
    throw EPublish("error calling suid helper: " + StringifyInt(exit_code));
}


void SetInConfig(const std::string &path, const std::string &key,
                 const std::string &value) {
  int fd = open(path.c_str(), O_RDWR | O_CREAT, kDefaultFileMode);
  if (fd < 0)
    throw EPublish("cannot modify configuration file " + path);

  std::string new_content;
  std::string line;
  bool key_exists = false;
  while (GetLineFd(fd, &line)) {
    std::string trimmed = Trim(line);
    if (HasPrefix(trimmed, key + "=", false /* ignore_case */)) {
      key_exists = true;
      if (!value.empty())
        new_content += key + "=" + value + "\n";
    } else {
      new_content += line + "\n";
    }
  }
  if (!key_exists && !value.empty())
    new_content += key + "=" + value + "\n";

  off_t off_zero = lseek(fd, 0, SEEK_SET);
  if (off_zero != 0) {
    close(fd);
    throw EPublish("cannot rewind configuration file " + path);
  }
  int rvi = ftruncate(fd, 0);
  if (rvi != 0) {
    close(fd);
    throw EPublish("cannot truncate configuration file " + path);
  }
  bool rvb = SafeWrite(fd, new_content.data(), new_content.length());
  close(fd);
  if (!rvb)
    throw EPublish("cannot rewrite configuration file " + path);
}


std::string SendTalkCommand(const std::string &socket, const std::string &cmd) {
  int fd = ConnectSocket(socket);
  if (fd < 0) {
    if (errno == ENOENT)
      throw EPublish("Socket " + socket + " not found");
    throw EPublish("Socket " + socket + " inaccessible");
  }

  WritePipe(fd, cmd.data(), cmd.size());

  std::string result;
  char buf;
  int retval;
  while ((retval = read(fd, &buf, 1)) == 1) {
    result.push_back(buf);
  }
  close(fd);
  if (retval != 0)
    throw EPublish("Broken socket: " + socket);

  return result;
}

}  // namespace publish
