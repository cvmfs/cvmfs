/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "repository_util.h"

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#include <cstdio>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "hash.h"
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

  CheckoutMarker *marker = new CheckoutMarker(tokens[0], tokens[2],
    shash::MkFromHexPtr(shash::HexPtr(tokens[1]), shash::kSuffixCatalog),
    previous_branch);
  return marker;
}

void CheckoutMarker::SaveAs(const std::string &path) const {
  std::string marker =
    tag_ + " " + hash_.ToString(false /* with_suffix */) + " " + branch_;
  if (!previous_branch_.empty())
    marker += " " + previous_branch_;
  marker += "\n";
  SafeWriteToFile(marker, path, kDefaultFileMode);
}


//------------------------------------------------------------------------------


bool ServerLockFile::Acquire(const std::string &path, bool ignore_stale) {
  std::string tmp_path;
  FILE *ftmp = CreateTempFile(path + ".tmp", kDefaultFileMode, "w", &tmp_path);
  if (ftmp == NULL)
    throw EPublish("cannot create lock temp file " + path);
  std::string pid = StringifyInt(getpid());
  bool retval = SafeWrite(fileno(ftmp), pid.data(), pid.length());
  fclose(ftmp);
  if (!retval)
    throw EPublish("cannot create transaction marker " + path);

  if (IsLocked(path, ignore_stale)) {
    unlink(tmp_path.c_str());
    return false;
  }

  Release(path);
  if (link(tmp_path.c_str(), path.c_str()) == 0) {
    unlink(tmp_path.c_str());
    return true;
  }
  unlink(tmp_path.c_str());
  if (errno == EEXIST)
    return false;
  throw EPublish("cannot commit lock file " + path);
}


bool ServerLockFile::IsLocked(const std::string &path, bool ignore_stale) {
  int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    if (errno == ENOENT)
      return false;
    throw EPublish("cannot open transaction marker " + path);
  }

  if (ignore_stale) {
    close(fd);
    return true;
  }

  std::string line;
  bool retval = GetLineFd(fd, &line);
  close(fd);
  if (!retval || line.empty()) {
    // invalid marker, seen as stale
    return false;
  }
  line = Trim(line, true /* trim_newline */);
  pid_t pid = String2Int64(line);
  if (pid <= 0) {
    // invalid marker, seen as stale
    return false;
  }

  return ProcessExists(pid);
}


void ServerLockFile::Release(const std::string &path) {
  unlink(path.c_str());
}


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
                            false /* drop_credentials */,
                            true /* clear_env */,
                            false /* double_fork */,
                            &child_pid);
  if (!retval)
    throw EPublish("cannot spawn suid helper");
  int exit_code = WaitForChild(child_pid);
  if (exit_code != 0)
    throw EPublish("error calling suid helper: " + StringifyInt(exit_code));
}


void SetInConfig(const std::string &path,
                 const std::string &key, const std::string &value)
{
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

}  // namespace publish
