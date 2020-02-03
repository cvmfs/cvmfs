/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "repository_util.h"

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#include <cstdio>
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
  if (tokens.size() != 3)
    throw publish::EPublish("checkout marker not parsable: " + line);

  CheckoutMarker *marker = new CheckoutMarker(tokens[0], tokens[2],
    shash::MkFromHexPtr(shash::HexPtr(tokens[1]), shash::kSuffixCatalog));
  return marker;
}

void CheckoutMarker::SaveAs(const std::string &path) const {
  std::string marker =
    tag_ + " " + hash_.ToString(false /* with_suffix */) + " " + branch_ + "\n";
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
  EPublish("cannot commit lock file " + path);
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

}  // namespace publish
