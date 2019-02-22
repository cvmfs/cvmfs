/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "publish/settings.h"

#include <string>
#include <vector>

#include "publish/except.h"
#include "util/posix.h"
#include "util/string.h"

namespace publish {


void SettingsTransaction::SetUnionFsType(const std::string &union_fs) {
  if (union_fs == "aufs") {
    union_fs_ = kUnionFsAufs;
  } else if ((union_fs == "overlay") || (union_fs == "overlayfs")) {
    union_fs_ = kUnionFsOverlay;
  } else if (union_fs == "tarball") {
    union_fs_ = kUnionFsTarball;
  } else {
    throw EPublish("unsupported union file system: " + union_fs);
  }
}

void SettingsTransaction::DetectUnionFsType() {
  // TODO(jblomer): shall we switch the order?
  if (DirectoryExists("/sys/fs/aufs")) {
    union_fs_ = kUnionFsAufs;
    return;
  }
  // TODO(jblomer): modprobe aufs, try again
  if (DirectoryExists("/sys/module/overlay")) {
    union_fs_ = kUnionFsOverlay;
    return;
  }
  // TODO(jblomer): modprobe overlay, try again
  throw EPublish("neither AUFS nor OverlayFS detected on the system");
}

bool SettingsTransaction::ValidateUnionFs() {
  // TODO(jblomer)
  return true;
}

//------------------------------------------------------------------------------


std::string SettingsStorage::GetLocator() const {
  return std::string(upload::SpoolerDefinition::kDriverNames[type_]) +
    "," + tmp_dir_ +
    "," + endpoint_;
}

void SettingsStorage::MakeS3(
  const std::string &s3_config,
  const SettingsSpoolArea &spool_area)
{
  type_ = upload::SpoolerDefinition::S3;
  tmp_dir_ = spool_area.tmp_dir();
  endpoint_ = fqrn_ + "@" + s3_config;
}

void SettingsStorage::SetLocator(const std::string &locator) {
  std::vector<std::string> tokens = SplitString(locator, ',');
  if (tokens.size() != 3) {
    throw EPublish("malformed storage locator, expected format is "
                   "<type>,<temporary directory>,<endpoint>");
  }
  if (tokens[0] == "local") {
    type_ = upload::SpoolerDefinition::Local;
  } else if (tokens[0] == "S3") {
    type_ = upload::SpoolerDefinition::S3;
  } else if (tokens[0] == "gw") {
    type_ = upload::SpoolerDefinition::Gateway;
  } else {
    throw EPublish("unsupported storage type: " + tokens[0]);
  }
  tmp_dir_ = tokens[1];
  endpoint_ = tokens[2];
}


//------------------------------------------------------------------------------


void SettingsPublisher::SetUrl(const std::string &url) {
  // TODO(jblomer): sanitiation, check availability
  url_ = url;
}

}  // namespace publish
