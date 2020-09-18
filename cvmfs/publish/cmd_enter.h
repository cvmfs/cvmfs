/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_CMD_ENTER_H_
#define CVMFS_PUBLISH_CMD_ENTER_H_

#include <string>

#include "publish/command.h"

namespace publish {

class SettingsPublisher;

class CmdEnter : public Command {
 public:
  CmdEnter()
    : cvmfs_binary_("/usr/bin/cvmfs2")
    , overlayfs_binary_("/usr/bin/fuse-overlayfs")
  { }
  virtual std::string GetName() const { return "enter"; }
  virtual std::string GetBrief() const {
    return "Opens an ephemeral namespace to publish content";
  }
  virtual std::string GetUsage() const {
    return "[options] <fully qualified repository name>";
  }
  virtual ParameterList GetParams() const {
    ParameterList p;
    p.push_back(Parameter::Optional("stratum0", 'w', "stratum0 url",
      "HTTP endpoint of the authoritative storage"));
    return p;
  }
  virtual unsigned GetMinPlainArgs() const { return 1; }

  virtual int Main(const Options &options);

 private:
  void MountCvmfs(const SettingsPublisher &settings);
  void MountOverlayfs(const SettingsPublisher &settings);
  void CreateUnderlay(const std::string &source_dir,
                      const std::string &dest_dir,
                      const std::vector<std::string> &empty_dirs);

  std::string cvmfs_binary_;
  std::string overlayfs_binary_;
};

}  // namespace publish

#endif  // CVMFS_PUBLISH_CMD_ENTER_H_
