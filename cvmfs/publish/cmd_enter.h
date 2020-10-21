/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_CMD_ENTER_H_
#define CVMFS_PUBLISH_CMD_ENTER_H_

#include <string>
#include <vector>

#include "publish/command.h"

namespace publish {

class SettingsPublisher;

class CmdEnter : public Command {
 public:
  CmdEnter()
    : cvmfs2_binary_("/usr/bin/cvmfs2")
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
    p.push_back(Parameter::Optional("cvmfs2", 'c', "path",
      "Path to the cvmfs2 binary"));
    p.push_back(Parameter::Optional("cvmfs-config", 'C', "path",
      "Path to extra configuration for the CernVM-FS client"));
    p.push_back(Parameter::Switch("root", 'r', "Run as fake root"));
    return p;
  }
  virtual unsigned GetMinPlainArgs() const { return 1; }

  virtual int Main(const Options &options);

 private:
  void MountOverlayfs();
  void CreateUnderlay(const std::string &source_dir,
                      const std::string &dest_dir,
                      const std::vector<std::string> &empty_dirs);
  void WriteCvmfsConfig();
  void MountCvmfs();
  pid_t RunInteractiveShell();

  std::string fqrn_;
  std::string session_dir_;
  std::string target_dir_;
  std::string lower_layer_;
  std::string cvmfs2_binary_;
  std::string rootfs_dir_;
  std::string config_path_;  ///< CernVM-FS configuration
  std::string usyslog_path_;
  std::string cache_dir_;
  std::string upper_layer_;
  std::string ovl_workdir_;

  std::string overlayfs_binary_;
};

}  // namespace publish

#endif  // CVMFS_PUBLISH_CMD_ENTER_H_
