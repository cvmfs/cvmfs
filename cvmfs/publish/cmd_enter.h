/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_CMD_ENTER_H_
#define CVMFS_PUBLISH_CMD_ENTER_H_

#include <string>
#include <vector>

#include "publish/command.h"
#include "publish/settings.h"

namespace publish {

class SettingsPublisher;

class CmdEnter : public Command {
 public:
  CmdEnter()
    : settings_spool_area_("unset")  // will be set by SetSpoolArea in Main()
    , cvmfs2_binary_("/usr/bin/cvmfs2")
    , overlayfs_binary_("/usr/bin/fuse-overlayfs")
  { }
  virtual std::string GetName() const { return "enter"; }
  virtual std::string GetBrief() const {
    return "Open an ephemeral namespace to publish content";
  }
  virtual std::string GetUsage() const {
    return "[options] <fully qualified repository name> [-- <command> <parms>]";
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
    p.push_back(Parameter::Switch("keep-session", 'k',
      "Do not remove the session directory when the shell exits"));
    p.push_back(Parameter::Switch("keep-logs", 'l',
      "Clean the session directory on shell exit except for the logs"));
    return p;
  }
  virtual unsigned GetMinPlainArgs() const { return 1; }

  virtual int Main(const Options &options);

 private:
  void MountOverlayfs();
  void CreateUnderlay(const std::string &source_dir,
                      const std::string &dest_dir,
                      const std::vector<std::string> &empty_dirs,
                      std::vector<std::string> *new_paths);
  void WriteCvmfsConfig(const std::string &extra_config);
  void MountCvmfs();
  pid_t RunInteractiveShell();

  std::string GetCvmfsXattr(const std::string &name);
  void CleanupSession(bool keep_logs,
                      const std::vector<std::string> &new_paths);

  SettingsSpoolArea settings_spool_area_;
  std::string fqrn_;
  std::string cvmfs2_binary_;
  std::string overlayfs_binary_;
  std::string session_dir_;  ///< In $HOME/.cvmfs/fqrn, container spool area
  std::string env_conf_;  ///< Stores the session directory environment
  std::string rootfs_dir_;  ///< Destination to chroot() to in the namespace
  std::string stdout_path_;  ///< Logs stdout of background commands
  std::string stderr_path_;  ///< Logs stdout of background commands
};

}  // namespace publish

#endif  // CVMFS_PUBLISH_CMD_ENTER_H_
