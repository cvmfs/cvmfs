/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_SCRUB_H_
#define CVMFS_SWISSKNIFE_SCRUB_H_

#include <cassert>
#include <string>

#include "crypto/hash.h"
#include "ingestion/pipeline.h"
#include "swissknife.h"

namespace swissknife {

class CommandScrub : public Command {
 public:
  CommandScrub();
  ~CommandScrub();
  virtual std::string GetName() const { return "scrub"; }
  virtual std::string GetDescription() const {
    return "CernVM File System repository file storage checker. Finds silent "
           "disk corruptions by recomputing all file content checksums in the "
           "backend file storage.";
  }
  virtual ParameterList GetParams() const;
  int Main(const ArgumentList &args);


 protected:
  struct Alerts {
    enum Type {
      kUnexpectedFile = 1,
      kUnexpectedSymlink,
      kUnexpectedSubdir,
      kUnexpectedModifier,
      kMalformedHash,
      kMalformedCasSubdir,
      kContentHashMismatch,
      kNumberOfErrorTypes  // This should _always_ stay the last entry!
    };

    static const char *ToString(const Type t);
  };

  void FileCallback(const std::string &relative_path,
                    const std::string &file_name);
  void DirCallback(const std::string &relative_path,
                   const std::string &dir_name);
  void SymlinkCallback(const std::string &relative_path,
                       const std::string &symlink_name);

  void OnFileHashed(const ScrubbingResult &scrubbing_result);

  void PrintAlert(const Alerts::Type type,
                  const std::string &path,
                  const std::string &affected_hash = "") const;
  void ShowAlertsHelpMessage() const;

 private:
  std::string CheckPathAndExtractHash(const std::string &relative_path,
                                      const std::string &file_name,
                                      const std::string &full_path) const;
  bool CheckHashString(const std::string &hash_string,
                       const std::string &full_path) const;

  std::string MakeFullPath(const std::string &relative_path,
                           const std::string &file_name) const;
  std::string MakeRelativePath(const std::string &full_path);

  ScrubbingPipeline pipeline_scrubbing_;
  std::string repo_path_;
  bool machine_readable_output_;

  mutable unsigned int alerts_;
  mutable pthread_mutex_t alerts_mutex_;
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_SCRUB_H_
