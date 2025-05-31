/**
 * This file is part of the CernVM File System.
 */

#define __STDC_FORMAT_MACROS

#include "swissknife_scrub.h"

#include "util/fs_traversal.h"
#include "util/logging.h"
#include "util/posix.h"
#include "util/smalloc.h"
#include "util/string.h"

using namespace std;  // NOLINT

namespace swissknife {

const size_t kHashSubtreeLength = 2;
const std::string kTxnDirectoryName = "txn";

CommandScrub::CommandScrub() : machine_readable_output_(false), alerts_(0) {
  const int retval = pthread_mutex_init(&alerts_mutex_, NULL);
  assert(retval == 0);
}


CommandScrub::~CommandScrub() { pthread_mutex_destroy(&alerts_mutex_); }


swissknife::ParameterList CommandScrub::GetParams() const {
  swissknife::ParameterList r;
  r.push_back(Parameter::Mandatory('r', "repository directory"));
  r.push_back(Parameter::Switch('m', "machine readable output"));
  return r;
}

const char *CommandScrub::Alerts::ToString(const CommandScrub::Alerts::Type t) {
  switch (t) {
    case Alerts::kUnexpectedFile:
      return "unexpected regular file";
    case Alerts::kUnexpectedSymlink:
      return "unexpected symlink";
    case Alerts::kUnexpectedSubdir:
      return "unexpected subdir in CAS subdir";
    case Alerts::kUnexpectedModifier:
      return "unknown object modifier";
    case Alerts::kMalformedHash:
      return "malformed content hash";
    case Alerts::kMalformedCasSubdir:
      return "malformed CAS subdir length";
    case Alerts::kContentHashMismatch:
      return "mismatch of file name and content hash";
    default:
      return "unknown alert";
  }
}

void CommandScrub::FileCallback(const std::string &relative_path,
                                const std::string &file_name) {
  assert(!file_name.empty());

  if (relative_path.empty()) {
    PrintAlert(Alerts::kUnexpectedFile, repo_path_ + "/" + file_name);
    return;
  }
  if (relative_path == kTxnDirectoryName) {
    // transaction directory should be ignored
    return;
  }

  const string full_path = MakeFullPath(relative_path, file_name);
  const std::string hash_string = CheckPathAndExtractHash(relative_path,
                                                          file_name, full_path);
  if (hash_string.empty()) {
    return;
  }

  if (!shash::HexPtr(hash_string).IsValid()) {
    PrintAlert(Alerts::kMalformedHash, full_path, hash_string);
    return;
  }

  const shash::Any hash_from_name =
      shash::MkFromSuffixedHexPtr(shash::HexPtr(hash_string));
  IngestionSource *full_path_source = new FileIngestionSource(full_path);
  pipeline_scrubbing_.Process(
      full_path_source, hash_from_name.algorithm, hash_from_name.suffix);
}


void CommandScrub::DirCallback(const std::string &relative_path,
                               const std::string &dir_name) {
  const string full_path = MakeFullPath(relative_path, dir_name);

  // The directory "/srv/cvmfs/<REPO_NAME>/data/txn/receiver" is whitelisted
  if (HasSuffix(full_path, "data/txn/receiver", false)) {
    return;
  }

  // Check for nested subdirs
  if (relative_path.size() > 0) {
    PrintAlert(Alerts::kUnexpectedSubdir, full_path);
    return;
  }

  // Check CAS hash subdirectory name length
  if (!dir_name.empty() && dir_name.size() != kHashSubtreeLength
      && dir_name != kTxnDirectoryName) {
    PrintAlert(Alerts::kMalformedCasSubdir, full_path);
  }
}

void CommandScrub::SymlinkCallback(const std::string &relative_path,
                                   const std::string &symlink_name) {
  const string full_path = MakeFullPath(relative_path, symlink_name);
  PrintAlert(Alerts::kUnexpectedSymlink, full_path);
}

void CommandScrub::OnFileHashed(const ScrubbingResult &scrubbing_result) {
  const string full_path = scrubbing_result.path;
  const string file_name = GetFileName(full_path);
  const string parent_path = GetParentPath(full_path);
  const string relative_path = MakeRelativePath(parent_path);
  assert(!file_name.empty());

  const std::string hash_string = CheckPathAndExtractHash(relative_path,
                                                          file_name, full_path);
  assert(!hash_string.empty());
  assert(shash::HexPtr(hash_string).IsValid());


  if (scrubbing_result.hash
      != shash::MkFromSuffixedHexPtr(shash::HexPtr(hash_string))) {
    PrintAlert(Alerts::kContentHashMismatch, full_path,
               scrubbing_result.hash.ToString());
  }
}

std::string CommandScrub::CheckPathAndExtractHash(
    const std::string &relative_path,
    const std::string &file_name,
    const std::string &full_path) const {
  // check for a valid object modifier on the end of the file name
  const char last_character = *(file_name.end() - 1);
  bool has_object_modifier = false;
  if (std::isupper(last_character)) {
    has_object_modifier = true;
  }
  if (has_object_modifier && last_character != shash::kSuffixHistory
      && last_character != shash::kSuffixCatalog
      && last_character != shash::kSuffixPartial
      && last_character != shash::kSuffixCertificate
      && last_character != shash::kSuffixMicroCatalog
      && last_character != shash::kSuffixMetainfo) {
    PrintAlert(Alerts::kUnexpectedModifier, full_path);
    return "";
  }

  const string hash_string = GetFileName(GetParentPath(full_path))
                             + (has_object_modifier
                                    ? file_name.substr(0,
                                                       file_name.length() - 1)
                                    : file_name);
  return hash_string;
}


int CommandScrub::Main(const swissknife::ArgumentList &args) {
  repo_path_ = MakeCanonicalPath(*args.find('r')->second);
  machine_readable_output_ = (args.find('m') != args.end());

  pipeline_scrubbing_.RegisterListener(&CommandScrub::OnFileHashed, this);
  pipeline_scrubbing_.Spawn();

  // initialize file system recursion engine
  FileSystemTraversal<CommandScrub> traverser(this, repo_path_, true);
  traverser.fn_new_file = &CommandScrub::FileCallback;
  traverser.fn_enter_dir = &CommandScrub::DirCallback;
  traverser.fn_new_symlink = &CommandScrub::SymlinkCallback;
  traverser.Recurse(repo_path_);

  // wait for reader to finish all jobs
  pipeline_scrubbing_.WaitFor();

  return (alerts_ == 0) ? 0 : 1;
}

void CommandScrub::PrintAlert(const Alerts::Type type,
                              const std::string &path,
                              const std::string &affected_hash) const {
  const MutexLockGuard l(alerts_mutex_);

  const char *msg = Alerts::ToString(type);
  if (machine_readable_output_) {
    LogCvmfs(kLogUtility, kLogStderr, "%d %s %s", type,
             ((affected_hash.empty()) ? "-" : affected_hash.c_str()),
             path.c_str());
  } else {
    LogCvmfs(kLogUtility, kLogStderr, "%s | at: %s", msg, path.c_str());
  }

  ++alerts_;
}

std::string CommandScrub::MakeFullPath(const std::string &relative_path,
                                       const std::string &file_name) const {
  return (relative_path.empty())
             ? repo_path_ + "/" + file_name
             : repo_path_ + "/" + relative_path + "/" + file_name;
}

std::string CommandScrub::MakeRelativePath(const std::string &full_path) {
  assert(HasPrefix(full_path, repo_path_ + "/", false));
  return full_path.substr(repo_path_.length() + 1);
}

void CommandScrub::ShowAlertsHelpMessage() const {
  LogCvmfs(kLogUtility, kLogStdout, "to come...");
}

}  // namespace swissknife
