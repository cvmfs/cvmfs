/**
 * This file is part of the CernVM File System.
 */

#define __STDC_FORMAT_MACROS

#include "cvmfs_config.h"
#include "swissknife_scrub.h"

#include "fs_traversal.h"
#include "logging.h"
#include "smalloc.h"

using namespace std;  // NOLINT

namespace swissknife {

const size_t      kHashSubtreeLength = 2;
const std::string kTxnDirectoryName  = "txn";

CommandScrub::StoredFile::StoredFile(const std::string &path,
                                     const std::string &expected_hash) :
  AbstractFile(path, GetFileSize(path)),
  hash_done_(false)
{
  expected_hash_ = shash::MkFromHexPtr(shash::HexPtr(expected_hash));
  hash_context_.algorithm = expected_hash_.algorithm;
  hash_context_.size = shash::GetContextSize(expected_hash_.algorithm);
  hash_context_.buffer = smalloc(hash_context_.size);
  shash::Init(hash_context_);
}


void CommandScrub::StoredFile::Update(const unsigned char *data,
                                      const size_t nbytes)
{
  assert(!hash_done_);
  shash::Update(data, nbytes, hash_context_);
}


void CommandScrub::StoredFile::Finalize() {
  assert(!hash_done_);
  shash::Final(hash_context_, &content_hash_);
  free(hash_context_.buffer);
  hash_context_.buffer = NULL;
  hash_done_ = true;
}



tbb::task* CommandScrub::StoredFileScrubbingTask::execute() {
  StoredFile          *file   = StoredFileScrubbingTask::file();
  upload::CharBuffer  *buffer = StoredFileScrubbingTask::buffer();

  file->Update(buffer->ptr(), buffer->used_bytes());
  if (IsLast()) {
    file->Finalize();
  }

  return Finalize();
}



swissknife::ParameterList CommandScrub::GetParams() {
  swissknife::ParameterList r;
  r.push_back(Parameter::Mandatory('r', "repository directory"));
  r.push_back(Parameter::Switch('m', "machine readable output"));
  return r;
}


const char* CommandScrub::Alerts::ToString(const CommandScrub::Alerts::Type t) {
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
                                const std::string &file_name)
{
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
                                                          file_name,
                                                          full_path);
  if (hash_string.empty()) {
    return;
  }

  if (!shash::HexPtr(hash_string).IsValid()) {
    PrintAlert(Alerts::kMalformedHash, full_path, hash_string);
    return;
  }

  assert(reader_ != NULL);
  reader_->ScheduleRead(new StoredFile(full_path, hash_string));
}


void CommandScrub::DirCallback(const std::string &relative_path,
                               const std::string &dir_name)
{
  const string full_path = MakeFullPath(relative_path, dir_name);
  // Check for nested subdirs
  if (relative_path.size() > 0) {
    PrintAlert(Alerts::kUnexpectedSubdir, full_path);
    return;
  }

  // Check CAS hash subdirectory name length
  if (!dir_name.empty()                      &&
       dir_name.size() != kHashSubtreeLength &&
       dir_name        != kTxnDirectoryName)
  {
    PrintAlert(Alerts::kMalformedCasSubdir, full_path);
  }
}


void CommandScrub::SymlinkCallback(const std::string &relative_path,
                                   const std::string &symlink_name)
{
  const string full_path = MakeFullPath(relative_path, symlink_name);
  PrintAlert(Alerts::kUnexpectedSymlink, full_path);
}


void CommandScrub::FileProcessedCallback(StoredFile* const& file) {
  if (file->content_hash() != file->expected_hash()) {
    PrintAlert(Alerts::kContentHashMismatch, file->path(),
               file->content_hash().ToString());
  }
}


std::string CommandScrub::CheckPathAndExtractHash(
                                           const std::string &relative_path,
                                           const std::string &file_name,
                                           const std::string &full_path) const
{
  // check for a valid object modifier on the end of the file name
  const char last_character = *(file_name.end() - 1);
  bool has_object_modifier = false;
  if (std::isupper(last_character)) {
    has_object_modifier = true;
  }
  if (has_object_modifier                          &&
      last_character != shash::kSuffixHistory      &&
      last_character != shash::kSuffixCatalog      &&
      last_character != shash::kSuffixPartial      &&
      last_character != shash::kSuffixCertificate  &&
      last_character != shash::kSuffixMicroCatalog &&
      last_character != shash::kSuffixMetainfo)
  {
    PrintAlert(Alerts::kUnexpectedModifier, full_path);
    return "";
  }

  const string hash_string = GetFileName(GetParentPath(full_path)) +
    (has_object_modifier
    ? file_name.substr(0, file_name.length()-1)
    : file_name);
  return hash_string;
}


int CommandScrub::Main(const swissknife::ArgumentList &args) {
  repo_path_               = MakeCanonicalPath(*args.find('r')->second);
  machine_readable_output_ = (args.find('m') != args.end());

  // initialize alert printer mutex
  const bool mutex_init = (pthread_mutex_init(&alerts_mutex_, NULL) == 0);
  assert(mutex_init);

  // initialize asynchronous reader
  const size_t       max_buffer_size     = 512 * 1024;
  const unsigned int max_files_in_flight = 100;
  reader_ = new ScrubbingReader(max_buffer_size, max_files_in_flight);
  reader_->RegisterListener(&CommandScrub::FileProcessedCallback, this);
  reader_->Initialize();

  // initialize file system recursion engine
  FileSystemTraversal<CommandScrub> traverser(this, repo_path_, true);
  traverser.fn_new_file    = &CommandScrub::FileCallback;
  traverser.fn_enter_dir   = &CommandScrub::DirCallback;
  traverser.fn_new_symlink = &CommandScrub::SymlinkCallback;
  traverser.Recurse(repo_path_);

  // wait for reader to finish all jobs
  reader_->Wait();
  reader_->TearDown();

  return (alerts_ == 0) ? 0 : 1;
}


void CommandScrub::PrintAlert(const Alerts::Type   type,
                              const std::string   &path,
                              const std::string   &affected_hash) const {
  MutexLockGuard l(alerts_mutex_);

  const char *msg = Alerts::ToString(type);
  if (machine_readable_output_) {
    LogCvmfs(kLogUtility, kLogStderr, "%d %s %s",
             type,
             ((affected_hash.empty()) ? "-" : affected_hash.c_str()),
             path.c_str());
  } else {
    LogCvmfs(kLogUtility, kLogStderr, "%s | at: %s", msg, path.c_str());
  }

  ++alerts_;
}


std::string CommandScrub::MakeFullPath(const std::string &relative_path,
                                       const std::string &file_name) const
{
  return (relative_path.empty())
              ? repo_path_ + "/" + file_name
              : repo_path_ + "/" + relative_path + "/" + file_name;
}


void CommandScrub::ShowAlertsHelpMessage() const {
  LogCvmfs(kLogUtility, kLogStdout, "to come...");
}


CommandScrub::~CommandScrub() {
  if (reader_ != NULL) {
    delete reader_;
    reader_ = NULL;
  }

  pthread_mutex_destroy(&alerts_mutex_);
}

}  // namespace swissknife
