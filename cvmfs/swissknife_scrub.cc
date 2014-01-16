/**
 * This file is part of the CernVM File System.
 */

#define __STDC_FORMAT_MACROS

#include "swissknife_scrub.h"
#include "fs_traversal.h"
#include "logging.h"
#include "smalloc.h"

#include <sstream>

using namespace swissknife;

const size_t      kHashSubtreeLength = 2;
const size_t      kHashStringLength  = 40;
const std::string kTxnDirectoryName  = "txn";

CommandScrub::StoredFile::StoredFile(const std::string &path,
                                     const std::string &expected_hash) :
  AbstractFile(path, GetFileSize(path)),
  hash_done_(false),
  hash_context_(shash::kSha1),
  expected_hash_(shash::kSha1, shash::HexPtr(expected_hash))
{
  hash_context_.buffer = smalloc(hash_context_.size);
  shash::Init(hash_context_);
}


void CommandScrub::StoredFile::Update(const unsigned char *data,
                                      const size_t nbytes) {
  assert (! hash_done_);
  shash::Update(data, nbytes, hash_context_);
}


void CommandScrub::StoredFile::Finalize() {
  assert (! hash_done_);
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
  swissknife::ParameterList result;
  result.push_back(Parameter('r', "repository directory", false, false));
  // to be extended...
  return result;
}


void CommandScrub::FileCallback(const std::string &relative_path,
                                const std::string &file_name) {
  assert (file_name.size() > 0);

  if (relative_path.size() == 0) {
    PrintWarning("unexpected regular file", repo_path_ + "/" + file_name);
    return;
  }

  const std::string full_path = repo_path_ + "/" + relative_path + "/" + file_name;

  if (relative_path == kTxnDirectoryName) {
    // transaction directory should be ignored
    return;
  }

  const std::string hash_string = CheckPathAndExtractHash(relative_path,
                                                          file_name,
                                                          full_path);
  if (hash_string.empty()) {
    return;
  }

  assert (reader_ != NULL);
  reader_->ScheduleRead(new StoredFile(full_path, hash_string));
}


void CommandScrub::DirCallback(const std::string &relative_path,
                               const std::string &dir_name) {
  const std::string full_path = repo_path_ + "/" + relative_path + "/" + dir_name;
  // check for nested subdirs
  if (relative_path.size() > 0) {
    PrintWarning("unexpected subdir in CAS subdir", full_path);
    return;
  }

  // check CAS hash subdirectory name length
  if (! dir_name.empty()                      &&
        dir_name.size() != kHashSubtreeLength &&
        dir_name        != kTxnDirectoryName) {
    std::stringstream ss;
    ss << "malformed CAS subdir length: " << dir_name.size();
    PrintWarning(ss.str(), full_path);
  }
}


void CommandScrub::SymlinkCallback(const std::string &relative_path,
                                   const std::string &symlink_name) {
  const std::string full_path = repo_path_ + "/" + relative_path + "/" + symlink_name;
  PrintWarning("unexpected symlink", full_path);
}


void CommandScrub::FileProcessedCallback(StoredFile* const& file) {
  if (file->content_hash() != file->expected_hash()) {
    std::stringstream ss;
    ss << "mismatch of file name and content hash: "
       << file->content_hash().ToString();
    PrintWarning(ss.str(), file->path());
  }
}


std::string CommandScrub::CheckPathAndExtractHash(
                                           const std::string &relative_path,
                                           const std::string &file_name,
                                           const std::string &full_path) const {
  // check for a valid object modifier on the end of the file name
  const char last_character = *(file_name.end() - 1); // TODO: C++11: file_name.back()
  bool has_object_modifier = false;
  if (std::isupper(last_character)) {
    has_object_modifier = true;
  }
  if (has_object_modifier   &&
      last_character != 'H' && // history
      last_character != 'C' && // catalog
      last_character != 'P' && // partial
      last_character != 'X' && // certificate
      last_character != 'L')   // micro catalogs (currently only reserved)
  {
    std::stringstream ss;
    ss << "unknown object modifier: " << last_character;
    PrintWarning(ss.str(), full_path);
    return "";
  }

  // check for a fitting file name length of the object
  if ( (!has_object_modifier &&
        file_name.size() != kHashStringLength - kHashSubtreeLength)
       ||
       (has_object_modifier &&
        file_name.size() != kHashStringLength - kHashSubtreeLength + 1)) {
    std::stringstream ss;
    ss << "malformed file name length: " << file_name.size();
    PrintWarning(ss.str(), full_path);
    return "";
  }

  // reconstruct the hash string
  const std::string hash_string = (!has_object_modifier)
    ? relative_path + file_name
    : relative_path + file_name.substr(0, kHashStringLength - kHashSubtreeLength);

  // check if the resulting hash string is sane
  if (CheckHashString(hash_string, full_path)) {
    return hash_string;
  } else {
    return "";
  }
}



bool CommandScrub::CheckHashString(const std::string &hash_string,
                                   const std::string &full_path) const {
  if (hash_string.size() != kHashStringLength) {
    PrintWarning("malformed hash string length", full_path);
    return false;
  }

  for (unsigned int i = 0; i < hash_string.size(); ++i) {
    const char c = hash_string[i];
    if ( ! ( (c >= 48 /* '0' */ && c <=  57 /* '9' */) ||
             (c >= 97 /* 'a' */ && c <= 102 /* 'f' */) ) ) {
      std::stringstream ss;
      ss << "illegal hash character (" << c << ") found";
      PrintWarning(ss.str(), full_path);
      return false;
    }
  }

  return true;
}


int CommandScrub::Main(const swissknife::ArgumentList &args) {
  repo_path_ = *args.find('r')->second;

  // initialize warning printer mutex
  const bool mutex_init = (pthread_mutex_init(&warning_mutex_, NULL) == 0);
  assert (mutex_init);

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

  return (warnings_ == 0) ? 0 : 1;
}


void CommandScrub::PrintWarning(const std::string &msg,
                                const std::string &path) const {
  MutexLockGuard l(warning_mutex_);
  LogCvmfs(kLogUtility, kLogStderr, "%s | at: %s", msg.c_str(), path.c_str());
  ++warnings_;
}


CommandScrub::~CommandScrub() {
  if (reader_ != NULL) {
    delete reader_;
    reader_ = NULL;
  }

  pthread_mutex_destroy(&warning_mutex_);
}
