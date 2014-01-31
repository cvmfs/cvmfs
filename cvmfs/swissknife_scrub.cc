/**
 * This file is part of the CernVM File System.
 */

#define __STDC_FORMAT_MACROS

#include "swissknife_scrub.h"
#include "fs_traversal.h"
#include "logging.h"
#include "smalloc.h"

using namespace swissknife;
using namespace std;  // NOLINT

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
                                const std::string &file_name)
{
  assert (file_name.size() > 0);

  if (relative_path.size() == 0) {
    PrintWarning("unexpected regular file", repo_path_ + "/" + file_name);
    return;
  }
  if (relative_path == kTxnDirectoryName) {
    // transaction directory should be ignored
    return;
  }

  const string full_path = repo_path_ + "/" + relative_path + "/" + file_name;
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
                               const std::string &dir_name)
{
  const string full_path = repo_path_ + "/" + relative_path + "/" + dir_name;
  // Check for nested subdirs
  if (relative_path.size() > 0) {
    PrintWarning("unexpected subdir in CAS subdir", full_path);
    return;
  }

  // Check CAS hash subdirectory name length
  if (! dir_name.empty()                      &&
        dir_name.size() != kHashSubtreeLength &&
        dir_name        != kTxnDirectoryName)
  {
    PrintWarning("malformed CAS subdir length: " +
                 StringifyInt(dir_name.size()), full_path);
  }
}


void CommandScrub::SymlinkCallback(const std::string &relative_path,
                                   const std::string &symlink_name)
{
  const string full_path = repo_path_ + "/" + relative_path + "/" +
                           symlink_name;
  PrintWarning("unexpected symlink", full_path);
}


void CommandScrub::FileProcessedCallback(StoredFile* const& file) {
  if (file->content_hash() != file->expected_hash()) {
    PrintWarning("mismatch of file name and content hash: " +
                 file->content_hash().ToString(), file->path());
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
  if (has_object_modifier   &&
      last_character != 'H' && // history
      last_character != 'C' && // catalog
      last_character != 'P' && // partial
      last_character != 'X' && // certificate
      last_character != 'L')   // micro catalogs (currently only reserved)
  {
    PrintWarning("unknown object modifier: " + string(&last_character, 1),
                 full_path);
    return "";
  }

  const string hash_string = GetFileName(GetParentPath(full_path)) +
    (has_object_modifier
    ? file_name.substr(0, file_name.length()-1)
    : file_name);
  return hash_string;
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
