/**
 * This file is part of the CernVM File System.
 */

#define __STDC_FORMAT_MACROS

#include "swissknife_scrub.h"
#include "fs_traversal.h"
#include "logging.h"

#include <sstream>

using namespace swissknife;

const size_t kHashSubtreeLength = 2;
const size_t kHashStringLength  = 40;

CommandScrub::StoredFile::StoredFile(const std::string &path,
                                     const std::string &expected_hash) :
  AbstractFile(path, GetFileSize(path)),
  hash_done_(false),
  hash_context_(hash::kSha1),
  expected_hash_(hash::kSha1, hash::HexPtr(expected_hash))
{
  hash_context_.buffer = malloc(hash_context_.size);
  hash::Init(hash_context_);
}


void CommandScrub::StoredFile::Update(const unsigned char *data,
                                      const size_t nbytes) {
  assert (! hash_done_);
  hash::Update(data, nbytes, hash_context_);
}


void CommandScrub::StoredFile::Finalize() {
  assert (! hash_done_);
  hash::Final(hash_context_, &content_hash_);
  free(hash_context_.buffer);
  hash_context_.buffer = NULL;
  hash_done_ = true;
}



tbb::task* CommandScrub::FileScrubbingTask::execute() {
  StoredFile          *file   = FileScrubbingTask::file();
  upload::CharBuffer  *buffer = FileScrubbingTask::buffer();

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
  assert (relative_path.size() > 0 && file_name.size() > 0);
  const std::string full_path = repo_path_ + "/" + relative_path + "/" + file_name;

  const std::string hash_string = CheckPathAndExtractHash(relative_path,
                                                          file_name,
                                                          full_path);
  if (hash_string.empty()) {
    return;
  }

  assert (reader_ != NULL);
  reader_->ScheduleRead(new StoredFile(full_path, hash_string));
}


void CommandScrub::SymlinkCallback(const std::string &relative_path,
                                   const std::string &symlink_name) {
  PrintWarning("unexpected symlink",
               repo_path_ + "/" + relative_path + "/" + symlink_name);
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
  // check CAS hash subdirectory name length
  if (relative_path.size() != kHashSubtreeLength) {
    std::stringstream ss;
    ss << "malformed CAS subdir length: " << relative_path.size();
    PrintWarning(ss.str(), full_path);
    return "";
  }

  // check for a valid object modifier on the end of the file name
  const char last_character = *(file_name.end() - 1); // TODO: C++11: file_name.back()
  bool has_object_modifier = false;
  if (std::isupper(last_character)) {
    has_object_modifier = true;
  }
  if (has_object_modifier   &&
      last_character != 'H' &&
      last_character != 'C' &&
      last_character != 'P' &&
      last_character != 'X') {
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
    std::stringstream ss1;
    ss1        << has_object_modifier << std::endl
              << file_name << std::endl;

              PrintWarning(ss1.str(), full_path);

              assert (false);

    std::stringstream ss;
    ss << "malformed file name length: " << file_name.size();
    PrintWarning(ss.str(), full_path);
    return "";
  }

  // reconstruct the hash string
  const std::string hash_string = (!has_object_modifier)
    ? relative_path + file_name
    : relative_path + file_name.substr(0, kHashStringLength - kHashSubtreeLength);
  return hash_string;
}


int CommandScrub::Main(const swissknife::ArgumentList &args) {
  repo_path_ = *args.find('r')->second;

  // initialize asynchronous reader
  const size_t       max_buffer_size = 512 * 1024;
  const unsigned int max_buffers_in_flight = 100;
  reader_ = new ScrubbingReader(max_buffer_size, max_buffers_in_flight);
  reader_->RegisterListener(&CommandScrub::FileProcessedCallback, this);

  // initialize file system recursion engine
  FileSystemTraversal<CommandScrub> traverser(this, repo_path_, true);
  traverser.fn_new_file    = &CommandScrub::FileCallback;
  traverser.fn_new_symlink = &CommandScrub::SymlinkCallback;
  traverser.Recurse(repo_path_);

  // wait for reader to finish all jobs
  reader_->Wait();

  return (warnings_ == 0) ? 0 : 1;
}


void CommandScrub::PrintWarning(const std::string &msg,
                                const std::string &path) const {
  LogCvmfs(kLogUtility, kLogStderr, "%s | at: %s\n", msg.c_str(), path.c_str());
  ++warnings_;
}


CommandScrub::~CommandScrub() {
  if (reader_ != NULL) {
    delete reader_;
    reader_ = NULL;
  }
}
