/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_SCRUB_H_
#define CVMFS_SWISSKNIFE_SCRUB_H_

#include "swissknife.h"

#include <string>
#include <openssl/sha.h>
#include <cassert>

#include "file_processing/async_reader.h"
#include "file_processing/file.h"

namespace swissknife {

class CommandScrub : public Command {
 private:
  class StoredFile : public upload::AbstractFile {
   public:
    StoredFile(const std::string &path, const std::string &expected_hash);
    void Update(const void *data, const size_t nbytes);
    hash::Any GetHash();

    const hash::Any& expected_hash() const { return expected_hash_; }

   private:
    bool           sha1_done_;
    SHA_CTX        sha1_context_;
    unsigned char  sha1_digest_[SHA_DIGEST_LENGTH];

    hash::Any      expected_hash_;
  };

  class FileScrubbingTask : public upload::AbstractFileScrubbingTask<StoredFile> {
   public:
    FileScrubbingTask(StoredFile           *file,
                      upload::CharBuffer *buffer,
                      const bool          is_last_piece) :
      upload::AbstractFileScrubbingTask<StoredFile>(file, buffer, is_last_piece) {}

   protected:
    tbb::task* execute();
  };

  typedef upload::Reader<FileScrubbingTask, StoredFile> ScrubbingReader;

 public:
  CommandScrub() : reader_(NULL), warnings_(0) {}
  ~CommandScrub();
  std::string GetName() { return "scrub"; }
  std::string GetDescription() {
    return "CernVM File System repository file storage checker. Finds silent "
           "disk corruptions by recomputing all file content checksums in the "
           "backend file storage.";
  };
  ParameterList GetParams();
  int Main(const ArgumentList &args);


 protected:
  void FileCallback(const std::string &relative_path,
                    const std::string &file_name);
  void SymlinkCallback(const std::string &relative_path,
                       const std::string &symlink_name);

  void FileProcessedCallback(StoredFile* const& file);

  void PrintWarning(const std::string &msg, const std::string &path) const;


 private:
  std::string CheckPathAndExtractHash(const std::string &relative_path,
                                      const std::string &file_name,
                                      const std::string &full_path) const;

 private:
  std::string           repo_path_;
  ScrubbingReader      *reader_;
  mutable unsigned int  warnings_;
};

}

#endif  // CVMFS_SWISSKNIFE_SCRUB_H_
