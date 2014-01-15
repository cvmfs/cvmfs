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
#include "hash.h"

namespace swissknife {

class CommandScrub : public Command {
 private:
  class StoredFile : public upload::AbstractFile {
   public:
    StoredFile(const std::string &path, const std::string &expected_hash);
    void Update(const unsigned char *data, const size_t nbytes);
    void Finalize();

    const shash::Any& content_hash() const {
      assert(hash_done_); return content_hash_;
    }
    const shash::Any& expected_hash() const { return expected_hash_; }

   private:
    bool              hash_done_;
    shash::ContextPtr hash_context_;

    shash::Any        content_hash_;
    shash::Any        expected_hash_;
  };

  class StoredFileScrubbingTask :
                          public upload::AbstractFileScrubbingTask<StoredFile> {
   public:
    StoredFileScrubbingTask(StoredFile              *file,
                            upload::CharBuffer      *buffer,
                            const bool               is_last_piece,
                            upload::AbstractReader  *reader) :
      upload::AbstractFileScrubbingTask<StoredFile>(file,
                                                    buffer,
                                                    is_last_piece,
                                                    reader) {}

   protected:
    tbb::task* execute();
  };

  typedef upload::Reader<StoredFileScrubbingTask, StoredFile> ScrubbingReader;

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
  void DirCallback(const std::string &relative_path,
                   const std::string &dir_name);
  void SymlinkCallback(const std::string &relative_path,
                       const std::string &symlink_name);

  void FileProcessedCallback(StoredFile* const& file);

  void PrintWarning(const std::string &msg, const std::string &path) const;


 private:
  std::string CheckPathAndExtractHash(const std::string &relative_path,
                                      const std::string &file_name,
                                      const std::string &full_path) const;
  bool CheckHashString(const std::string &hash_string,
                       const std::string &full_path) const;

 private:
  std::string              repo_path_;
  ScrubbingReader         *reader_;
  mutable unsigned int     warnings_;
  mutable pthread_mutex_t  warning_mutex_;
};

}

#endif  // CVMFS_SWISSKNIFE_SCRUB_H_
