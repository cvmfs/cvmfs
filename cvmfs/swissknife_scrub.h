/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_SCRUB_H_
#define CVMFS_SWISSKNIFE_SCRUB_H_

#include "swissknife.h"

#include <cassert>
#include <string>

#include "file_processing/async_reader.h"
#include "file_processing/file.h"
#include "hash.h"

namespace swissknife {

class CommandScrub : public Command {
 private:
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

    static const char* ToString(const Type t);
  };

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
  CommandScrub() : machine_readable_output_(false),
                   reader_(NULL),
                   alerts_(0) { }
  ~CommandScrub();
  std::string GetName() { return "scrub"; }
  std::string GetDescription() {
    return "CernVM File System repository file storage checker. Finds silent "
           "disk corruptions by recomputing all file content checksums in the "
           "backend file storage.";
  }
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

  void PrintAlert(const Alerts::Type   type,
                  const std::string   &path,
                  const std::string   &affected_hash = "") const;
  void ShowAlertsHelpMessage() const;

 private:
  std::string CheckPathAndExtractHash(const std::string &relative_path,
                                      const std::string &file_name,
                                      const std::string &full_path) const;
  bool CheckHashString(const std::string &hash_string,
                       const std::string &full_path) const;

  std::string MakeFullPath(const std::string &relative_path,
                           const std::string &file_name) const;

 private:
  std::string                   repo_path_;
  bool                          machine_readable_output_;
  ScrubbingReader              *reader_;

  mutable unsigned int          alerts_;
  mutable pthread_mutex_t       alerts_mutex_;
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_SCRUB_H_
