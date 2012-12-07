/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_JOBS_H_
#define CVMFS_UPLOAD_JOBS_H_

#include <string>

#include "hash.h"

namespace upload {

  class Spooler;

  class Job {
   public:
    Job(Spooler* delegate) : return_code_(-1), delegate_(delegate) {}

    inline virtual bool IsStorageJob()       const { return false; }
    inline virtual bool IsCompressionJob()   const { return false; }
    inline virtual bool IsCopyJob()          const { return false; }
    inline virtual bool IsDeathSentenceJob() const { return false; }
    inline virtual std::string name() const { return "Abstract Job"; }

    inline bool IsSuccessful() const                { return return_code_ == 0; }
    inline void Failed(const int return_code = 1)   { Done(return_code); }
    inline void Finished(const int return_code = 0) { Done(return_code); }

    inline int return_code() const { return return_code_; }

   protected:
    void Done(const int return_code);

   private:
    int return_code_;
    Spooler* delegate_;
  };

  class DeathSentenceJob : public Job {
   public:
    DeathSentenceJob(Spooler *delegate) : Job(delegate) {}

    inline bool IsDeathSentenceJob() const { return true; }
    inline virtual std::string name() const { return "Death Sentence Job"; }
  };

  class StorageJob : public Job {
   public:
    StorageJob(const std::string            &local_path,
               const bool                    move,
               Spooler                      *delegate) :
      Job(delegate),
      local_path_(local_path),
      move_(move) {}

    inline bool IsStorageJob() const { return true; }
    inline virtual std::string name() const { return "Abstract Storage Job"; }

    inline bool move()                     const { return move_; }
    inline const std::string& local_path() const { return local_path_; }

   private:
    const std::string             local_path_;
    const bool                    move_;
  };

  class StorageCompressionJob : public StorageJob {
   public:
    StorageCompressionJob(const std::string &local_path,
                          const std::string &remote_dir,
                          const std::string &file_suffix,
                          const bool         move,
                          Spooler           *delegate) :
      StorageJob(local_path, move, delegate),
      remote_dir_(remote_dir),
      file_suffix_(file_suffix),
      content_hash_(hash::kSha1) {}

    inline bool IsCompressionJob() const { return true; }
    inline virtual std::string name() const { return "Compression Job"; }

    inline const std::string& remote_dir()   const { return remote_dir_; }
    inline const std::string& file_suffix()  const { return file_suffix_; }
    inline const hash::Any&   content_hash() const { return content_hash_; }
    inline hash::Any&         content_hash()       { return content_hash_; }

    private:
     const std::string remote_dir_;
     const std::string file_suffix_;

     hash::Any content_hash_;
  };

  class StorageCopyJob : public StorageJob {
   public:
    StorageCopyJob(const std::string &local_path,
                   const std::string &remote_path,
                   const bool         move,
                   Spooler           *delegate) :
      StorageJob(local_path, move, delegate),
      remote_path_(remote_path) {}

    inline bool IsCopyJob() const { return true; }
    inline virtual std::string name() const { return "Copy Job"; }

    inline const std::string& remote_path() const { return remote_path_; }

   private:
    const std::string remote_path_;
  };

}

 #endif