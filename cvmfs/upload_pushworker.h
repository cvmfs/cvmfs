#ifndef CVMFS_UPLOAD_PUSHWORKER_H_
#define CVMFS_UPLOAD_PUSHWORKER_H_

#include "upload_backend.h"

namespace upload
{
  class StoragePushJob {
   public:
    StoragePushJob(const std::string            &local_path,
                   const std::string            &remote_dir,
                   const std::string            &file_suffix,
                   const bool                    move) :
      local_path_(local_path),
      remote_dir_(remote_dir),
      file_suffix_(file_suffix),
      move_(move) {}

    void Wait() const;

   private:
    const std::string             local_path_;
    const std::string             remote_dir_;
    const std::string             file_suffix_;
    const bool                    move_;

    std::string                   compressed_file_path_;
    hash::Any                     content_hash_;
  };

  class AbstractPushWorker {
   public:
    AbstractPushWorker();
    virtual ~AbstractPushWorker();

    virtual bool Initialize();
    virtual bool IsReady() const;

    virtual bool ProcessJob(StoragePushJob *job) = 0;
  };
}


#endif /* CVMFS_UPLOAD_PUSHWORKER_H_ */
