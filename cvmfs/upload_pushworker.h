#ifndef CVMFS_UPLOAD_PUSHWORKER_H_
#define CVMFS_UPLOAD_PUSHWORKER_H_

#include "upload_backend.h"

namespace upload
{
  class StorageJob {
   public:
    StorageJob(const std::string            &local_path,
               const bool                    move) :
      local_path_(local_path),
      move_(move) {}

    void Wait() const;

    inline virtual bool IsCompressionJob() const { return false; }
    inline virtual bool IsCopyJob()        const { return false; }

   protected:
    void Finished();

   private:
    const std::string             local_path_;
    const bool                    move_;
  };

  class StorageCompressionJob : public StorageJob {
   public:
    StorageCompressionJob(const std::string &local_path,
                          const std::string &remote_dir,
                          const std::string &file_suffix,
                          const bool         move) :
      StorageJob(local_path, move),
      remote_dir_(remote_dir),
      file_suffix_(file_suffix) {}

    inline bool IsCompressionJob() const { return true; }

    private:
     const std::string remote_dir_;
     const std::string file_suffix_;

     hash::Any content_hash_;
  };

  class StorageCopyJob : public StorageJob {
   public:
    StorageCopyJob(const std::string &local_path,
                   const std::string &remote_path,
                   const bool         move) :
      StorageJob(local_path, move),
      remote_path_(remote_path) {}

    inline bool IsCopyJob() const { return true; }

   private:
    const std::string remote_path_;
  };

  class AbstractPushWorker {
   public:
    AbstractPushWorker();
    virtual ~AbstractPushWorker();

    virtual bool Initialize();
    virtual bool IsReady() const;

    virtual bool ProcessJob(StorageJob *job) = 0;
  };
}


#endif /* CVMFS_UPLOAD_PUSHWORKER_H_ */
