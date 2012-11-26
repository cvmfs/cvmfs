#ifndef CVMFS_UPLOAD_LOCAL_H_
#define CVMFS_UPLOAD_LOCAL_H_

#include "upload_pushworker.h"

namespace upload
{
  class LocalPushWorker : public AbstractPushWorker {
   public:
    LocalPushWorker(const std::string &spooler_description);

    bool Initialize();
    bool IsReady() const;

    bool ProcessJob(StorageJob *job);
   
   protected:
    void Copy(const std::string &local_path,
              const std::string &remote_path,
              const bool move);
    void Process(const std::string &local_path,
                 const std::string &remote_dir,
                 const std::string &file_suffix,
                 const bool move);

   private:
    const std::string upstream_path_;
    bool initialized_;
  };
}

#endif /* CVMFS_UPLOAD_LOCAL_H_ */
