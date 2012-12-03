#ifndef CVMFS_UPLOAD_LOCAL_H_
#define CVMFS_UPLOAD_LOCAL_H_

#include "upload_pushworker.h"

namespace upload
{
  class LocalPushWorker : public AbstractPushWorker {
   public:
    /**
     * See AbstractPushWorker for description
     */
    struct Context : public AbstractPushWorker::ContextBase<SpoolerBackend<LocalPushWorker> > {
      Context(SpoolerBackend<LocalPushWorker> *master,
              const std::string               &upstream_path) :
        AbstractPushWorker::ContextBase<SpoolerBackend<LocalPushWorker> >(master),
        upstream_path(upstream_path) {}

      const std::string upstream_path;
    };

    /**
     * See AbstractPushWorker for description
     */
    static Context* GenerateContext(SpoolerBackend<LocalPushWorker> *master,
                                    const std::string               &upstream_path);

    /**
     * See AbstractPushWorker for description
     */
    static int GetNumberOfWorkers(const Context *context);


   public:
    LocalPushWorker(Context *context);

    bool Initialize();
    bool IsReady() const;

    bool ProcessJob(StorageJob *job);
   
   protected:
    void Copy(StorageCopyJob *copy_job);
    void CompressAndProcess(StorageCompressionJob *compression_job);

   private:
    Context *context_;

    const std::string upstream_path_;
    bool initialized_;
  };
}

#endif /* CVMFS_UPLOAD_LOCAL_H_ */
