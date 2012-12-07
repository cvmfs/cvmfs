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
    class Context : public AbstractPushWorker::ContextBase<SpoolerImpl<LocalPushWorker> > {
     public:
      Context(SpoolerImpl<LocalPushWorker> *master,
              const std::string            &upstream_path) :
        AbstractPushWorker::ContextBase<SpoolerImpl<LocalPushWorker> >(master),
        upstream_path(upstream_path) {}

     public:
      const std::string upstream_path;
    };

    /**
     * See AbstractPushWorker for description
     */
    static Context* GenerateContext(SpoolerImpl<LocalPushWorker>     *master,
                                    const Spooler::SpoolerDefinition &spooler_definition);

    /**
     * See AbstractPushWorker for description
     */
    static int GetNumberOfWorkers(const Context *context);


   public:
    LocalPushWorker(Context *context);

    bool Initialize();
    bool IsReady() const;
   
   protected:
    void ProcessCopyJob(StorageCopyJob *copy_job);
    void ProcessCompressionJob(StorageCompressionJob *compression_job);

   private:
    Context *context_;

    const std::string upstream_path_;
    bool initialized_;
  };
}

#endif /* CVMFS_UPLOAD_LOCAL_H_ */
