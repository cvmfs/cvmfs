#ifndef CVMFS_UPLOAD_PUSHWORKER_H_
#define CVMFS_UPLOAD_PUSHWORKER_H_

#include "upload_backend.h"

namespace upload
{
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
