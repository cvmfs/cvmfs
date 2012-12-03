#ifndef CVMFS_UPLOAD_PUSHWORKER_H_
#define CVMFS_UPLOAD_PUSHWORKER_H_

#include <pthread.h>

#include "upload_backend.h"

namespace upload
{
  class AbstractPushWorker {
   public:
    /**
     * Every PushWorker implementation MUST have a context structure containing
     * shared data between the PushWorker instances. Please be careful with
     * thread synchronisation here, contexts are not locked automatically. Every
     * context object contains a mutex for this purpose, though.
     */
    struct Context {
      Context() {
        pthread_mutex_init(&mutex, NULL);
      }

      pthread_mutex_t mutex;
    };

    /**
     * Generates a context object for a PushWorker swarm to share data between
     * the instances. It is the caller's responsibility to delete this object
     * after all PushWorkers are gone!
     * @param spooler_description   the description string of the spooler backend
     *                              to be initialized
     * @return                      a context object to be passed into the
     *                              constructor of every new PushWorker
     */
    static Context* GenerateContext(const std::string &spooler_description);

   public:
    AbstractPushWorker(Context* context);
    virtual ~AbstractPushWorker();

    virtual bool Initialize();
    virtual bool IsReady() const;

    virtual bool ProcessJob(StorageJob *job) = 0;
  };
}


#endif /* CVMFS_UPLOAD_PUSHWORKER_H_ */
