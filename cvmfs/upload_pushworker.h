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
     * 
     * This is just an abstract base class for all concrete PushWorker contexts
     * and will never be instantiated alone.
     */
    template <class SpoolerBackendT>
    struct ContextBase {
      ContextBase(SpoolerBackendT *master) :
        master(master)
      {
        pthread_mutex_init(&mutex, NULL);
      }

      SpoolerBackendT *master;
      pthread_mutex_t mutex;
    };

    /**
     * Generates a context object for a PushWorker swarm to share data between
     * the instances. It is the caller's responsibility to delete this object
     * after all PushWorkers are gone!
     *
     * This method MUST be implemented for each concrete implementation of Push-
     * Worker objects.
     *
     * @param spooler_description   the description string of the spooler backend
     *                              to be initialized
     * @return                      a context object to be passed into the
     *                              constructor of every new PushWorker
     */
    //static Context* GenerateContext(const std::string &spooler_description);

    /**
     * Determines the number of workers to be spawned for the desired worker
     * swarm.
     *
     * This method MUST be implemented for each concrete implementation of Push-
     * Worker objects.
     *
     * @param context   pointer to a generated Context object containing infor-
     *                  mation about the worker swarm to be created
     *                  See AbstractPushWorker::Context for more information
     * @return          the number of PushWorkers to be spawned
     */
    //static int GetNumberOfWorkers(const Context *context);

   public:
    AbstractPushWorker();
    virtual ~AbstractPushWorker();

    virtual bool Initialize();
    virtual bool IsReady() const;

    virtual bool ProcessJob(StorageJob *job) = 0;
  };
}


#endif /* CVMFS_UPLOAD_PUSHWORKER_H_ */
