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
    class ContextBase {
     public:
      ContextBase(SpoolerBackendT *master) :
        master(master),
        base_thread_number(0)
      {
        pthread_mutex_init(&mutex, NULL);
      }

      ~ContextBase() {
        pthread_mutex_destroy(&mutex);
      }

      inline void Lock()   { pthread_mutex_lock  (&mutex); }
      inline void Unlock() { pthread_mutex_unlock(&mutex); }

     public:
      SpoolerBackendT *master;
      int base_thread_number; ///< this is increased by 1 for new threads to have an ID

     private:
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

    bool ProcessJob(StorageJob *job);

   protected:
    virtual void ProcessCopyJob(StorageCopyJob *job)               = 0;
    virtual void ProcessCompressionJob(StorageCompressionJob *job) = 0;

    static int GetNumberOfCpuCores();

   private:
    static const int default_number_of_processors;
  };
}


#endif /* CVMFS_UPLOAD_PUSHWORKER_H_ */
