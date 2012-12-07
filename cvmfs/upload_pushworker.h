#ifndef CVMFS_UPLOAD_PUSHWORKER_H_
#define CVMFS_UPLOAD_PUSHWORKER_H_

#include <pthread.h>

#include "upload.h"

namespace upload
{
  class AbstractPushWorker {
   
   protected:
    class Context {
     public:
      virtual ~Context() {
        pthread_mutex_destroy(&mutex_);
      }

      inline void Lock()   { pthread_mutex_lock  (&mutex_); }
      inline void Unlock() { pthread_mutex_unlock(&mutex_); }

     protected:
      // This class is abstract and should never be instantiated alone.
      Context() {
        pthread_mutex_init(&mutex_, NULL);
      }

     private:
      pthread_mutex_t mutex_;
    };

   public:
    /**
     * Every PushWorker implementation MUST have a context structure containing
     * shared data between the PushWorker instances. Please be careful with
     * thread synchronisation here, contexts are not locked automatically. Every
     * context object contains a mutex for this purpose, though.
     * Consider using util.h LockGuard to lock Contexts.
     * 
     * This is just an abstract base class for all concrete PushWorker contexts
     * and will never be instantiated alone.
     */
    template <class SpoolerT>
    class ContextBase : public Context {
     protected:
      ContextBase(SpoolerT *master) :
        master(master) {}

     public:
      SpoolerT *master;
    };

    /**
     * Generates a context object for a PushWorker swarm to share data between
     * the instances. It is the caller's responsibility to delete this object
     * after all PushWorkers are gone!
     *
     * This method MUST be implemented for each concrete implementation of Push-
     * Worker objects. This method only shows the interface and will produce
     * linker errors when used directly.
     *
     * @param spooler_definition    a struct defining the spooler to construct
     * @return                      a context object to be passed into the
     *                              constructor of every new PushWorker
     */
    static Context* GenerateContext(/* PushWorkerT *master, */ const Spooler::SpoolerDefinition& spooler_definition);

    /**
     * Determines the number of workers to be spawned for the desired worker
     * swarm.
     *
     * This method MUST be implemented for each concrete implementation of Push-
     * Worker objects. This method only shows the interface and will produce
     * linker errors when used directly.
     *
     * @param context   pointer to a generated Context object containing infor-
     *                  mation about the worker swarm to be created
     *                  See AbstractPushWorker::Context for more information
     * @return          the number of PushWorkers to be spawned
     */
    static int GetNumberOfWorkers(const Context *context);

    /**
     * This method can do global initialization work before any PushWorker is
     * instanciated. For example it can create some global state.
     *
     * The default implementation here is essentially a NOOP
     *
     * @return  true on success, false otherwise
     */
    static bool DoGlobalInitialization();

    /**
     * This method can do global cleanup work after all PushWorkers are destroyed
     *
     * The default implementation here is essentially a NOOP
     */
    static void DoGlobalCleanup();

   public:
    virtual bool Initialize();
    virtual bool IsReady() const;

    void ProcessJob(StorageJob *job);

   protected:
    virtual void ProcessCopyJob(StorageCopyJob *job)               = 0;
    virtual void ProcessCompressionJob(StorageCompressionJob *job) = 0;

    static int GetNumberOfCpuCores();

   private:
    static const int default_number_of_processors;
  };
}


#endif /* CVMFS_UPLOAD_PUSHWORKER_H_ */
