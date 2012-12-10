/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_PUSHWORKER_H_
#define CVMFS_UPLOAD_PUSHWORKER_H_

#include <pthread.h>

#include "upload.h"

namespace upload
{
  /**
   * The AbstractPushWorker describes an interface for concrete PushWorkers.
   * Each concrete PushWorker can generate a Context object before any of its
   * objects is instantiated. Use this to facilitate a global state between Push-
   * Worker objects where necessary.
   * Before instantiation of any PushWorker object a concrete PushWorker has to
   * decide how many PushWorker instances should be created. Each of these
   * instances will then be run concurrently in different threads.
   */
  class AbstractPushWorker {
   
   protected:
    /**
     * Base class for a PushWorker Context object. Before startup of any Push-
     * Worker object this will be constructed using a static method each Push-
     * Worker class should provide.
     * The context contains a mutex object and can be used to produce a global
     * state between instances of the same PushWorker class.
     */
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
     * Consider using the LockGuard clas in util.h to lock Contexts.
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
    AbstractPushWorker() : processed_jobs_count_(0) {}
    virtual ~AbstractPushWorker();

    virtual bool Initialize();
    virtual bool IsReady() const;

    /**
     * This method is called by the Spooler for each Job a concrete PushWorker
     * instance should process. The threading and distribution of the Jobs is
     * handled by the Spooler object as well.
     * This method distributes the Job depending on its type to one of the abstract
     * processing methods listed below. These should be implemented by concrete
     * PushWorker classes.
     *
     * @param job   the job that should be processed by the PushWorker
     */
    void ProcessJob(StorageJob *job);

   protected:
    virtual void ProcessCopyJob(StorageCopyJob *job)               = 0;
    virtual void ProcessCompressionJob(StorageCompressionJob *job) = 0;

    /**
     * @return  the number of CPU cores currently present in the system
     */
    static int GetNumberOfCpuCores();

   private:
    static const int default_number_of_processors;

    int processed_jobs_count_;
  };
}


#endif /* CVMFS_UPLOAD_PUSHWORKER_H_ */
