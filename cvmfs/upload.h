/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_BACKEND_H_
#define CVMFS_UPLOAD_BACKEND_H_

#include <string>
#include <cstdio>
#include <pthread.h>
#include <vector>
#include <queue>

#include "hash.h"
#include "atomic.h"

namespace upload
{
  class Job;

  class BackendStat {
   public:
    BackendStat(const std::string &base_path) { base_path_ = base_path; }
    virtual ~BackendStat() { }
    virtual bool Stat(const std::string &path) = 0;
   protected:
    std::string base_path_;
  };


  class LocalStat : public BackendStat {
   public:
    LocalStat(const std::string &base_path) : BackendStat(base_path) { }
    bool Stat(const std::string &path);
  };

  BackendStat *GetBackendStat(const std::string &spooler_definition);

  /**
   * Encapsulates the callback function that handles responses from the external
   * Spooler.
   *
   * This is an abstract base class for the spooler callback function objects.
   * There are two specializations:
   *  --> 1. for static members or global C-like functions
   *  --> 2. for member functions of arbitrary objects
   *
   * Note: digest will be zero, if the callback is invoked after a copy-job has
   *       finished.
   */
  class SpoolerCallbackBase {
   public:
    virtual void operator()(const std::string &path,
                            const int          retval,
                            const hash::Any   &digest = hash::Any(hash::kSha1)) = 0;
  };

  /**
   * This callback function object can be used to call static members or global
   * functions with the following signature:
   * void <name>(const std::string &path,
   *             const int          retval,
   *             const hash::Any   &digest)
   */
  class SpoolerCallback : public SpoolerCallbackBase {
   public:
    typedef void (*Callback)(const std::string &path,
                             const int          retval,
                             const hash::Any   &digest);

    SpoolerCallback(Callback function) :
      function_(function) {}

    void operator()(const std::string &path,
                            const int  retval,
                    const hash::Any   &digest = hash::Any(hash::kSha1)) {
      function_(path, retval, digest);
    }

   private:
    Callback function_;
  };

  /**
   * A BoundSpoolerCallback can be used to call a member of an arbitrary object
   * as a callback for finished spooler jobs.
   * The member must have the following interface:
   * void <class name>::<member name>(const std::string &path,
   *                                  const int          retval,
   *                                  const hash::Any   &digest)
   *
   * @param DelegateT   the <class name> of the object the member <member name>
   *                    should be invoked in
   */
  template <class DelegateT>
  class BoundSpoolerCallback : public SpoolerCallbackBase {
   public:
    typedef void (DelegateT::*Callback)(const std::string &path,
                                        const int          retval,
                                        const hash::Any   &digest);

    BoundSpoolerCallback(DelegateT* delegate, Callback method) :
      delegate_(delegate),
      method_(method) {}
   
    void operator()(const std::string &path,
                    const int          retval,
                    const hash::Any   &digest = hash::Any(hash::kSha1)) {
      (delegate_->*method_)(path, retval, digest);
    }

   private:
    DelegateT* delegate_;
    Callback   method_;
  };


  // ---------------------------------------------------------------------------


  /**
   * The Spooler takes care of the upload procedure of files into a backend
   * storage. It can be extended to multiple supported backend storage types,
   * like f.e. the local file system or a key value storage.
   * For that, the Spooler internally spawns PushWorker objects that distribute
   * files to their specific backend storage. Furthermore the PushWorkers take 
   * care of compression and hashing of files on demand.
   * 
   * PushWorkers can be run in parallel to speed up the compression and upload
   * process.
   */
  class Spooler {
   public:
    /**
     * SpoolerDefinition is given by a string of the form:
     * <spooler type>:<spooler description>
     *
     * F.e: local:/srv/cvmfs/dev.cern.ch
     *      to define a local spooler with upstream path /srv/cvmfs/dev.cern.ch
     */
    struct SpoolerDefinition {
      enum DriverType {
        Riak,
        Local,
        Unknown
      };

      SpoolerDefinition(const std::string& definition_string,
                        const int          max_pending_jobs);
      bool IsValid() const { return valid_; }

      DriverType  driver_type;
      std::string spooler_description;
      std::string paths_out_pipe;
      std::string digests_in_pipe;

      int         max_pending_jobs;

      bool valid_;
    };


   public:
    /**
     * Constructs a spooler object as described in the definition_string. This
     * handles the whole initialization of the concrete PushWorkers and returns
     * a pointer to a ready to use Spooler object.
     *
     * @param definition_string   a spooler definition string as described for
     *                            SpoolerDefinition class
     * @param max_pending_jobs    the maximal number of processing jobs a spooler
     *                            will aggregate in it's job queue
     * @return                    a ready to use Spooler object or NULL if some-
     *                            thing went wrong
     */
    static Spooler* Construct(const std::string &definition_string,
                              const int          max_pending_jobs = 500);
    virtual ~Spooler();

    /**
     * Schedules a copy job that transfers a file found at local_path to the
     * location pointed to by remote_path. Copy Jobs do not hash or compress the
     * given file. They simply upload it.
     * When the processing has finish a callback will be invoked asynchronously.
     *
     * @param local_path    path to the file which needs to be copied into the
     *                      backend storage
     * @param remote_path   the destination of the file to be copied in the
     *                      backend storage
     */
    void Copy(const std::string &local_path,
              const std::string &remote_path);

    /**
     * Schedules a process job that compresses, hashes and uploads the file in
     * local_path and uploads it into the CAS backend. The remote path to the
     * file is determined by the content hash of the compressed file appended by
     * file_suffix.
     * When the processing has finish a callback will be invoked asynchronously.
     *
     * @param local_path    the location of the file to be processed and uploaded
     *                      into the backend storage
     * @param remote_dir    the base directory that should be used in the back-
     *                      end storage <remote_dir>/<content hash><file suffix>
     * @param file_suffix   a suffix that will be appended to the end of the
     *                      final remote path used in the backend storage
     */
    void Process(const std::string &local_path,
                 const std::string &remote_dir,
                 const std::string &file_suffix);

    /**
     * This should be the final call to any Spooler object.
     * It waits until all jobs are finished and terminates the PushWorker threads
     */
    void EndOfTransaction();

    /**
     * Blocks until all jobs currently under processing are finished.
     * Note: We assume that no one schedules new jobs after this method was
     *       called. Otherwise it might never return, since the job queue does
     *       not get empty.
     */
    void WaitForUpload() const;

    /**
     * Blocks until all jobs are processed and all PushWorkers terminated
     * successfully.
     * Call this after you have called EndOfTransaction() to wait until the
     * Spooler terminated.
     */
    virtual void WaitForTermination() const = 0;

    /**
     * Checks if EndOfTransaction was already called. This does not necessarily
     * mean, that all jobs were sucessfully finished, yet.
     * @return   true if transaction has already ended
     */
    inline bool TransactionEnded() const { return transaction_ends_; }
    virtual int GetNumberOfWorkers() const = 0;

    inline int num_errors() { return atomic_read32(&jobs_failed_); }

    /**
     * Set a callback function object that should be invoked for every
     * successfully finished job in the queue.
     * See the description of the SpoolerCallback objects for more details.
     * Note: this method retains the ownership of the provided pointer!
     *
     * @param callback_object    the callback object to be invoked on job finish
     */
    void SetCallback(SpoolerCallbackBase *callback_object);

    /**
     * Releases the callback function object that was provided by SetCallback()
     */
    void UnsetCallback();

    inline void set_move_mode(const bool move) { move_ = move; }

   protected:
    Spooler(const SpoolerDefinition &spooler_definition);

    virtual bool Initialize();
    virtual void TearDown() = 0;

    /**
     * Schedules a new job for execution by one of the PushWorker threads
     *
     * @param job    the job to be scheduled
     */
    void Schedule(Job *job);

    /**
     * Acquires a job from the job queue and returns it. This method blocks until
     * a job is available if the queue is currently empty.
     *
     * @return   a Job object to be processed by the acquiring thread
     */
    Job* AcquireJob();

    /**
     * Spawns the PushWorker nodes (is overwritten in SpoolerImpl template)
     */
    virtual bool SpawnPushWorkers() = 0;

    /**
     * If a Job object is processed (both sucessfully and failed) this method
     * will be called.
     * Note: The call to this method might come from a PushWorker thread, so
     *       keep it thread safe.
     *
     * @param job   the finished job object
     */
    friend class Job;
    void JobFinishedCallback(Job* job);

    /**
     * Called by JobFinishedCallback for each finished Job. Takes care of the
     * invokation of the external SpoolerCallback function object
     *
     * @param job   the finished job object
     */
    void InvokeExternalCallback(Job* job);

    const SpoolerDefinition& spooler_definition() const { return spooler_definition_; }

   private:
    // Callback
    SpoolerCallbackBase *callback_;

    // Job Queue
    std::queue<Job*>         job_queue_;
    mutable pthread_mutex_t  job_queue_mutex_;
    mutable pthread_cond_t   job_queue_cond_not_empty_;
    mutable pthread_cond_t   job_queue_cond_not_full_;
    mutable pthread_cond_t   jobs_all_done_;
    mutable atomic_int32     jobs_pending_;
    mutable atomic_int32     jobs_failed_;
    mutable atomic_int32     death_sentences_executed_;

    // Status Information
    const SpoolerDefinition spooler_definition_;
    bool                    transaction_ends_;
    bool                    initialized_;
    bool                    move_;
  };


  // ---------------------------------------------------------------------------


  /**
   * This template encapsulates the functionality that is dependent on the the
   * concrete PushWorker objects.
   *
   * @param PushWorkerT  the PushWorker class to be used by the Spooler to up-
   *                     load into a specific backend storage
   */
  template <class PushWorkerT>
  class SpoolerImpl : public Spooler {
   public:
    SpoolerImpl(const SpoolerDefinition &spooler_definition) :
      Spooler(spooler_definition) {}

    /**
     * Returns the number of workers that were spawned (is determined by the
     * concrete PushWorker object)
     *
     * @return   the number of actually spawned PushWorker threads
     */
    int GetNumberOfWorkers() const;
    void WaitForTermination() const;

   protected:
    bool SpawnPushWorkers();
    void TearDown();

   private:
    /**
     * Entry function fo a new PushWorker thread. Basically contains the
     * initialization, processing loop and tear down functionality of all Push-
     * Worker objects.
     *
     * @param context   the Context object generated by the PushWorker object
     *                  this is passed by void* pointer here, since we handle
     *                  PushWorkers as pthreads that only allow to pass a void*
     *                  to their entry function.
     */
    static void* RunPushWorker(void* context);

   private:
    typename PushWorkerT::Context* pushworker_context_;

    // PushWorker environment
    typedef std::vector<pthread_t> WorkerThreads;
    WorkerThreads pushworker_threads_;
  };

}

#include "upload_impl.h"

#endif /* CVMFS_UPLOAD_BACKEND_H_ */
