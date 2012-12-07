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
   */
  class SpoolerCallbackBase {
   public:
    virtual void operator()(const std::string &path,
                                    const int  retval,
                            const hash::Any   &digest = hash::Any(hash::kSha1)) = 0;
  };

  class SpoolerCallback : public SpoolerCallbackBase {
   public:
    typedef void (*Callback)(const std::string &path,
                             const int retval,
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


  class Spooler {
   protected:
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

      SpoolerDefinition(const std::string& definition_string);
      bool IsValid() const { return valid_; }

      DriverType  driver_type;
      std::string spooler_description;
      std::string upstream_urls;
      std::string paths_out_pipe;
      std::string digests_in_pipe;

      bool valid_;
    };


   public:
    static Spooler* Construct(const std::string &definition_string,
                              const int          max_pending_jobs = 1000);
    virtual ~Spooler();

    void Copy(const std::string &local_path,
              const std::string &remote_path);
    void Process(const std::string &local_path,
                 const std::string &remote_dir,
                 const std::string &file_suffix);
    void EndOfTransaction();
    virtual void Wait() const = 0;

    inline bool TransactionEnded() const { return transaction_ends_; }
    virtual int GetNumberOfWorkers() const = 0;

    inline int num_errors() { return atomic_read32(&jobs_failed_); }

    void SetCallback(SpoolerCallbackBase *callback_object);
    void UnsetCallback();

    inline void set_move_mode(const bool move) { move_ = move; }

   protected:
    Spooler(const std::string &spooler_description,
            const int          max_pending_jobs);

    virtual bool Initialize();

    void Schedule(Job *job);
    Job* AcquireJob();

    virtual bool SpawnPushWorkers() = 0;

    friend class Job;
    void JobFinishedCallback(Job* job);
    void InvokeExternalCallback(Job* job);

    inline const std::string& spooler_description() const { return spooler_description_; }

   private:
    // Callback
    SpoolerCallbackBase *callback_;

    // Job Queue
    std::queue<Job*> job_queue_;
    size_t           job_queue_max_length_;
    pthread_mutex_t  job_queue_mutex_;
    pthread_cond_t   job_queue_cond_not_empty_;
    pthread_cond_t   job_queue_cond_not_full_;
    atomic_int32     jobs_pending_;
    atomic_int32     jobs_failed_;

    // Status Information
    const std::string spooler_description_;
    bool              transaction_ends_;
    bool              initialized_;
    bool              move_;
  };


  // ---------------------------------------------------------------------------


  template <class PushWorkerT>
  class SpoolerImpl : public Spooler {
   public:
    SpoolerImpl(const std::string &spooler_description,
                const int          max_pending_jobs) :
      Spooler(spooler_description, max_pending_jobs) {}

    int GetNumberOfWorkers() const;
    void Wait() const;

   protected:
    bool SpawnPushWorkers();

   private:
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
