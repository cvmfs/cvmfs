#ifndef CVMFS_UPLOAD_BACKEND_H_
#define CVMFS_UPLOAD_BACKEND_H_

#include <string>
#include <cstdio>
#include <pthread.h>
#include <vector>
#include <queue>

#include "hash.h"

namespace upload
{
  /**
   * Commands to the spooler
   */
  enum Commands {
    kCmdProcess           = 1,
    kCmdCopy,
    kCmdEndOfTransaction,
    kCmdMoveFlag          = 128,
  };


  // ---------------------------------------------------------------------------


  class Job {
   public:
    Job() : return_code_(-1) {}

    inline virtual bool IsStorageJob()       const { return false; }
    inline virtual bool IsCompressionJob()   const { return false; }
    inline virtual bool IsCopyJob()          const { return false; }
    inline virtual bool IsDeathSentenceJob() const { return false; }
    inline virtual std::string name() const { return "Abstract Job"; }

    inline bool IsSuccessful() const          { return return_code_ == 0; }
    inline void Failed(int return_code = 1)   { return_code_ = return_code; }
    inline void Finished(int return_code = 0) { return_code_ = return_code; }

    inline int return_code() const { return return_code_; }

   private:
    int return_code_;
  };

  class DeathSentenceJob : public Job {
   public:
    inline bool IsDeathSentenceJob() const { return true; }
    inline virtual std::string name() const { return "Death Sentence Job"; }
  };

  class StorageJob : public Job {
   public:
    StorageJob(const std::string            &local_path,
               const bool                    move) :
      local_path_(local_path),
      move_(move) {}

    inline bool IsStorageJob() const { return true; }
    inline virtual std::string name() const { return "Abstract Storage Job"; }

    inline bool move()                     const { return move_; }
    inline const std::string& local_path() const { return local_path_; }

   private:
    const std::string             local_path_;
    const bool                    move_;
  };

  class StorageCompressionJob : public StorageJob {
   public:
    StorageCompressionJob(const std::string &local_path,
                          const std::string &remote_dir,
                          const std::string &file_suffix,
                          const bool         move) :
      StorageJob(local_path, move),
      remote_dir_(remote_dir),
      file_suffix_(file_suffix),
      content_hash_(hash::kSha1) {}

    inline bool IsCompressionJob() const { return true; }
    inline virtual std::string name() const { return "Compression Job"; }

    inline const std::string& remote_dir()   const { return remote_dir_; }
    inline const std::string& file_suffix()  const { return file_suffix_; }
    inline const hash::Any&   content_hash() const { return content_hash_; }
    inline hash::Any&         content_hash()       { return content_hash_; }

    private:
     const std::string remote_dir_;
     const std::string file_suffix_;

     hash::Any content_hash_;
  };

  class StorageCopyJob : public StorageJob {
   public:
    StorageCopyJob(const std::string &local_path,
                   const std::string &remote_path,
                   const bool         move) :
      StorageJob(local_path, move),
      remote_path_(remote_path) {}

    inline bool IsCopyJob() const { return true; }
    inline virtual std::string name() const { return "Copy Job"; }

    inline const std::string& remote_path() const { return remote_path_; }

   private:
    const std::string remote_path_;
  };


  // ---------------------------------------------------------------------------
  

  class SpoolerBackend {
   public:
    static SpoolerBackend* Construct(const int          driver_type, // THIS IS SUBJECT TO CHANGE!
                                     const std::string &spooler_description,
                                     const int          max_pending_jobs = 1000);

    virtual ~SpoolerBackend();

    int Run();

    void Copy(const std::string &local_path,
              const std::string &remote_path,
              const bool move);
    void Process(const std::string &local_path,
                 const std::string &remote_dir,
                 const std::string &file_suffix,
                 const bool move);

    void EndOfTransaction();

    virtual int GetNumberOfWorkers() const = 0;
    void Wait();

   protected:
    SpoolerBackend(const std::string &spooler_description,
                   const int          max_pending_jobs);

    virtual bool Initialize();

    void Schedule(Job *job);
    Job* AcquireJob();

    virtual bool SpawnPushWorkers() = 0;

    inline const std::string& spooler_description() const { return spooler_description_; }

   private:
    // Job Queue
    std::queue<Job*> job_queue_;
    size_t           job_queue_max_length_;
    pthread_mutex_t  job_queue_mutex_;
    pthread_cond_t   job_queue_cond_not_empty_;
    pthread_cond_t   job_queue_cond_not_full_;

    // Status Information
    const std::string spooler_description_;

    // Status information and flags
    bool initialized_;
  };


  // ---------------------------------------------------------------------------


  template <class PushWorkerT>
  class SpoolerBackendImpl : public SpoolerBackend {
   public:
    SpoolerBackendImpl(const std::string &spooler_description,
                       const int          max_pending_jobs) :
      SpoolerBackend(spooler_description, max_pending_jobs) {}

    int GetNumberOfWorkers() const;

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

#include "upload_backend_impl.h"

#endif /* CVMFS_UPLOAD_BACKEND_H_ */
