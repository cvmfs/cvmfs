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


  template <class PushWorkerT>
  class SpoolerBackend {
   public:
    SpoolerBackend(const std::string &spooler_description,
                   const int          max_pending_jobs = 1000);
    virtual ~SpoolerBackend();

    bool Connect(const std::string &fifo_paths,
                 const std::string &fifo_digests);
    bool Initialize();
    int Run();

    virtual bool IsReady() const;

   protected:
    void EndOfTransaction();
    void Copy(const bool move);
    void Process(const bool move);
    void Unknown(const unsigned char command);

    bool SpawnPushWorkers();

    void SendResult(const int error_code,
                    const std::string &local_path = "",
                    const hash::Any &compressed_hash = hash::Any()) const;
    void SendResult(const StorageJob* storage_job) const;

    void Schedule(Job *job);
    Job* AcquireJob();

   private:
    bool OpenPipes();
    bool GetString(FILE *f, std::string *str) const;

    static void* RunPushWorker(void* context);

   private:
    // PushWorker environment
    typename PushWorkerT::Context* pushworker_context_;
    typedef std::vector<pthread_t> WorkerThreads;
    WorkerThreads pushworker_threads_;

    // Job Queue
    std::queue<Job*> job_queue_;
    size_t           job_queue_max_length_;
    pthread_mutex_t  job_queue_mutex_;
    pthread_cond_t   job_queue_cond_not_empty_;
    pthread_cond_t   job_queue_cond_not_full_;

    // Connection to the user process
    FILE *fpathes_;
    int fd_digests_;

    // Status information and flags
    const std::string spooler_description_;
    bool pipes_connected_;
    bool initialized_;
  };

}

#include "upload_backend_impl.h"

#endif /* CVMFS_UPLOAD_BACKEND_H_ */
