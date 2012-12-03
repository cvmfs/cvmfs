#ifndef CVMFS_UPLOAD_BACKEND_H_
#define CVMFS_UPLOAD_BACKEND_H_

#include <string>
#include <cstdio>
#include <pthread.h>
#include <vector>

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
    inline virtual bool IsStorageJob()       const { return false; }
    inline virtual bool IsCompressionJob()   const { return false; }
    inline virtual bool IsCopyJob()          const { return false; }
    inline virtual bool IsDeathSentenceJob() const { return false; }
  };

  class DeathSentenceJob : public Job {
   public:
    inline bool IsDeathSentenceJob() const { return true; }
  };

  class StorageJob : public Job {
   public:
    StorageJob(const std::string            &local_path,
               const bool                    move) :
      local_path_(local_path),
      move_(move) {}

    inline bool IsStorageJob() const { return true; }

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
      file_suffix_(file_suffix) {}

    inline bool IsCompressionJob() const { return true; }

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

   private:
    const std::string remote_path_;
  };


  // ---------------------------------------------------------------------------


  template <class PushWorkerT>
  class SpoolerBackend {
   public:
    SpoolerBackend(const std::string &spooler_description);
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

    void Schedule(Job *job);
    Job* AcquireJob();

   private:
    bool OpenPipes();
    bool GetString(FILE *f, std::string *str) const;

    static void* RunPushWorker(void* context);

   private:
    const std::string spooler_description_;

    typename PushWorkerT::Context* pushworker_context_;
    typedef std::vector<pthread_t> WorkerThreads;
    WorkerThreads pushworker_threads_;

    FILE *fpathes_;
    int fd_digests_;
    bool pipes_connected_;
    bool initialized_;
  };

}

#include "upload_backend_impl.h"

#endif /* CVMFS_UPLOAD_BACKEND_H_ */
