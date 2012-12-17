/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_LOCAL_H_
#define CVMFS_UPLOAD_LOCAL_H_

#include "upload.h"

#include "util_concurrency.h"

namespace upload
{
  class LocalSpooler : public AbstractSpooler {
   protected:
    class LocalCompressionWorker : public ConcurrentWorker<LocalCompressionWorker> {
     public:
      typedef compression_parameters expected_data;
      typedef SpoolerResult          returned_data;

      struct worker_context {
        worker_context(const std::string &upstream_path) :
          upstream_path(upstream_path) {}

        const std::string upstream_path;
      };

     public:
      LocalCompressionWorker(const worker_context *context);
      void operator()(const expected_data &data);

     private:
      const std::string upstream_path_;
    };

   public:
    LocalSpooler(const SpoolerDefinition &spooler_definition);

    void Copy(const std::string &local_path,
              const std::string &remote_path);
    void Process(const std::string &local_path,
                 const std::string &remote_dir,
                 const std::string &file_suffix);

    void EndOfTransaction();
    void WaitForUpload() const;
    void WaitForTermination() const;

    unsigned int num_errors();

   protected:
    bool Initialize();
    void TearDown();

    void CompressionCallback(const LocalCompressionWorker::returned_data &data);

   private:
    const std::string upstream_path_;

    UniquePtr<ConcurrentWorkers<LocalCompressionWorker> > concurrent_compression_;
    UniquePtr<LocalCompressionWorker::worker_context>     worker_context_;
  };
}

#endif /* CVMFS_UPLOAD_LOCAL_H_ */
