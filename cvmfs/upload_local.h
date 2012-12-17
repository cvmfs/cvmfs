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
      struct expected_data {
        expected_data(const std::string &local_path,
                      const std::string &remote_dir,
                      const std::string &file_suffix,
                      const bool         move) :
          local_path(local_path),
          remote_dir(remote_dir),
          file_suffix(file_suffix),
          move(move) {}

        expected_data() : local_path(), remote_dir(), file_suffix(), move(false) {}

        const std::string local_path;
        const std::string remote_dir;
        const std::string file_suffix;
        const bool        move;
      };

      struct returned_data {
        returned_data(const int          return_code,
                      const std::string &local_path = "",
                      const hash::Any   &content_hash = hash::Any()) :
          return_code(return_code),
          local_path(local_path),
          content_hash(content_hash) {}

        const int         return_code;
        const std::string local_path;
        const hash::Any   content_hash;
      };

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

    int num_errors();

   protected:
    bool Initialize();
    void TearDown();

    void CompressionCallback(const LocalCompressionWorker::returned_data &data);

   private:
    const std::string upstream_path_;

    ConcurrentWorkers<LocalCompressionWorker> *concurrent_compression_;
    LocalCompressionWorker::worker_context    *worker_context_;
  };
}

#endif /* CVMFS_UPLOAD_LOCAL_H_ */
