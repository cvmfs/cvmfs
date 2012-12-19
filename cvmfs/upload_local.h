/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_LOCAL_H_
#define CVMFS_UPLOAD_LOCAL_H_

#include "upload.h"

#include "util_concurrency.h"

namespace upload
{
  /**
   * The LocalSpooler implements the AbstractSpooler interface to push files
   * into a local CVMFS repository backend. Compression is done concurrently
   * using the ConcurrentWorkers.
   * For a detailed description of the classes interface please have a look into
   * the AbstractSpooler base class.
   */
  class LocalSpooler : public AbstractSpooler {
   protected:
    /**
     * Implements a concurrent compression worker based on the Concurrent-
     * Workers template. File compression is done in parallel when possible.
     */
    class LocalCompressionWorker : public ConcurrentWorker<LocalCompressionWorker> {
     public:
      typedef compression_parameters expected_data;
      typedef SpoolerResult          returned_data;

      struct worker_context {
        worker_context(const std::string &upstream_path) :
          upstream_path(upstream_path) {}

        const std::string upstream_path; //!< base path to store compression results
      };

     public:
      LocalCompressionWorker(const worker_context *context);
      void operator()(const expected_data &data);

     private:
      const std::string upstream_path_;
    };

   public:
    /**
     * Copy() is not done concurrently in the current implementation of the
     * LocalSpooler, since it is a simple move or copy of a file without CPU
     * intensive operation
     * This method calls NotifyListeners and invokes a callback for all registered
     * listeners (see the Observable template for details).
     */
    void Copy(const std::string &local_path,
              const std::string &remote_path);

    /**
     * Process() schedules a job in the LocalCompressionWorker to allow for
     * concurrent and asynchronous compression.
     * This method calls NotifyListeners and invokes a callback for all registered
     * listeners (see the Observable template for details).
     */
    void Process(const std::string &local_path,
                 const std::string &remote_dir,
                 const std::string &file_suffix);

    void EndOfTransaction();
    void WaitForUpload() const;
    void WaitForTermination() const;

    /**
     * Determines the number of failed jobs in the LocalCompressionWorker as
     * well as in the Copy() command.
     */
    unsigned int GetNumberOfErrors() const;

   protected:
    friend class AbstractSpooler;
    LocalSpooler(const SpoolerDefinition &spooler_definition);

    bool Initialize();
    void TearDown();

    /**
     * This method is called when a compression job finishes execution.
     */
    void CompressionCallback(const LocalCompressionWorker::returned_data &data);

   private:
    // state information
    const std::string    upstream_path_;
    mutable atomic_int32 copy_errors_;   //!< counts the number of occured errors in Copy()

    // concurrency subsystem
    UniquePtr<ConcurrentWorkers<LocalCompressionWorker> > concurrent_compression_;
    UniquePtr<LocalCompressionWorker::worker_context>     worker_context_;
  };
}

#endif /* CVMFS_UPLOAD_LOCAL_H_ */
