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
     * Determines the number of failed jobs in the LocalCompressionWorker as
     * well as in the Copy() command.
     */
    unsigned int GetNumberOfErrors() const;

   protected:
    friend class AbstractSpooler;
    LocalSpooler(const SpoolerDefinition &spooler_definition);

   private:
    // state information
    const std::string    upstream_path_;
    mutable atomic_int32 copy_errors_;   //!< counts the number of occured errors in Copy()
  };
}

#endif /* CVMFS_UPLOAD_LOCAL_H_ */
