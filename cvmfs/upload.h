/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_H_
#define CVMFS_UPLOAD_H_

#include <string>
#include <cstdio>
#include <vector>

#include "hash.h"
#include "atomic.h"

#include "util_concurrency.h"

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


  // ---------------------------------------------------------------------------


  struct SpoolerResult {
    SpoolerResult(const int         return_code = -1,
                  const std::string &local_path = "",
                  const hash::Any   &digest     = hash::Any()) :
      return_code(return_code),
      local_path(local_path),
      content_hash(digest) {}

    const int         return_code;
    const std::string local_path;
    const hash::Any   content_hash;
  };

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
  class AbstractSpooler : public Observable<SpoolerResult> {
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

      SpoolerDefinition(const std::string& definition_string);
      bool IsValid() const { return valid_; }

      DriverType  driver_type;
      std::string spooler_description;
      std::string paths_out_pipe;
      std::string digests_in_pipe;

      bool valid_;
    };


    struct compression_parameters {
      compression_parameters(const std::string &local_path,
                             const std::string &remote_dir,
                             const std::string &file_suffix,
                             const bool         move) :
        local_path(local_path),
        remote_dir(remote_dir),
        file_suffix(file_suffix),
        move(move) {}

      // default constructor to create an 'empty' struct
      compression_parameters() :
        local_path(), remote_dir(), file_suffix(), move(false) {}

      const std::string local_path;
      const std::string remote_dir;
      const std::string file_suffix;
      const bool        move;
    };


    struct callback_param {

    };


   public:
    static AbstractSpooler* Construct(const std::string &definition_string);
    virtual ~AbstractSpooler();

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
    virtual void Copy(const std::string &local_path,
                      const std::string &remote_path) = 0;

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
    virtual void Process(const std::string &local_path,
                         const std::string &remote_dir,
                         const std::string &file_suffix) = 0;

    /**
     * This should be the final call to any Spooler object.
     * It waits until all jobs are finished and terminates the PushWorker threads
     */
    virtual void EndOfTransaction() = 0;

    /**
     * Blocks until all jobs currently under processing are finished.
     * Note: We assume that no one schedules new jobs after this method was
     *       called. Otherwise it might never return, since the job queue does
     *       not get empty.
     */
    virtual void WaitForUpload() const = 0;

    /**
     * Blocks until all jobs are processed and all PushWorkers terminated
     * successfully.
     * Call this after you have called EndOfTransaction() to wait until the
     * Spooler terminated.
     */
    virtual void WaitForTermination() const = 0;

    virtual unsigned int num_errors() = 0;

    inline void set_move_mode(const bool move) { move_ = move; }

   protected:
    AbstractSpooler(const SpoolerDefinition &spooler_definition);

    virtual bool Initialize() = 0;
    virtual void TearDown() = 0;

    inline const SpoolerDefinition& spooler_definition() const { return spooler_definition_; }
    inline bool move() const { return move_; }

   private:
    // Status Information
    const SpoolerDefinition spooler_definition_;
    bool                    move_;
  };

}

#endif /* CVMFS_UPLOAD_H_ */
