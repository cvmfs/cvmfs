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


  /**
   * This data structure will be passed to every callback spoolers will invoke.
   * It encapsulates the results of a spooler command along with the given
   * local_path to identify the spooler action performed.
   *
   * Note: When the return_code is different from 0 the content_hash is most
   *       likely undefined, Null or rubbish.
   */
  struct SpoolerResult {
    SpoolerResult(const int         return_code = -1,
                  const std::string &local_path = "",
                  const hash::Any   &digest     = hash::Any()) :
      return_code(return_code),
      local_path(local_path),
      content_hash(digest) {}

    const int         return_code;  //!< the return value of the spooler operation
    const std::string local_path;   //!< the local_path previously given as input
    const hash::Any   content_hash; //!< the content_hash derived during processing
  };

  /**
   * The Spooler takes care of the upload procedure of files into a backend
   * storage. It can be extended to multiple supported backend storage types,
   * like f.e. the local file system or a key value storage.
   *
   * This AbstractSpooler defines not much more than the common spooler inter-
   * face. There are derived classes that actually implement different types of
   * spoolers.
   *
   * Note: A spooler is derived from the Observable template, meaning that it
   *       allows for Listeners to be registered onto it.
   *       Concrete implementations of this class should take care of calling
   *       NotifyListeners() when a spooler job has finished.
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

      /**
       * Reads a given definition_string as described above and interprets
       * it. If the provided string turns out to be malformed the created
       * SpoolerDefinition object will not be valid. A user should check this
       * after creation using IsValid().
       *
       * @param definition_string   the spooler definition string to be inter-
       *                            preted by the constructor
       */
      SpoolerDefinition(const std::string& definition_string);
      bool IsValid() const { return valid_; }

      DriverType  driver_type;         //!< the type of the spooler driver
      std::string spooler_description; //!< a driver specific spooler description (interpreted by the concrete spooler object)
      std::string temp_directory;      //!< the temp directory to use as scratch space

      bool valid_;
    };


    /**
     * This structure encapsulates the required data for a spooler compression
     * operation and is used internally by concrete spoolers as input to their
     * concurrent compression workers.
     * An external user of AbstractSpooler and its derived classes should NOT
     * use this structure.
     */
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
      // (needed by the ConcurrentWorkers implementation)
      compression_parameters() :
        local_path(), remote_dir(), file_suffix(), move(false) {}

      const std::string local_path;
      const std::string remote_dir;
      const std::string file_suffix;
      const bool        move;
    };


   public:
    /**
     * Instantiates a concrete spooler class according to the given spooler
     * definition string. This static method should be used as a replacement
     * for a usual constructor from the outside.
     *
     * @param definition_string   a spooler definition string describing the
     *                            spooler object to be generated
     * @return   a concrete instance of a Spooler backend that allows for
     *           file upload into different backend storages.
     */
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
     * Schedules a process job that compresses and hashes the provided file in
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
     * This method should always be called after all desired spooler operations
     * are scheduled. It will wait until all operations are finished an allows
     * the concrete spooler implementations to do potential commiting steps or
     * clean-up work.
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

    /**
     * Checks how many of the already processed jobs are failed.
     *
     * @return   the number of failed jobs at the time this method is invoked
     */
    virtual unsigned int GetNumberOfErrors() const = 0;

    inline void set_move_mode(const bool move) { move_ = move; }

   protected:
    /**
     * No concrete spooler should have a public constructor. Instead one should
     * extend the static Construct() method of AbstractSpooler to transparently
     * create a new Spooler based on the given spooler definition string.
     *
     * Note: Concrete spoolers should overwrite this constructor and process the
     *       given spooler_definition in it.
     *
     * @param spooler_definition   the SpoolerDefinition structure that defines
     *                             some intrinsics of the concrete Spoolers.
     */
    AbstractSpooler(const SpoolerDefinition &spooler_definition);

    /**
     * This method is called once before any other operations are performed on
     * a concrete Spooler. Implement this in your concrete Spooler class to do
     * global initialization work.
     */
    virtual bool Initialize() = 0;

    /**
     * This method is called right before the Spooler object will terminate.
     * Implement this to do global clean up work. You should not finish jobs
     * in this method, since it is meant to be called after the Spooler has
     * stopped its actual work or was terminated prematurely.
     */
    virtual void TearDown() = 0;

    /*
     * @return   the spooler definition that was initially given to any Spooler
     *           constructor.
     */
    inline const SpoolerDefinition& spooler_definition() const { return spooler_definition_; }
    inline bool move() const { return move_; }

   private:
    // Status Information
    const SpoolerDefinition spooler_definition_;
    bool                    move_;
  };

}

#endif /* CVMFS_UPLOAD_H_ */
