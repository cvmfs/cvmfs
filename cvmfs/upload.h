/**
 * This file is part of the CernVM File System.
 */

/**
 * Backend Storage Spooler
 *
 * TODO: Write up the general workflow of file processing and upload...
 */

#ifndef CVMFS_UPLOAD_H_
#define CVMFS_UPLOAD_H_

#include <string>
#include <cstdio>
#include <vector>

#include "hash.h"
#include "upload_file_processor.h"

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
    SpoolerResult(const int           return_code = -1,
                  const std::string  &local_path  = "",
                  const hash::Any    &digest      = hash::Any(),
                  const FileChunks   &file_chunks = FileChunks()) :
      return_code(return_code),
      local_path(local_path),
      content_hash(digest),
      file_chunks(file_chunks) {}

    inline bool IsChunked() const { return !file_chunks.empty(); }

    const int         return_code;  //!< the return value of the spooler operation
    const std::string local_path;   //!< the local_path previously given as input
    const hash::Any   content_hash; //!< the content_hash of the bulk file derived during processing
    const FileChunks  file_chunks;  //!< the file chunks generated during processing
  };


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
    SpoolerDefinition(const std::string& definition_string,
                      const bool          use_file_chunking   = false,
                      const size_t        min_file_chunk_size = 0,
                      const size_t        avg_file_chunk_size = 0,
                      const size_t        max_file_chunk_size = 0);
    bool IsValid() const { return valid_; }

    DriverType  driver_type;           //!< the type of the spooler driver
    std::string temporary_path;        //!< scratch space for the FileProcessor
    std::string spooler_configuration; //!< a driver specific spooler configuration string
                                       //!< (interpreted by the concrete spooler object)
    bool        use_file_chunking;
    size_t      min_file_chunk_size;
    size_t      avg_file_chunk_size;
    size_t      max_file_chunk_size;

    bool valid_;
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
  class AbstractSpooler : public Observable<SpoolerResult>,
                          public PolymorphicConstruction<AbstractSpooler,
                                                         SpoolerDefinition> {
   public:

   public:
    static void RegisterPlugins();

    virtual ~AbstractSpooler();

    /**
     * This method is called once before any other operations are performed on
     * a concrete Spooler. Implement this in your concrete Spooler class to do
     * global initialization work.
     *
     * Note: DO NOT FORGET TO UP-CALL THIS METHOD!
     */
    bool Initialize();

    /**
     * Schedules a copy job that transfers a file found at local_path to the
     * location pointed to by remote_path. Copy Jobs do not hash or compress the
     * given file. They simply upload it.
     * When the copying has finished a callback will be invoked asynchronously.
     *
     * @param local_path    path to the file which needs to be copied into the
     *                      backend storage
     * @param remote_path   the destination of the file to be copied in the
     *                      backend storage
     */
    virtual void Upload(const std::string &local_path,
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
     * @param file_suffix   a suffix that will be appended to the end of the
     *                      final remote path used in the backend storage
     */
    void Process(const std::string &local_path,
                 const bool         allow_chunking = true);

    /**
     * This method should always be called after all desired spooler operations
     * are scheduled. It will wait until all operations are finished an allows
     * the concrete spooler implementations to do potential commiting steps or
     * clean-up work.
     *
     * Note: DO NOT FORGET TO UP-CALL THIS METHOD!
     */
    virtual void EndOfTransaction();

    /**
     * Blocks until all jobs currently under processing are finished.
     * Note: We assume that no one schedules new jobs after this method was
     *       called. Otherwise it might never return, since the job queue does
     *       not get empty.
     *
     * Note: DO NOT FORGET TO UP-CALL THIS METHOD!
     */
    virtual void WaitForUpload() const;

    /**
     * Blocks until all jobs are processed and all PushWorkers terminated
     * successfully.
     * Call this after you have called EndOfTransaction() to wait until the
     * Spooler terminated.
     *
     * Note: DO NOT FORGET TO UP-CALL THIS METHOD!
     */
    virtual void WaitForTermination() const;

    /**
     * Checks how many of the already processed jobs are failed.
     *
     * Note: DO NOT FORGET TO UP-CALL THIS METHOD AND ADD YOUR OWN ERROR COUNT!
     *
     * @return   the number of failed jobs at the time this method is invoked
     */
    virtual unsigned int GetNumberOfErrors() const;


   protected:
    /**
     * Uploads the results of a FileProcessor job. This could be only one file
     * or a list of file chunks + one bulk version of the file.
     *
     * @param data  the results data structure obtained from the FileProcessor
     *              callback method
     */
    virtual void Upload(const FileProcessor::Results &data) = 0;

    /**
     * This method is called right before the Spooler object will terminate.
     * Implement this to do global clean up work. You should not finish jobs
     * in this method, since it is meant to be called after the Spooler has
     * stopped its actual work or was terminated prematurely.
     *
     * Note: DO NOT FORGET TO UP-CALL THIS METHOD!
     */
    virtual void TearDown();


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
     * Concrete implementations of the AbstractSpooler must call this method
     * when they finish an upload job. A single upload job might contain more
     * than one file to be uploaded (see Upload(FileProcessor::Results) ).
     *
     * Note: If the concrete spooler implements uploading as an asynchronous
     *       task, this method MUST be called when all files for one upload
     *       job are processed.
     *
     * The concrete implementations of AbstractSpooler are responsible to fill
     * the SpoolerResult structure properly and pass it to this method.
     *
     * JobDone() will inform Listeners of the Spooler object about the finished
     * job.
     */
    void JobDone(const SpoolerResult &result);

    /**
     * Used internally: Is called when FileProcessor finishes a job.
     * Will automatically take care of, that processed files get uploaded using
     * Upload(FileProcessor::Results)
     */
    void ProcessingCallback(const FileProcessor::Results &data);

    /*
     * @return   the spooler definition that was initially given to any Spooler
     *           constructor.
     */
    inline const SpoolerDefinition& spooler_definition() const {
      return spooler_definition_;
    }

   private:
    // Status Information
    const SpoolerDefinition                      spooler_definition_;

    // File processor
    UniquePtr<ConcurrentWorkers<FileProcessor> > concurrent_processing_;
    UniquePtr<FileProcessor::worker_context >    concurrent_processing_context_;
  };

}

#endif /* CVMFS_UPLOAD_H_ */
