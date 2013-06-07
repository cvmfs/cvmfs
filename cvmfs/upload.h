/**
 * This file is part of the CernVM File System.
 */

/**
 *    Backend Storage Spooler
 *    ~~~~~~~~~~~~~~~~~~~~~~~
 *
 * This is the entry point to the general file processing facility of the CVMFS
 * backend. It works with a two-stage approach:
 *
 *   1. Process file content
 *      -> create smaller file chunks for big input files
 *      -> compress the file content (optionally chunked)
 *      -> generate a content hash of the compression result
 *
 *   2. Upload files
 *      -> pluggable to support different upload pathes (local, Riak, ...)
 *
 * There are a number of different entities involved in this process. Namely:
 *   -> Spooler            - general steering tasks ( + common interface )
 *   -> FileProcessor      - chunking, compression and hashing of files
 *   -> AbstractUploader   - abstract base class for uploading facilities
 *   -> concrete Uploaders - upload functionality for various backend storages
 *
 * Stage 1 aka. the processing of files is handled by the FileProcessor, since
 * it is independent from the actual uploading this functionality is outsourced.
 * The FileProcessor will take care of the above mentioned steps in a concurrent
 * fashion. This process is invoked by calling Spooler::Process().
 * While processing, the FileProcessor immediately schedules Upload jobs in
 * order to push data to the backend storage as early as possible.
 *
 * Stage 2 aka. the upload is handled by one of the concrete Uploader classes.
 * Uploaders have a thin interface described in the AbstractUploader class.
 * The file processor uses a concrete Uploader to push files into the backend
 * storage as part of it's processing.
 * Furthermore the user can directly upload files using the Spooler::Upload()
 * method as described in the next paragraph.
 *
 * For some specific files we need to be able to circumvent the FileProcessor to
 * directly push them into the backend storage (i.e. .cvmfspublished). For this
 * Spooler::Upload() is used. It directly schedules an upload job in the used
 * concrete Uploader facility. These files will not get compressed or check-
 * summed by any means.
 *
 * In any case, calling Spooler::Process() or Spooler::Upload() will invoke a
 * callback once the whole job has been finished. Callbacks are provided by the
 * Observable template. Please see the implementation of this template for more
 * details on usage and implementation.
 * The data structure provided by this callback is called SpoolerResult and con-
 * tains information about the processed file (status, content hash, chunks, ..)
 * Note: Even if a concrete Uploader internally spawns more than one upload job
 *       to send out chunked files, the user will only see a single invocation
 *       containing information about the uploaded file including it's generated
 *       chunks.
 *
 * Workflow:
 *
 *   User
 *   \O/                Callback (SpoolerResult)
 *    |   <----------------------+
 *   / \                         |
 *    |                          |
 *    | Process()                |          File
 *    +-----------> ################### ---------------------> #################
 *    | Upload()    #     Spooler     #                        # FileProcessor #
 *    +-----------> ################### <--------------------- #################
 *                   |               ^    FileProcessor::Results   |    ^
 *                   |               |                             |    |
 *     direct Upload |      Callback |                             |    |
 *                  `|´              |            Schedule Upload  |    |
 *                 ##################### <-------------------------+    |
 *                 #  Upload facility  #                                |
 *                 ##################### -------------------------------+
 *                           |             Callback (UploaderResults)
 *                    Upload |
 *                          `|´
 *                 *********************
 *                 *  Backend Storage  *
 *                 *********************
 *
 */

#ifndef CVMFS_UPLOAD_H_
#define CVMFS_UPLOAD_H_

#include <string>
#include <cstdio>
#include <vector>

#include "hash.h"
#include "file_chunk.h"

#include "upload_spooler_definition.h"
#include "upload_spooler_result.h"

#include "upload_file_processor.h"
#include "upload_facility.h"

namespace upload
{
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
   *
   * Note: Concrete implementations of AbstractSpooler are responsible to pro-
   *       duce a SpoolerResult once they finish a job and pass it upwards by
   *       invoking JobDone(). AbstractSpooler will then take care of notifying
   *       all registered listeners.
   */
  class Spooler : public Observable<SpoolerResult> {
   public:

   public:
    static Spooler* Construct(const SpoolerDefinition &spooler_definition);
    virtual ~Spooler();

    /**
     * Prints the name of the targeted backend storage.
     * Intended for debugging purposes only!
     */
    std::string backend_name() const;

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
    void Upload(const std::string &local_path,
                const std::string &remote_path);

    /**
     * Schedules a process job that compresses and hashes the provided file in
     * local_path and uploads it into the CAS backend. The remote path to the
     * file is determined by the content hash of the compressed file appended by
     * file_suffix.
     * When the processing has finish a callback will be invoked asynchronously.
     *
     * Note: This method might decide to chunk the file into a number of smaller
     *       parts and upload them separately. Still, you will receive a single
     *       callback for the whole job, that contains information about the
     *       generated chunks.
     *
     * @param local_path      the location of the file to be processed and up-
     *                        loaded into the backend storage
     * @param allow_chunking  (optional) controls if this file should be cut in
     *                        chunks or uploaded at once
     */
    void Process(const std::string &local_path,
                 const bool         allow_chunking = true);

    /**
     * Deletes the given file from the repository backend storage. This is done
     * synchronous, in any case.
     *
     * @param file_to_delete   path to the file to be deleted
     * @return                 true if file was successfully removed
     */
    bool Remove(const std::string &file_to_delete);

    /**
     * Checks if a file is already present in the backend storage
     *
     * @param path  the path of the file to be peeked
     * @return      true if the file was found in the backend storage
     */
    bool Peek(const std::string &path) const;

    /**
     * Blocks until all jobs currently under processing are finished. After it
     * returned, more jobs can be scheduled if needed.
     * Note: We assume that no one schedules new jobs while this method is in
     *       waiting state. Otherwise it might never return, since the job queue
     *       does not get empty.
     */
    void WaitForUpload() const;

    /**
     * Blocks until all jobs are processed and all worker threads terminated
     * successfully. Afterwards the spooler will be out of service.
     * Call this after you have called WaitForUpload() to wait until the
     * Spooler terminates.
     * Note: after calling this method NO JOBS should be scheduled anymore.
     */
    void WaitForTermination() const;

    /**
     * Checks how many of the already processed jobs have failed.
     *
     * @return   the number of failed jobs at the time this method is invoked
     */
    unsigned int GetNumberOfErrors() const;


   protected:
    /**
     * This method is called once before any other operations are performed on
     * a Spooler. Implements global initialization work.
     */
    bool Initialize();

    /**
     * This method is called right before the Spooler object will terminate.
     * Implement this to do global clean up work. You should not finish jobs
     * in this method, since it is meant to be called after the Spooler has
     * stopped its actual work or was terminated prematurely.
     */
    void TearDown();


   protected:
    /**
     * @param spooler_definition   the SpoolerDefinition structure that defines
     *                             some intrinsics of the concrete Spoolers.
     */
    Spooler(const SpoolerDefinition &spooler_definition);

    /**
     * Concrete implementations of the AbstractSpooler must call this method
     * when they finish an upload job. A single upload job might contain more
     * than one file to be uploaded (see Upload(FileProcessor::Results) ).
     *
     * Note: If the concrete spooler implements uploading as an asynchronous
     *       task, this method MUST be called when all items for one upload
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
     * Automatically takes care of processed files and prepares them for upload
     * by calling AbstractSpooler::Upload(FileProcessor::Results)
     */
    void ProcessingCallback(const SpoolerResult &data);

    void UploadingCallback(const UploaderResults &data);

    /*
     * @return   the spooler definition that was initially given to any Spooler
     *           constructor.
     */
    inline const SpoolerDefinition& spooler_definition() const {
      return spooler_definition_;
    }

   private:
    // Status Information
    const SpoolerDefinition      spooler_definition_;

    UniquePtr<FileProcessor>     file_processor_;
    UniquePtr<AbstractUploader>  uploader_;
  };

}

#endif /* CVMFS_UPLOAD_H_ */
