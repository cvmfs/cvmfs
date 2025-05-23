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
 *      -> pluggable to support different upload paths (local, S3, ...)
 *
 * There are a number of different entities involved in this process. Namely:
 *   -> Spooler            - general steering tasks ( + common interface )
 *   -> IngestionPipeline  - chunking, compression and hashing of files
 *   -> AbstractUploader   - abstract base class for uploading facilities
 *   -> concrete Uploaders - upload functionality for various backend storages
 *
 * Stage 1 aka. the processing of files is handled by the IngestionPipeline,
 * since it is independent from the actual uploading this functionality is
 * outsourced. The IngestionPipeline will take care of the above mentioned steps
 * in a concurrent fashion. This process is invoked by calling
 * Spooler::Process(). While processing, the IngestionPipeline immediately
 * schedules Upload jobs in order to push data to the backend storage as early
 *  as possible.
 *
 * Stage 2 aka. the upload is handled by one of the concrete Uploader classes.
 * Uploaders have a thin interface described in the AbstractUploader class.
 * The IngestionPipeline uses a concrete Uploader to push files into the backend
 * storage as part of it's processing.
 * Furthermore the user can directly upload files using the Spooler::Upload()
 * method as described in the next paragraph.
 *
 * For some specific files we need to be able to circumvent the
 * IngestionPipeline to directly push them into the backend storage (i.e.
 * .cvmfspublished). For this Spooler::Upload() is used. It directly schedules
 * an upload job in the used concrete Uploader facility. These files will not
 * get compressed or check-summed by any means.
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
 *    +-----------> ################### -----------------> #####################
 *    | Upload()    #     Spooler     #                    # IngestionPipeline #
 *    +-----------> ################### <----------------- #####################
 *                   |               ^      SpoolerResult          |    ^
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
 *
 * TODO(rmeusel): special purpose ::Process...() methods should (optionally?)
 *                return Future<> instead of relying on the callbacks. Those are
 *                somewhat one-shot calls and require a rather fishy idiom when
 *                using callbacks, like:
 *
 *                   cb = spooler->RegisterListener(...certificate_callback...);
 *                   spooler->ProcessCertificate(...);
 *                   spooler->WaitForUpload();
 *                   spooler->UnregisterListener(cb);
 *                   cert_hash = global_cert_hash;
 *
 *                   void certificate_callback(shash::Any &hash) {
 *                     global_cert_hash = hash;
 *                   }
 *
 *                If ProcessCertificate(), ProcessHistory(), ProcessMetainfo(),
 *                UploadManifest() and UploaderReflog() would return Future<>,
 *                the code would be much more comprehensible and free of user-
 *                managed global state:
 *
 *                   Future<shash::Any> fc = spooler->ProcessCertificate(...);
 *                   cert_hash = fc.get();
 *
 */

#ifndef CVMFS_UPLOAD_H_
#define CVMFS_UPLOAD_H_

#include <cstdio>
#include <string>
#include <vector>

#include "crypto/hash.h"
#include "file_chunk.h"
#include "ingestion/ingestion_source.h"
#include "ingestion/pipeline.h"
#include "repository_tag.h"
#include "upload_facility.h"
#include "upload_spooler_definition.h"
#include "upload_spooler_result.h"
#include "util/concurrency.h"
#include "util/pointer.h"
#include "util/shared_ptr.h"

namespace upload {

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
 */
class Spooler : public Observable<SpoolerResult> {
 public:
  static Spooler *Construct(const SpoolerDefinition &spooler_definition,
                            perf::StatisticsTemplate *statistics = NULL);
  virtual ~Spooler();

  /**
   * Prints the name of the targeted backend storage.
   * Intended for debugging purposes only!
   */
  std::string backend_name() const;

  /**
   * Calls the concrete uploader to create a new repository area
   */
  bool Create();

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
  void Upload(const std::string &local_path, const std::string &remote_path);

  /**
   * Ownership of source is transferred to the spooler
   */
  void Upload(const std::string &remote_path, IngestionSource *source);

  /**
   * Convenience wrapper to upload the Manifest file into the backend storage
   *
   * @param local_path  the location of the (signed) manifest to be uploaded
   */
  void UploadManifest(const std::string &local_path);

  /**
   * Convenience wrapper to upload a Reflog database into the backend storage
   *
   * @param local_path  the SQLite file location of the Reflog to be uploaded
   */
  void UploadReflog(const std::string &local_path);

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
   * @param source          the ingestion source of the file to be processed
   *                        and uploaded into the backend storage
   * @param allow_chunking  (optional) controls if this file should be cut in
   *                        chunks or uploaded at once
   */
  void Process(IngestionSource *source, const bool allow_chunking = true);

  /**
   * Convenience wrapper to process a catalog file. Please always use this
   * for catalog processing. It will add special flags and hash suffixes
   *
   * @param local_path  the location of the catalog file to be processed
   */
  void ProcessCatalog(const std::string &local_path);

  /**
   * Convenience wrapper to process a history database file. This sets the
   * processing parameters (like chunking and hash suffixes) accordingly.
   *
   * @param local_path  the location of the history database file
   */
  void ProcessHistory(const std::string &local_path);

  /**
   * Convenience wrapper to process a certificate file. This sets the
   * processing parameters (like chunking and hash suffixes) accordingly.
   *
   * @param local_path  the location of the source of the certificate file
   */
  void ProcessCertificate(const std::string &local_path);
  /**
   * Ownership of source is transferred to the ingestion pipeline
   */
  void ProcessCertificate(IngestionSource *source);

  /**
   * Convenience wrapper to process a meta info file.
   *
   * @param local_path  the location of the meta info file
   */
  void ProcessMetainfo(const std::string &local_path);
  /**
   * Ownership of source is transferred to the ingestion pipeline
   */
  void ProcessMetainfo(IngestionSource *source);

  /**
   * Deletes the given file from the repository backend storage.  This requires
   * using WaitForUpload() to make sure the delete operations reached the
   * upload backend.
   *
   * @param file_to_delete   path to the file to be deleted
   * @return                 true if file was successfully removed
   */
  void RemoveAsync(const std::string &file_to_delete);

  /**
   * Checks if a file is already present in the backend storage
   *
   * @param path  the path of the file to be peeked
   * @return      true if the file was found in the backend storage
   */
  bool Peek(const std::string &path) const;

  /**
   * Make directory in upstream storage. Noop if directory already present.
   * NOTE: currently only used to create the 'stats/' subdirectory
   *
   * @param path relative directory path in the upstream storage
   * @return true if the directory was successfully created or already present
   */
  bool Mkdir(const std::string &path);

  /**
   * Creates a top-level shortcut to the given data object. This is particularly
   * useful for bootstrapping repositories whose data-directory is secured by
   * a VOMS certificate.
   *
   * @param object  content hash of the object to be exposed on the top-level
   * @return        true on success
   */
  bool PlaceBootstrappingShortcut(const shash::Any &object) const;

  /**
   * Blocks until all jobs currently under processing are finished. After it
   * returned, more jobs can be scheduled if needed.
   * Note: We assume that no one schedules new jobs while this method is in
   *       waiting state. Otherwise it might never return, since the job queue
   *       does not get empty.
   */
  void WaitForUpload() const;

  bool FinalizeSession(bool commit, const std::string &old_root_hash = "",
                       const std::string &new_root_hash = "",
                       const RepositoryTag &tag = RepositoryTag()) const;

  /**
   * Checks how many of the already processed jobs have failed.
   *
   * @return   the number of failed jobs at the time this method is invoked
   */
  unsigned int GetNumberOfErrors() const;

  shash::Algorithms GetHashAlgorithm() const {
    return spooler_definition_.hash_algorithm;
  }

  SpoolerDefinition::DriverType GetDriverType() const {
    return spooler_definition_.driver_type;
  }

 protected:
  /**
   * This method is called once before any other operations are performed on
   * a Spooler. Implements global initialization work.
   */
  bool Initialize(perf::StatisticsTemplate *statistics);

  /**
   * @param spooler_definition   the SpoolerDefinition structure that defines
   *                             some intrinsics of the concrete Spoolers.
   */
  explicit Spooler(const SpoolerDefinition &spooler_definition);

  /**
   * Used internally: Is called when ingestion pipeline finishes a job.
   * Automatically takes care of processed files and prepares them for upload.
   */
  void ProcessingCallback(const SpoolerResult &data);

  void UploadingCallback(const UploaderResults &data);

  /*
   * @return   the spooler definition that was initially given to any Spooler
   *           constructor.
   */
  inline const SpoolerDefinition &spooler_definition() const {
    return spooler_definition_;
  }

 private:
  // Status Information
  const SpoolerDefinition spooler_definition_;

  UniquePtr<IngestionPipeline> ingestion_pipeline_;
  UniquePtr<AbstractUploader> uploader_;
};

}  // namespace upload

#endif  // CVMFS_UPLOAD_H_
