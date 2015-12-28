/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_FACILITY_H_
#define CVMFS_UPLOAD_FACILITY_H_

#include <tbb/concurrent_queue.h>
#include <tbb/tbb_thread.h>

#include <string>

#include "upload_spooler_definition.h"
#include "util.h"
#include "util_concurrency.h"

namespace upload {

class CharBuffer;

struct UploaderResults {
  enum Type {
    kFileUpload,
    kBufferUpload,
    kChunkCommit
  };

  UploaderResults(const int return_code, const std::string &local_path) :
    type(kFileUpload),
    return_code(return_code),
    local_path(local_path),
    buffer(NULL) {}

  UploaderResults(const int return_code, CharBuffer *buffer) :
    type(kBufferUpload),
    return_code(return_code),
    local_path(""),
    buffer(buffer) {}

  explicit UploaderResults(const int return_code) :
    type(kChunkCommit),
    return_code(return_code),
    local_path(""),
    buffer(NULL) {}

  const Type         type;
  const int          return_code;
  const std::string  local_path;
  CharBuffer        *buffer;
};


struct UploadStreamHandle;


/**
 * Abstract base class for all backend upload facilities
 * This class defines an interface and constructs the concrete Uploaders,
 * futhermore it handles callbacks to the outside world to notify users of done
 * upload jobs.
 *
 * Note: Users could be both the Spooler (when calling Spooler::Upload()) and
 *       the FileProcessor (when calling Spooler::Process()). We therefore
 *       cannot use the Observable template here, since this would forward
 *       finished upload jobs to ALL listeners instead of only the owner of the
 *       specific job.
 */
class AbstractUploader : public PolymorphicConstruction<AbstractUploader,
                                                        SpoolerDefinition>,
                         public Callbackable<UploaderResults> {
 protected:
  typedef Callbackable<UploaderResults>::CallbackTN* CallbackPtr;

  struct UploadJob {
    enum Type {
      Upload,
      Commit,
      Terminate
    };

    UploadJob(UploadStreamHandle  *handle,
              CharBuffer          *buffer,
              const CallbackTN    *callback = NULL) :
      type(Upload), stream_handle(handle), buffer(buffer), callback(callback) {}

    UploadJob(UploadStreamHandle  *handle,
              const shash::Any    &content_hash) :
      type(Commit), stream_handle(handle), buffer(NULL), callback(NULL),
      content_hash(content_hash) {}

    UploadJob() :
      type(Terminate), stream_handle(NULL), buffer(NULL), callback(NULL) {}

    Type                 type;
    UploadStreamHandle  *stream_handle;

    // type=Upload specific fields
    CharBuffer          *buffer;
    const CallbackTN    *callback;

    // type=Commit specific fields
    shash::Any           content_hash;
  };

 public:
  virtual ~AbstractUploader() {
    assert(torn_down_ && "Call AbstractUploader::TearDown() before dtor!");
  }

  /**
   * This is called right after the constructor of AbstractUploader or/and its
   * derived class has been executed. You can override that to do additional
   * initialization that cannot be done in the constructor itself.
   *
   * @return   true on successful initialization
   */
  virtual bool Initialize();

  /**
   * This must be called right before the destruction of the AbstractUploader!
   * You are _not_ supposed to overwrite this method in your concrete Uploader.
   * Please do all your cleanup work in your destructor, but keep in mind that
   * your WorkerThread() will already be terminated by this TearDown() method.
   */
  void TearDown();

  /**
   * Uploads the file at the path local_path into the backend storage under the
   * path remote_path. When the upload has finished it calls callback.
   * Note: This method might be implemented in a synchronous way.
   *
   * @param local_path   path to the file to be uploaded
   * @param remote_path  desired path for the file in the backend storage
   * @param callback     (optional) gets notified when the upload was finished
   */
  void Upload(const std::string  &local_path,
              const std::string  &remote_path,
              const CallbackTN   *callback = NULL) {
    ++jobs_in_flight_;
    FileUpload(local_path, remote_path, callback);
  }


  /**
   * This method is called before the first data Block of a streamed upload is
   * scheduled (see above implementation of UploadStreamHandle for details).
   *
   * Note: This method is called in the context of a TBB worker thread and must
   *       both be thread safe and non-blocking!
   *
   * @param callback   (optional) this callback will be invoked once this parti-
   *                   cular streamed upload is committed.
   * @return           a pointer to the initialized UploadStreamHandle
   */
  virtual UploadStreamHandle* InitStreamedUpload(
                                       const CallbackTN   *callback = NULL) = 0;


  /**
   * This method schedules a CharBuffer to be uploaded in the context of the
   * given UploadStreamHandle. The actual upload will happen asynchronously by
   * a concrete implementation of AbstractUploader. As soon has the scheduled
   * upload job is complete (either successful or not) the optionally passed
   * callback is supposed to be invoked using AbstractUploader::Respond().
   *
   * Note: This method is called in the context of a TBB worker thread it is
   *       supposed to be non-blocking!
   *
   * @param handle    Pointer to a previously acquired UploadStreamHandle
   * @param buffer    CharBuffer containing the data Block to be uploaded
   * @param callback  (optional) callback object to be invoked once the given
   *                  upload is finished (see AbstractUploader::Respond())
   */
  void ScheduleUpload(UploadStreamHandle  *handle,
                      CharBuffer          *buffer,
                      const CallbackTN    *callback = NULL) {
    ++jobs_in_flight_;
    upload_queue_.push(UploadJob(handle, buffer, callback));
  }


  /**
   * This method schedules a commit job as soon as all data Blocks of a streamed
   * upload are (successfully) uploaded. The concrete implementation of Abstract
   * Uploader is supposed to clean up the streamed upload.
   *
   * Note: This method is called in the context of a TBB worker thread it is
   *       supposed to be non-blocking!
   *
   * @param handle        Pointer to a previously acquired UploadStreamHandle
   * @param content_hash  the content hash of the full uploaded data Chunk
   */
  void ScheduleCommit(UploadStreamHandle   *handle,
                      const shash::Any     &content_hash) {
    ++jobs_in_flight_;
    upload_queue_.push(UploadJob(handle, content_hash));
  }


  /**
   * Removes a file from the backend storage. This might be done synchronously.
   *
   * Note: If the file doesn't exist before calling this method it will report
   *       a successful deletion anyways.
   *
   * Note: This method is currently used very sparsely! If this changes in the
   *       future, one might think about doing deletion asynchronously!
   *
   * @param file_to_delete  path to the file to be removed
   * @return                true if the file does not exist (anymore), false if
   *                        the removal failed
   */
  virtual bool Remove(const std::string &file_to_delete) = 0;


  /**
   * Overloaded Remove method used to remove a object based on its content hash.
   *
   * @param hash_to_delete  the content hash of a file to be deleted
   * @return                true on successful removal (removing a non-existant
   *                        object is a successful deletion as well!)
   */
  virtual bool Remove(const shash::Any &hash_to_delete) {
    return Remove("data/" + hash_to_delete.MakePath());
  }


  /**
   * Checks if a file is already present in the backend storage. This might be a
   * synchronous operation.
   *
   * @param path  the path of the file to be checked
   * @return      true if the file was found in the backend storage
   */
  virtual bool Peek(const std::string &path) const = 0;


  /**
   * Creates a top-level shortcut to the given data object. This is particularly
   * useful for bootstrapping repositories whose data-directory is secured by
   * a VOMS certificate.
   *
   * @param object  content hash of the object to be exposed on the top-level
   * @return        true on success
   */
  virtual bool PlaceBootstrappingShortcut(const shash::Any &object) const = 0;


  /**
   * Waits until the current upload queue is empty.
   *
   * Note: This does NOT necessarily mean, that all files are actuall uploaded.
   *       If new jobs are concurrently scheduled the behavior of this method is
   *       not defined (it returns also on intermediate empty queues)
   */
  virtual void WaitForUpload() const;

  virtual unsigned int GetNumberOfErrors() const = 0;
  static void RegisterPlugins();


 protected:
  explicit AbstractUploader(const SpoolerDefinition& spooler_definition);

  virtual void FileUpload(const std::string  &local_path,
                          const std::string  &remote_path,
                          const CallbackTN   *callback = NULL) = 0;

  /**
   * This notifies the callback that is associated to a finishing job. Please
   * do not call the handed callback yourself in concrete Uploaders!
   *
   * Note: Since the job is finished after we respond to it, the callback object
   *       gets automatically destroyed by this call!
   *       Therefore you must not call Respond() twice or use the callback later
   *       by any means!
   */
  void Respond(const CallbackTN       *callback,
               const UploaderResults  &result) const {
    if (callback != NULL) {
      (*callback)(result);
      delete callback;
    }

    --jobs_in_flight_;
  }


  /**
   * Acquires a job from the job queue.
   *
   * Note: if there are no jobs in the queue, it will block the calling thread
   *       until new work is available in the job queue.
   *       Consider to use TryToAcquireNewJob() if you do not want to block!
   *
   * @return   an UploadJob to be processed by the concrete implementation of
   *           AbstractUploader
   */
  UploadJob AcquireNewJob() {
    UploadJob job;
    upload_queue_.pop(job);
    return job;
  }


  /**
   * Tries to acquires a job from the job queue. If there is no work in the job
   * queue, it will _not_ wait but immediately return with 'false'.
   *
   * Note: This method will not block on an empty queue (see AcquireNewJob())!
   *
   * @param job_slot   a reference for an UploadJob slot to be pulled
   * @return           true if a job was successfully popped
   */
  bool TryToAcquireNewJob(UploadJob *job_slot) {
    return upload_queue_.try_pop(*job_slot);
  }


  /**
   * This purely virtual function is called once the AbstractUploader has started
   * its dedicated writer thread. Overwrite this method to implement your event
   * loop that is supposed to run in its own thread. In this event loop you are
   * supposed to pull upload jobs using AbstractUploader::AcquireNewJob().
   *
   * If this method returns, AbstractUploader's write thread is terminated, so
   * make sure to exit that method if and only if you receive an UploadJob like:
   *   UploadJob.type == UploadJob::Terminate
   */
  virtual void WorkerThread() = 0;


  /**
   * This is the entry point into the worker thread.
   */
  void WriteThread() {
    thread_started_executing_.Set(true);
    this->WorkerThread();
  }

  const SpoolerDefinition& spooler_definition() const {
    return spooler_definition_;
  }

  const SynchronizingCounter<int32_t>& jobs_in_flight() const {
    return jobs_in_flight_;
  }

 private:
  const SpoolerDefinition                   spooler_definition_;
  tbb::concurrent_bounded_queue<UploadJob>  upload_queue_;
  tbb::tbb_thread                           writer_thread_;
  bool                                      torn_down_;

  mutable SynchronizingCounter<int32_t>     jobs_in_flight_;
  Future<bool>                              thread_started_executing_;
};



/**
 * Each implementation of AbstractUploader must provide its own derivate of the
 * UploadStreamHandle that is supposed to contain state information for the
 * streamed upload of one specific chunk.
 * Each UploadStreamHandle contains a callback object that is invoked as soon as
 * the streamed upload is committed.
 */
struct UploadStreamHandle {
  typedef AbstractUploader::CallbackTN CallbackTN;

  explicit UploadStreamHandle(const CallbackTN *commit_callback) :
    commit_callback(commit_callback) {}
  virtual ~UploadStreamHandle() {}

  const CallbackTN *commit_callback;
};

}  // namespace upload

#endif  // CVMFS_UPLOAD_FACILITY_H_
