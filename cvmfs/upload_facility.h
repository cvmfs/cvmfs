/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_FACILITY_
#define CVMFS_UPLOAD_FACILITY_

#include "util.h"
#include "util_concurrency.h"

#include "upload_spooler_definition.h"

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

  UploaderResults(const int return_code) :
    type(kChunkCommit),
    return_code(return_code),
    local_path(""),
    buffer(NULL) {}

  const Type         type;
  const int          return_code;
  const std::string  local_path;
  CharBuffer        *buffer;
};


struct UploadStreamHandle {
  typedef CallbackBase<UploaderResults> callback_t;

  UploadStreamHandle(const callback_t *commit_callback) :
    commit_callback(commit_callback) {}

  const callback_t *commit_callback;
};


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
  typedef Callbackable<UploaderResults>::callback_t* callback_ptr;

 public:
  virtual ~AbstractUploader() {};

  virtual bool Initialize();
  virtual void TearDown();

  /**
   * Uploads the file at the path local_path into the backend storage under the
   * path remote_path. When the upload has finished it calls callback.
   *
   * @param local_path   path to the file to be uploaded
   * @param remote_path  desired path for the file in the backend storage
   * @param callback     (optional) gets notified when the upload was finished
   */
  virtual void Upload(const std::string  &local_path,
                      const std::string  &remote_path,
                      const callback_t   *callback = NULL) = 0;

  /**
   *
   */
  virtual UploadStreamHandle* InitStreamedUpload(
                                       const callback_t   *callback = NULL) = 0;

  /**
   *
   */
  virtual void Upload(UploadStreamHandle  *handle,
                      CharBuffer          *buffer,
                      const callback_t    *callback = NULL) = 0;

  /**
   *
   */
   virtual void FinalizeStreamedUpload(UploadStreamHandle *handle,
                                       const shash::Any    content_hash,
                                       const std::string   hash_suffix) = 0;

  /**
   * Removes a file from the backend storage. This is done synchronously in any
   * case.
   *
   * Note: This method is currently used very sparsely! If this changes in the
   *       future, one might think about doing deletion asynchronously!
   *
   * @param file_to_delete  path to the file to be removed
   * @return                true if the file was successfully removed
   */
  virtual bool Remove(const std::string &file_to_delete) = 0;

  /**
   * Checks if a file is already present in the backend storage
   *
   * @param path  the path of the file to be checked
   * @return      true if the file was found in the backend storage
   */
  virtual bool Peek(const std::string &path) const = 0;

  /**
   * Waits until the current upload queue is empty.
   *
   * Note: This does NOT necessarily mean, that all files are actuall uploaded.
   *       If new jobs are concurrently scheduled the behavior of this method is
   *       not defined (it returns also on intermediate empty queues)
   */
  virtual void WaitForUpload() const;
  virtual unsigned int GetNumberOfErrors() const = 0;

  /**
   * Disables all precaching behavior in order to finish upload jobs as fast as
   * possible to avoid deadlocks with user-code that waits for files to be up-
   * loaded.
   *
   * Note: precaching is supposed to be activated by default
   */
  virtual void DisablePrecaching();

  /**
   * Re-enables precaching behavior.
   */
  virtual void EnablePrecaching();

  static void RegisterPlugins();


 protected:
  AbstractUploader(const SpoolerDefinition& spooler_definition);

  /**
   * This notifies the callback that is associated to a finishing job. Please
   * do not call the handed callback yourself in concrete Uploaders!
   *
   * Note: Since the job is finished after we respond to it, the callback object
   *       gets automatically destroyed by this call!
   *       Therefore you must not call Respond() twice or use the callback later
   *       by any means!
   */
  void Respond(const callback_t       *callback,
               const UploaderResults  &result) const;

  const SpoolerDefinition& spooler_definition() const {
    return spooler_definition_;
  }

 private:
  const SpoolerDefinition spooler_definition_;
};

}

#endif /* CVMFS_UPLOAD_FACILITY_ */
