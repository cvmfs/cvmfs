/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_FACILITY_
#define CVMFS_UPLOAD_FACILITY_

#include "util.h"
#include "util_concurrency.h"

#include "upload_spooler_definition.h"

namespace upload {

struct UploaderResults {
  UploaderResults(const int return_code, const std::string &local_path) :
    return_code(return_code),
    local_path(local_path) {}

  const int         return_code;
  const std::string local_path;
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
   * Uploads the file at the path local_path into the backend storage and saves
   * it under the provided content hash. Additionally the content hash can be
   * extended by a hash suffix.
   *
   * @param local_path    path to the file to be uploaded
   * @param content_hash  content hash to be used as path in the backend storage
   * @param hash_suffix   string to append behind the content hash
   * @param callback      (optional) gets notified when the upload was finished
   */
  virtual void Upload(const std::string  &local_path,
                      const hash::Any    &content_hash,
                      const std::string  &hash_suffix,
                      const callback_t   *callback = NULL) = 0;

  virtual void WaitForUpload() const;
  virtual unsigned int GetNumberOfErrors() const = 0;

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
  void Respond(const callback_t *callback,
               const int return_code,
               const std::string local_path);

  const SpoolerDefinition& spooler_definition() const {
    return spooler_definition_;
  }

 private:
  const SpoolerDefinition spooler_definition_;
};

}

#endif /* CVMFS_UPLOAD_FACILITY_ */
