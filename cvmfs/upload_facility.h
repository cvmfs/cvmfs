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


class AbstractUploader : public PolymorphicConstruction<AbstractUploader,
                                                        SpoolerDefinition>,
                         public Callbackable<UploaderResults> {
 protected:
  typedef Callbackable<UploaderResults>::callback_t* callback_ptr;

 public:
  virtual bool Initialize();
  virtual void TearDown();

  virtual void Upload(const std::string  &local_path,
                      const std::string  &remote_path,
                      const callback_t   *callback = NULL) = 0;
  virtual void Upload(const std::string  &local_path,
                      const hash::Any    &content_hash,
                      const std::string  &hash_suffix,
                      const callback_t   *callback = NULL) = 0;

  virtual void WaitForUpload() const;
  virtual unsigned int GetNumberOfErrors() const = 0;

  static void RegisterPlugins();


 protected:
  AbstractUploader(const SpoolerDefinition& spooler_definition);

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
