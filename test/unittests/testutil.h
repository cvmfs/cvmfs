#ifndef CVMFS_UNITTEST_TESTUTIL
#define CVMFS_UNITTEST_TESTUTIL

#include <sys/types.h>

#include "../../cvmfs/upload_facility.h"

#include "../../cvmfs/directory_entry.h"
#include "../../cvmfs/util.h"

pid_t GetParentPid(const pid_t pid);

namespace catalog {

class DirectoryEntryTestFactory {
 public:
  static catalog::DirectoryEntry RegularFile();
  static catalog::DirectoryEntry Directory();
  static catalog::DirectoryEntry Symlink();
  static catalog::DirectoryEntry ChunkedFile();
};

} /* namespace catalog */

class PolymorphicConstructionUnittestAdapter {
 public:
  template <class AbstractProductT, class ConcreteProductT>
  static void RegisterPlugin() {
    AbstractProductT::template RegisterPlugin<ConcreteProductT>();
  }

  template <class AbstractProductT>
  static void UnregisterAllPlugins() {
    AbstractProductT::UnregisterAllPlugins();
  }
};


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


static const std::string g_sandbox_path    = "/tmp/cvmfs_mockuploader";
static const std::string g_sandbox_tmp_dir = g_sandbox_path + "/tmp";
static upload::SpoolerDefinition MockSpoolerDefinition() {
  const size_t      min_chunk_size   = 512000;
  const size_t      avg_chunk_size   = 2 * min_chunk_size;
  const size_t      max_chunk_size   = 4 * min_chunk_size;

  return upload::SpoolerDefinition("mock," + g_sandbox_path + "," +
                                             g_sandbox_tmp_dir,
                                   shash::kSha1,
                                   true,
                                   min_chunk_size,
                                   avg_chunk_size,
                                   max_chunk_size);
}


/**
 * This is a simple base class for a mocked uploader. It implements only the
 * very common parts and takes care of the internal instrumentation of
 * PolymorphicConstruction.
 */
template <class DerivedT> // curiously recurring template pattern
class AbstractMockUploader : public upload::AbstractUploader {
 private:
  static const bool not_implemented = false;

 public:
  static const std::string sandbox_path;
  static const std::string sandbox_tmp_dir;

 public:
  AbstractMockUploader(const upload::SpoolerDefinition &spooler_definition) :
    AbstractUploader(spooler_definition),
    worker_thread_running(false) {}

  static DerivedT* MockConstruct() {
    PolymorphicConstructionUnittestAdapter::RegisterPlugin<
                                                      upload::AbstractUploader,
                                                      DerivedT>();
    DerivedT* result = dynamic_cast<DerivedT*>(
      AbstractUploader::Construct(MockSpoolerDefinition())
    );
    PolymorphicConstructionUnittestAdapter::UnregisterAllPlugins<
                                                    upload::AbstractUploader>();
    return result;
  }

  static bool WillHandle(const upload::SpoolerDefinition &spooler_definition) {
    return spooler_definition.driver_type == upload::SpoolerDefinition::Mock;
  }

  void WorkerThread() {
    worker_thread_running = true;

    bool running = true;
    while (running) {
      UploadJob job = AcquireNewJob();
      switch (job.type) {
        case UploadJob::Upload:
          Upload(job.stream_handle,
                 job.buffer,
                 job.callback);
          break;
        case UploadJob::Commit:
          FinalizeStreamedUpload(job.stream_handle,
                                 job.content_hash,
                                 job.hash_suffix);
          break;
        case UploadJob::Terminate:
          running = false;
          break;
        default:
          assert (AbstractMockUploader::not_implemented);
          break;
      }
    }

    worker_thread_running = false;
  }

  virtual void FileUpload(const std::string  &local_path,
                          const std::string  &remote_path,
                          const callback_t   *callback = NULL) {
    assert (AbstractMockUploader::not_implemented);
  }

  virtual upload::UploadStreamHandle* InitStreamedUpload(
                                            const callback_t *callback = NULL) {
    assert (AbstractMockUploader::not_implemented);
    return NULL;
  }

  virtual void Upload(upload::UploadStreamHandle  *handle,
                      upload::CharBuffer          *buffer,
                      const callback_t            *callback = NULL) {
    assert (AbstractMockUploader::not_implemented);
  }

  virtual void FinalizeStreamedUpload(upload::UploadStreamHandle *handle,
                                      const shash::Any            content_hash,
                                      const std::string           hash_suffix) {
    assert (AbstractMockUploader::not_implemented);
  }

  virtual bool Remove(const std::string &file_to_delete) {
    assert (AbstractMockUploader::not_implemented);
  }

  virtual bool Peek(const std::string &path) const {
    assert (AbstractMockUploader::not_implemented);
  }

  virtual unsigned int GetNumberOfErrors() const {
    assert (AbstractMockUploader::not_implemented);
  }

 public:
  volatile bool worker_thread_running;
};

template <class DerivedT>
const std::string AbstractMockUploader<DerivedT>::sandbox_path    = g_sandbox_path;
template <class DerivedT>
const std::string AbstractMockUploader<DerivedT>::sandbox_tmp_dir = g_sandbox_tmp_dir;

#endif /* CVMFS_UNITTEST_TESTUTIL */
