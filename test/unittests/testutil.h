#ifndef CVMFS_UNITTEST_TESTUTIL
#define CVMFS_UNITTEST_TESTUTIL

#include <sys/types.h>

#include "../../cvmfs/upload_facility.h"
#include "../../cvmfs/hash.h"
#include "../../cvmfs/directory_entry.h"
#include "../../cvmfs/util.h"
#include "../../cvmfs/history.h"

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
 protected:
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
                                      const shash::Suffix         hash_suffix) {
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


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


namespace manifest {
  class Manifest;
}

namespace swissknife {
  class CatalogTraversalParams;
}


/**
 * This is a minimal mock of a Catalog class.
 */
class MockCatalog {
 public:
  typedef std::map<shash::Any, MockCatalog*> AvailableCatalogs;
  static AvailableCatalogs available_catalogs;
  static unsigned int      instances;

  static const std::string rhs;
  static const shash::Any  root_hash;

  static void Reset();
  static void RegisterCatalog(MockCatalog *catalog);
  static void UnregisterCatalogs();
  static MockCatalog* GetCatalog(const shash::Any &catalog_hash);

 public:
  struct NestedCatalog {
    PathString   path;
    shash::Any   hash;
    MockCatalog *child;
    uint64_t     size;
  };
  typedef std::vector<NestedCatalog> NestedCatalogList;

  struct File {
    shash::Any  hash;
    size_t      size;
  };
  typedef std::vector<File> FileList;

  struct Chunk {
    shash::Any  hash;
    size_t      size;
  };
  typedef std::vector<Chunk> ChunkList;

  typedef std::vector<shash::Any> HashVector;

 public:
  MockCatalog(const std::string &root_path,
              const shash::Any  &catalog_hash,
              const uint64_t     catalog_size,
              const unsigned int revision,
              const bool         is_root,
              MockCatalog *parent   = NULL,
              MockCatalog *previous = NULL) :
    parent_(parent), previous_(previous), root_path_(root_path),
    catalog_hash_(catalog_hash), catalog_size_(catalog_size),
    revision_(revision), is_root_(is_root)
  {
    if (parent != NULL) {
      parent->RegisterChild(this);
    }
    ++MockCatalog::instances;
  }

  MockCatalog(const MockCatalog &other) :
    parent_(other.parent_), previous_(other.previous_),
    root_path_(other.root_path_), catalog_hash_(other.catalog_hash_),
    catalog_size_(other.catalog_size_), revision_(other.revision_),
    is_root_(other.is_root_), children_(other.children_), files_(other.files_),
    chunks_(other.chunks_)
  {
    ++MockCatalog::instances;
  }

  ~MockCatalog() {
    --MockCatalog::instances;
  }

 public: /* API in this 'public block' is used by CatalogTraversal
          * (see catalog.h - catalog::Catalog for details)
          */
  static MockCatalog* AttachFreely(const std::string  &root_path,
                                   const std::string  &file,
                                   const shash::Any   &catalog_hash,
                                         MockCatalog  *parent = NULL);

  bool IsRoot() const { return is_root_; }

  const NestedCatalogList& ListNestedCatalogs() const { return children_; }
  const HashVector&        GetReferencedObjects() const;

  unsigned int GetRevision() const { return revision_; }

  shash::Any GetPreviousRevision() const {
    return (previous_ != NULL) ? previous_->catalog_hash() : shash::Any();
  }

 public:
  const PathString   path()         const { return PathString(root_path_);  }
  const std::string& root_path()    const { return root_path_;              }
  const shash::Any&  catalog_hash() const { return catalog_hash_;           }
  uint64_t           catalog_size() const { return catalog_size_;           }
  unsigned int       revision()     const { return revision_;               }

  MockCatalog*       parent()       const { return parent_;                 }
  MockCatalog*       previous()     const { return previous_;               }

  void set_parent(MockCatalog *parent) { parent_ = parent; }

 public:
  void RegisterChild(MockCatalog *child);
  void AddFile(const shash::Any &content_hash, const size_t file_size);
  void AddChunk(const shash::Any &chunk_content_hash, const size_t chunk_size);

 protected:
  MockCatalog* Clone() const {
    return new MockCatalog(*this);
  }

 private:
  MockCatalog        *parent_;
  MockCatalog        *previous_;
  const std::string   root_path_;
  const shash::Any    catalog_hash_;
  const uint64_t      catalog_size_;
  const unsigned int  revision_;
  const bool          is_root_;

  NestedCatalogList   children_;
  FileList            files_;
  ChunkList           chunks_;

  mutable HashVector  referenced_objects_;
};



/**
 * This is a mock of an ObjectFetcher that does essentially nothing.
 */
class MockObjectFetcher {
 public:
  static UniquePtr<history::History>  *s_history;
  static std::set<shash::Any>         *deleted_catalogs;

 public:
  MockObjectFetcher(const swissknife::CatalogTraversalParams &params) {}
 public:
  manifest::Manifest* FetchManifest();
  history::History* FetchHistory() {
    return (MockObjectFetcher::s_history != NULL)
              ? s_history->Release()
              : NULL;
  }
  inline bool Fetch(const shash::Any  &catalog_hash,
                    std::string       *catalog_file) {
    return (deleted_catalogs == NULL ||
            deleted_catalogs->find(catalog_hash) == deleted_catalogs->end());
  }
  inline bool Exists(const std::string &file) {
    return false;
  }
};

#endif /* CVMFS_UNITTEST_TESTUTIL */
