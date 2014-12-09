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
              const time_t       last_modified,
              const bool         is_root,
              MockCatalog *parent   = NULL,
              MockCatalog *previous = NULL) :
    parent_(parent), previous_(previous), root_path_(root_path),
    catalog_hash_(catalog_hash), catalog_size_(catalog_size),
    revision_(revision), last_modified_(last_modified), is_root_(is_root)
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
    last_modified_(other.last_modified_), is_root_(other.is_root_),
    children_(other.children_), files_(other.files_), chunks_(other.chunks_)
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
                                         MockCatalog  *parent      = NULL,
                                   const bool          is_not_root = false);

  bool IsRoot() const { return is_root_; }

  const NestedCatalogList& ListNestedCatalogs() const { return children_; }
  const HashVector&        GetReferencedObjects() const;

  unsigned int GetRevision()     const { return revision_;      }
  uint64_t     GetLastModified() const { return last_modified_; }

  shash::Any GetPreviousRevision() const {
    return (previous_ != NULL) ? previous_->hash() : shash::Any();
  }

 public:
  const PathString   path()         const { return PathString(root_path_);  }
  const std::string& root_path()    const { return root_path_;              }
  const shash::Any&  hash()         const { return catalog_hash_;           }
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
  const time_t        last_modified_;
  const bool          is_root_;

  NestedCatalogList   children_;
  FileList            files_;
  ChunkList           chunks_;

  mutable HashVector  referenced_objects_;
};


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


class MockHistory : public history::History {
 public:
  typedef std::map<std::string, history::History::Tag> TagMap;
  typedef std::set<shash::Any>                         HashSet;

 public:
  // TODO: count number of instances
  MockHistory(const bool          writable,
              const std::string  &fqrn);
  MockHistory(const MockHistory &other);
  ~MockHistory() {}

  history::History* Clone(const bool writable = false) const;

  bool IsWritable()        const { return writable_;    }
  int GetNumberOfTags()    const { return tags_.size(); }
  bool BeginTransaction()  const { return true;         }
  bool CommitTransaction() const { return true;         }

  bool SetPreviousRevision(const shash::Any &history_hash) { return true; }

  bool Insert(const Tag &tag);
  bool Remove(const std::string &name);
  bool Exists(const std::string &name) const;
  bool GetByName(const std::string &name, Tag *tag) const;
  bool GetByDate(const time_t timestamp, Tag *tag) const;
  bool List(std::vector<Tag> *tags) const;
  bool Tips(std::vector<Tag> *channel_tips) const;

  bool ListRecycleBin(std::vector<shash::Any> *hashes) const;
  bool EmptyRecycleBin();

  bool Rollback(const Tag &updated_target_tag);
  bool ListTagsAffectedByRollback(const std::string  &target_tag_name,
                                  std::vector<Tag>   *tags) const;

  bool GetHashes(std::vector<shash::Any> *hashes) const;

 public:
  void set_writable(const bool writable) { writable_ = writable; }

 protected:
  void GetTags(std::vector<Tag> *tags) const;

 private:
  static const Tag& get_tag(const TagMap::value_type &itr) {
    return itr.second;
  }

  static const shash::Any& get_hash(const Tag &tag) {
    return tag.root_hash;
  }

  static bool gt_channel_revision(const Tag &lhs, const Tag &rhs) {
    return (lhs.channel == rhs.channel)
              ? lhs.revision > rhs.revision
              : lhs.channel > rhs.channel;
  }

  static bool eq_channel(const Tag &lhs, const Tag &rhs) {
    return lhs.channel == rhs.channel;
  }

  static bool gt_hashes(const Tag &lhs, const Tag &rhs) {
    return lhs.root_hash > rhs.root_hash;
  }

  static bool eq_hashes(const Tag &lhs, const Tag &rhs) {
    return lhs.root_hash == rhs.root_hash;
  }

  struct DateSmallerThan {
    DateSmallerThan(time_t date) : date_(date) {}
    bool operator()(const Tag &tag) const {
      return tag.timestamp <= date_;
    }
    const time_t date_;
  };

  struct RollbackPredicate {
    RollbackPredicate(const Tag &tag, const bool inverse = false) :
      tag_(tag),
      inverse_(inverse) {}
    bool operator()(const Tag &tag) const {
      const bool p = (tag.revision > tag_.revision || tag.name == tag_.name) &&
                      tag.channel == tag_.channel;
      return inverse_ ^ p;
    }
    const Tag  &tag_;
    const bool  inverse_;
  };

  struct TagRemover {
    TagRemover(MockHistory *history) : history_(history) {}
    bool operator()(const Tag &tag) {
      return history_->Remove(tag.name);
    }
    MockHistory *history_;
  };

 private:
  TagMap  tags_;
  HashSet recycle_bin_;
  bool    writable_;
};


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


/**
 * This is a mock of an ObjectFetcher that does essentially nothing.
 */
class MockObjectFetcher {
 public:
  static MockHistory          *s_history;
  static std::set<shash::Any> *s_deleted_catalogs;
  static bool                  history_available;

  static void Reset() {
    if (MockObjectFetcher::s_history != NULL) {
      delete MockObjectFetcher::s_history;
      MockObjectFetcher::s_history = NULL;
    }
    MockObjectFetcher::s_deleted_catalogs = NULL;
    MockObjectFetcher::history_available = true;
  }

 public:
  MockObjectFetcher(const swissknife::CatalogTraversalParams &params) {}
 public:
  manifest::Manifest* FetchManifest();
  history::History* FetchHistory() {
    return (MockObjectFetcher::history_available)
              ? MockObjectFetcher::s_history->Clone()
              : NULL;
  }
  inline bool Fetch(const shash::Any  &catalog_hash,
                    std::string       *catalog_file) {
    catalog_file->clear();
    return (s_deleted_catalogs == NULL ||
            s_deleted_catalogs->find(catalog_hash) == s_deleted_catalogs->end());
  }
  inline bool Exists(const std::string &file) {
    return false;
  }
};

#endif /* CVMFS_UNITTEST_TESTUTIL */
