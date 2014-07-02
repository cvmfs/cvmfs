#include "testutil.h"

#include <fstream>
#include <sstream>

#include <gtest/gtest.h>

#include "../../cvmfs/manifest.h"

#ifdef __APPLE__
  #include <sys/sysctl.h>
#endif


void SkipWhitespace(std::istringstream &iss) {
  while (iss.good()) {
    const char next = iss.peek();
    if (next != ' ' && next != '\t') {
      break;
    }
    iss.get();
  }
}


pid_t GetParentPid(const pid_t pid) {
  pid_t parent_pid = 0;

#ifdef __APPLE__
  int mib[4];
  size_t len;
  struct kinfo_proc kp;

  len = 4;
  sysctlnametomib("kern.proc.pid", mib, &len);

  mib[3] = pid;
  len = sizeof(kp);
  if (sysctl(mib, 4, &kp, &len, NULL, 0) == 0) {
    parent_pid = kp.kp_eproc.e_ppid;
  }
#else
  static const std::string ppid_label = "PPid:";

  std::stringstream proc_status_path;
  proc_status_path << "/proc/" << pid << "/status";

  std::ifstream proc_status(proc_status_path.str().c_str());

  std::string line;
  while (std::getline(proc_status, line)) {
    if (line.compare(0, ppid_label.size(), ppid_label) == 0) {
      const std::string s_ppid = line.substr(ppid_label.size());
      std::istringstream iss_ppid(s_ppid);
      SkipWhitespace(iss_ppid);
      int i_ppid = 0; iss_ppid >> i_ppid;
      if (i_ppid > 0) {
        parent_pid = static_cast<pid_t>(i_ppid);
      }
      break;
    }
  }
#endif

  return parent_pid;
}

namespace catalog {

DirectoryEntry DirectoryEntryTestFactory::RegularFile() {
  DirectoryEntry dirent;
  dirent.mode_ = 33188;
  return dirent;
}


DirectoryEntry DirectoryEntryTestFactory::Directory() {
  DirectoryEntry dirent;
  dirent.mode_ = 16893;
  return dirent;
}


DirectoryEntry DirectoryEntryTestFactory::Symlink() {
  DirectoryEntry dirent;
  dirent.mode_ = 41471;
  return dirent;
}


DirectoryEntry DirectoryEntryTestFactory::ChunkedFile() {
  DirectoryEntry dirent;
  dirent.mode_ = 33188;
  dirent.is_chunked_file_ = true;
  return dirent;
}

} /* namespace catalog */


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


MockCatalog::AvailableCatalogs MockCatalog::available_catalogs;
unsigned int                   MockCatalog::instances = 0;

const std::string MockCatalog::rhs       = "f9d87ae2cc46be52b324335ff05fae4c1a7c4dd4";
const shash::Any  MockCatalog::root_hash = shash::Any(shash::kSha1,
                                                      shash::HexPtr(MockCatalog::rhs),
                                                      'C');


void MockCatalog::Reset() {
  MockCatalog::instances = 0;
  MockCatalog::UnregisterCatalogs();
}

void MockCatalog::RegisterCatalog(MockCatalog *catalog) {
  ASSERT_EQ (MockCatalog::available_catalogs.end(),
             MockCatalog::available_catalogs.find(catalog->catalog_hash()));
  MockCatalog::available_catalogs[catalog->catalog_hash()] = catalog;
}

void MockCatalog::UnregisterCatalogs() {
  MockCatalog::AvailableCatalogs::const_iterator i, iend;
  for (i    = MockCatalog::available_catalogs.begin(),
       iend = MockCatalog::available_catalogs.end();
       i != iend; ++i)
  {
    delete i->second;
  }
  MockCatalog::available_catalogs.clear();
}

MockCatalog* MockCatalog::GetCatalog(const shash::Any &catalog_hash) {
  AvailableCatalogs::const_iterator clg_itr =
    MockCatalog::available_catalogs.find(catalog_hash);
  return (MockCatalog::available_catalogs.end() != clg_itr)
    ? clg_itr->second
    : NULL;
}

MockCatalog* MockCatalog::AttachFreely(const std::string  &root_path,
                                       const std::string  &file,
                                       const shash::Any   &catalog_hash,
                                             MockCatalog  *parent) {
  const MockCatalog *catalog = MockCatalog::GetCatalog(catalog_hash);
  if (catalog == NULL) {
    return NULL;
  } else {
    MockCatalog *new_catalog = catalog->Clone();
    new_catalog->set_parent(parent);
    return new_catalog;
  }
}

void MockCatalog::RegisterChild(MockCatalog *child) {
  NestedCatalog nested;
  nested.path  = PathString(child->root_path());
  nested.hash  = child->catalog_hash();
  nested.child = child;
  nested.size  = child->catalog_size();
  children_.push_back(nested);
}

void MockCatalog::AddFile(const shash::Any   &content_hash,
                          const size_t        file_size) {
  MockCatalog::File f;
  f.hash = content_hash;
  f.size = file_size;
  files_.push_back(f);
}

UniquePtr<history::History>* MockObjectFetcher::s_history;

manifest::Manifest* MockObjectFetcher::FetchManifest() {
  return new manifest::Manifest(MockCatalog::root_hash, 0, "");
}
