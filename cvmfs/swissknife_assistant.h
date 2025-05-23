/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_ASSISTANT_H_
#define CVMFS_SWISSKNIFE_ASSISTANT_H_

#include <string>

#include "crypto/hash.h"

namespace catalog {
class Catalog;
}
namespace download {
class DownloadManager;
}
namespace history {
class History;
}
namespace manifest {
class Manifest;
}

namespace swissknife {

/**
 * Common tasks when working with repositories, such as getting the a catalog
 * or getting the history database. Objects are automatically removed when the
 * Assistant goes out of scope.
 */
class Assistant {
 public:
  enum OpenMode {
    kOpenReadOnly,
    kOpenReadWrite
  };

  Assistant(download::DownloadManager *d,
            manifest::Manifest *m,
            const std::string &r,
            const std::string &t)
      : download_mgr_(d), manifest_(m), repository_url_(r), tmp_dir_(t) { }

  history::History *GetHistory(OpenMode open_mode);
  catalog::Catalog *GetCatalog(const shash::Any &catalog_hash,
                               OpenMode open_mode);

 private:
  bool FetchObject(const shash::Any &id, const std::string &local_path);

  download::DownloadManager *download_mgr_;
  manifest::Manifest *manifest_;
  std::string repository_url_;
  std::string tmp_dir_;
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_ASSISTANT_H_
