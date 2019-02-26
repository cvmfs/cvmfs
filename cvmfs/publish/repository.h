/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_REPOSITORY_H_
#define CVMFS_PUBLISH_REPOSITORY_H_

#include <string>

#include "publish/settings.h"
#include "util/single_copy.h"

namespace signature {
class SignatureManager;
}
namespace upload {
class Spooler;
}
namespace whitelist {
class Whitelist;
}

namespace publish {

class Repository : SingleCopy {
 public:
  Repository();
  ~Repository();

  void Check();
  void GarbageCollect();
  void List();
  void Diff();

  void TakeSpooler(upload::Spooler *spooler) { spooler_ = spooler; }
  upload::Spooler *spooler() { return spooler_; }

  void TakeWhitelist(whitelist::Whitelist *wl) { whitelist_ = wl; }
  whitelist::Whitelist *whitelist() { return whitelist_; }

 protected:
  upload::Spooler *spooler_;
  whitelist::Whitelist *whitelist_;
};

class __attribute__((visibility("default"))) Publisher : public Repository {
 public:
  static Publisher *Create(const SettingsPublisher &settings);

  Publisher(const SettingsPublisher &settings);
  ~Publisher();

  void UpdateMetaInfo();
  void Publish();
  void Ingest();
  void EditTags();
  void Rollback();
  void Resign();
  void Migrate();

  signature::SignatureManager *signature_mgr() { return signature_mgr_; }

 private:
  SettingsPublisher settings_;

  signature::SignatureManager *signature_mgr_;
};

class Replica : public Repository {
 public:
  static Replica *Create();
  void Snapshot();
};

}  // namespace publish

#endif  // CVMFS_PUBLISH_REPOSITORY_H_
