/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_REPOSITORY_H_
#define CVMFS_PUBLISH_REPOSITORY_H_

#include <string>

#include "publish/settings.h"
#include "upload.h"
#include "util/pointer.h"
#include "util/single_copy.h"

namespace publish {

class Repository : SingleCopy {
 public:
  void Check();
  void GarbageCollect();
  void List();
  void Diff();

 protected:
  UniquePtr<upload::Spooler> spooler_;
};

class __attribute__((visibility("default"))) Publisher : public Repository {
 public:
  static Publisher *Create(const SettingsPublisher &settings);

  Publisher(const SettingsPublisher &settings) : settings_(settings) {}

  void UpdateMetaInfo();
  void Publish();
  void Ingest();
  void EditTags();
  void Rollback();
  void Resign();
  void Migrate();

 private:
  SettingsPublisher settings_;
};

class Replica : public Repository {
 public:
  static Replica *Create();
  void Snapshot();
};

}  // namespace publish

#endif  // CVMFS_PUBLISH_REPOSITORY_H_
