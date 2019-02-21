/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_REPOSITORY_H_
#define CVMFS_PUBLISH_REPOSITORY_H_

#include <string>

#include "publish/settings.h"
#include "util/single_copy.h"

namespace publish {

class Storage : SingleCopy {
 public:
  void Put(const std::string &local_src, const std::string &remote_dst);
  void Get(const std::string &remote_src, const std::string &local_dst);
  bool Peek(const std::string &path);
  void Scrub();
};

class Repository : SingleCopy {
 public:
  void Check();
  void GarbageCollect();
  void List();
  void Diff();

 protected:
  Storage storage_;
};

class Publisher : public Repository {
 public:
  static Publisher *Create(const SettingsPublisher &settings);
  void UpdateMetaInfo();
  void Publish();
  void Ingest();
  void EditTags();
  void Rollback();
  void Resign();
  void Migrate();
};

class Replica : public Repository {
 public:
  static Replica *Create();
  void Snapshot();
};

}  // namespace publish

#endif  // CVMFS_PUBLISH_REPOSITORY_H_
