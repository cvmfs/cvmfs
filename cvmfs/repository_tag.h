/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_REPOSITORY_TAG_H_
#define CVMFS_REPOSITORY_TAG_H_

#include <string>

class RepositoryTag {
 public:
  RepositoryTag() : name_(""), channel_(""), description_("") {}
  RepositoryTag(const std::string& name,
                const std::string& channel,
                const std::string& description);

  void SetName(const std::string& name) {
    name_ = name;
  }
  void SetChannel(const std::string& channel) {
    channel_ = channel;
  }
  void SetDescription(const std::string& description) {
    description_ = description;
  }

  bool HasGenericName();
  void SetGenericName();

  std::string name() const { return name_; }
  std::string channel() const { return channel_; }
  std::string description() const { return description_; }

 private:
  std::string name_;
  std::string channel_;
  std::string description_;
};

#endif  // CVMFS_REPOSITORY_TAG_H_
