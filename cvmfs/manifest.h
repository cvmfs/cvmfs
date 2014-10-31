/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_MANIFEST_H_
#define CVMFS_MANIFEST_H_

#include <stdint.h>
#include <string>
#include <map>
#include "hash.h"
#include "history.h"

namespace manifest {

/**
 * The Manifest is the bootstrap snippet for a repository.  It is stored in
 * .cvmfspublished.
 */
class Manifest {
 public:
  static Manifest *LoadFile(const std::string &path);
  static Manifest *LoadMem(const unsigned char *buffer, const unsigned length);
  Manifest(const shash::Any &catalog_hash,
           const uint64_t catalog_size,
           const std::string &root_path);
  Manifest(const shash::Any &catalog_hash,
           const uint64_t catalog_size,
           const shash::Md5 &root_path,
           const uint32_t ttl,
           const uint64_t revision,
           const shash::Any &micro_catalog_hash,
           const std::string &repository_name,
           const shash::Any certificate,
           const shash::Any history,
           const uint64_t publish_timestamp,
           const std::vector<history::TagList::ChannelTag> &channel_tops) :
    catalog_hash_(catalog_hash),
    catalog_size_(catalog_size),
    root_path_(root_path),
    ttl_(ttl),
    revision_(revision),
    micro_catalog_hash_(micro_catalog_hash),
    repository_name_(repository_name),
    certificate_(certificate),
    history_(history),
    publish_timestamp_(publish_timestamp),
    channel_tops_(channel_tops) { };

  std::string ExportString() const;
  bool Export(const std::string &path) const;
  bool ExportChecksum(const std::string &directory, const int mode) const;

  shash::Algorithms GetHashAlgorithm() const { return catalog_hash_.algorithm; }

  void set_ttl(const uint32_t ttl) { ttl_ = ttl; }
  void set_revision(const uint64_t revision) { revision_ = revision; }
  void set_certificate(const shash::Any &certificate) {
    certificate_ = certificate;
  }
  void set_history(const shash::Any &history_db) {
    history_ = history_db;
  }
  void set_repository_name(const std::string &repository_name) {
    repository_name_ = repository_name;
  }
  void set_publish_timestamp(const uint32_t publish_timestamp) {
    publish_timestamp_ = publish_timestamp;
  }
  void set_channel_tops(const std::vector<history::TagList::ChannelTag> &v) {
    channel_tops_ = v;
  }
  void set_catalog_size(const uint64_t catalog_size) {
    catalog_size_ = catalog_size;
  }
  void set_catalog_hash(const shash::Any &catalog_hash) {
    catalog_hash_ = catalog_hash;
  }

  uint64_t revision() const { return revision_; }
  std::string repository_name() const { return repository_name_; }
  shash::Md5 root_path() const { return root_path_; }
  shash::Any catalog_hash() const { return catalog_hash_; }
  uint64_t catalog_size() const { return catalog_size_; }
  shash::Any certificate() const { return certificate_; }
  shash::Any history() const { return history_; }
  uint64_t publish_timestamp() const { return publish_timestamp_; }
 private:
  static Manifest *Load(const std::map<char, std::string> &content);
  shash::Any catalog_hash_;
  uint64_t catalog_size_;
  shash::Md5 root_path_;
  uint32_t ttl_;
  uint64_t revision_;
  shash::Any micro_catalog_hash_;
  std::string repository_name_;
  shash::Any certificate_;
  shash::Any history_;
  uint64_t publish_timestamp_;
  // ordered, newest releases first
  std::vector<history::TagList::ChannelTag> channel_tops_;
};  // class Manifest

}  // namespace manifest

#endif  // CVMFS_MANIFEST_H_
