/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_MANIFEST_H_
#define CVMFS_MANIFEST_H_

#include <stdint.h>
#include <string>
#include <map>
#include "hash.h"

namespace manifest {

/**
 * The Manifest is the bootstrap snippet for a repository.  It is stored in
 * .cvmfspublished.
 */
class Manifest {
 public:
  static Manifest *LoadFile(const std::string &path);
  static Manifest *LoadMem(const unsigned char *buffer, const unsigned length);
  Manifest(const hash::Any &catalog_hash, const std::string &root_path);
  Manifest(const hash::Any &catalog_hash,
           const hash::Md5 &root_path,
           const uint32_t ttl,
           const uint64_t revision,
           const hash::Any &micro_catalog_hash,
           const std::string &repository_name,
           const hash::Any certificate,
           const hash::Any history,
           const uint64_t publish_timestamp) :
    catalog_hash_(catalog_hash), root_path_(root_path), ttl_(ttl),
    revision_(revision), micro_catalog_hash_(micro_catalog_hash),
    repository_name_(repository_name), certificate_(certificate),
    history_(history), publish_timestamp_(publish_timestamp) { };

  std::string ExportString() const;
  bool Export(const std::string &path) const;

  void set_ttl(const uint32_t ttl) { ttl_ = ttl; }
  void set_revision(const uint64_t revision) { revision_ = revision; }
  void set_certificate(const hash::Any &certificate) {
    certificate_ = certificate;
  }
  void set_history(const hash::Any &history_db) {
    history_ = history_db;
  }
  void set_repository_name(const std::string &repository_name) {
    repository_name_ = repository_name;
  }
  void set_publish_timestamp(const uint32_t publish_timestamp) {
    publish_timestamp_ = publish_timestamp;
  }

  std::string repository_name() const { return repository_name_; }
  hash::Md5 root_path() const { return root_path_; }
  hash::Any catalog_hash() const { return catalog_hash_; }
  hash::Any certificate() const { return certificate_; }
  hash::Any history() const { return history_; }
  uint64_t publish_timestamp() const { return publish_timestamp_; }
 private:
  static Manifest *Load(const std::map<char, std::string> &content);
  hash::Any catalog_hash_;
  hash::Md5 root_path_;
  uint32_t ttl_;
  uint64_t revision_;
  hash::Any micro_catalog_hash_;
  std::string repository_name_;
  hash::Any certificate_;
  hash::Any history_;
  uint64_t publish_timestamp_;
};  // class Manifest

}  // namespace manifest

#endif  // CVMFS_MANIFEST_H_
