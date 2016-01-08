/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_MANIFEST_H_
#define CVMFS_MANIFEST_H_

#include <stdint.h>

#include <map>
#include <string>

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
           const bool garbage_collectable,
           const bool has_alt_catalog_path,
           const shash::Any &meta_info)
  : catalog_hash_(catalog_hash)
  , catalog_size_(catalog_size)
  , root_path_(root_path)
  , ttl_(ttl)
  , revision_(revision)
  , micro_catalog_hash_(micro_catalog_hash)
  , repository_name_(repository_name)
  , certificate_(certificate)
  , history_(history)
  , publish_timestamp_(publish_timestamp)
  , garbage_collectable_(garbage_collectable)
  , has_alt_catalog_path_(has_alt_catalog_path)
  , meta_info_(meta_info) { }

  std::string ExportString() const;
  bool Export(const std::string &path) const;
  bool ExportChecksum(const std::string &directory, const int mode) const;
  static bool ReadChecksum(const std::string &repo_name,
                           const std::string &directory,
                           shash::Any *hash,
                           uint64_t *last_modified);

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
  void set_catalog_size(const uint64_t catalog_size) {
    catalog_size_ = catalog_size;
  }
  void set_catalog_hash(const shash::Any &catalog_hash) {
    catalog_hash_ = catalog_hash;
  }
  void set_garbage_collectability(const bool garbage_collectable) {
    garbage_collectable_ = garbage_collectable;
  }
  void set_has_alt_catalog_path(const bool &has_alt_path) {
    has_alt_catalog_path_ = has_alt_path;
  }
  void set_meta_info(const shash::Any &meta_info) {
    meta_info_ = meta_info;
  }
  void set_root_path(const std::string &root_path) {
    root_path_ = shash::Md5(shash::AsciiPtr(root_path));
  }

  uint64_t revision() const { return revision_; }
  std::string repository_name() const { return repository_name_; }
  shash::Md5 root_path() const { return root_path_; }
  shash::Any catalog_hash() const { return catalog_hash_; }
  uint64_t catalog_size() const { return catalog_size_; }
  shash::Any certificate() const { return certificate_; }
  shash::Any history() const { return history_; }
  uint64_t publish_timestamp() const { return publish_timestamp_; }
  bool garbage_collectable() const { return garbage_collectable_; }
  bool has_alt_catalog_path() const { return has_alt_catalog_path_; }
  shash::Any meta_info() const { return meta_info_; }

  std::string MakeCatalogPath() const {
    return has_alt_catalog_path_ ? catalog_hash_.MakeAlternativePath() :
      ("data/" + catalog_hash_.MakePath());
  }

  std::string MakeCertificatePath() const {
    return has_alt_catalog_path_ ?
      certificate_.MakeAlternativePath() : ("data/" + certificate_.MakePath());
  }

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
  bool garbage_collectable_;

  /**
   * The root catalog and the certifacte might be available as .cvmfscatalog and
   * .cvmfscertificate.  That is helpful if the data subdirectory is protected
   * on the web server.
   */
  bool has_alt_catalog_path_;

  /**
   * Hash of a JSON object that describes the repository (owner, purpose, list
   * of recommended stratum 1s, ...)
   */
  shash::Any meta_info_;
};  // class Manifest

}  // namespace manifest

#endif  // CVMFS_MANIFEST_H_
