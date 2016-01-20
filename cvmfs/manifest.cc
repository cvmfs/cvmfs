/**
 * This file is part of the CernVM File System.
 */

#include "manifest.h"

#include <cstdio>
#include <map>

#include "catalog.h"
#include "util.h"

using namespace std;  // NOLINT

namespace manifest {

Manifest *Manifest::LoadMem(const unsigned char *buffer,
                            const unsigned length)
{
  map<char, string> content;
  ParseKeyvalMem(buffer, length, &content);

  return Load(content);
}


Manifest *Manifest::LoadFile(const std::string &from_path) {
  map<char, string> content;
  if (!ParseKeyvalPath(from_path, &content))
    return NULL;

  return Load(content);
}


Manifest *Manifest::Load(const map<char, string> &content) {
  map<char, string>::const_iterator iter;

  // Required keys
  shash::Any catalog_hash;
  shash::Md5 root_path;
  uint32_t ttl;
  uint64_t revision;

  iter = content.find('C');
  if ((iter = content.find('C')) == content.end())
    return NULL;
  catalog_hash = MkFromHexPtr(shash::HexPtr(iter->second),
                              shash::kSuffixCatalog);
  if ((iter = content.find('R')) == content.end())
    return NULL;
  root_path = shash::Md5(shash::HexPtr(iter->second));
  if ((iter = content.find('D')) == content.end())
    return NULL;
  ttl = String2Uint64(iter->second);
  if ((iter = content.find('S')) == content.end())
    return NULL;
  revision = String2Uint64(iter->second);


  // Optional keys
  uint64_t catalog_size = 0;
  shash::Any micro_catalog_hash;
  string repository_name;
  shash::Any certificate;
  shash::Any history;
  uint64_t publish_timestamp = 0;
  bool garbage_collectable = false;
  bool has_alt_catalog_path = false;
  shash::Any meta_info;

  if ((iter = content.find('B')) != content.end())
    catalog_size = String2Uint64(iter->second);
  if ((iter = content.find('L')) != content.end())
    micro_catalog_hash = MkFromHexPtr(shash::HexPtr(iter->second),
                                      shash::kSuffixMicroCatalog);
  if ((iter = content.find('N')) != content.end())
    repository_name = iter->second;
  if ((iter = content.find('X')) != content.end())
    certificate = MkFromHexPtr(shash::HexPtr(iter->second),
                               shash::kSuffixCertificate);
  if ((iter = content.find('H')) != content.end())
    history = MkFromHexPtr(shash::HexPtr(iter->second),
                           shash::kSuffixHistory);
  if ((iter = content.find('T')) != content.end())
    publish_timestamp = String2Uint64(iter->second);
  if ((iter = content.find('G')) != content.end())
    garbage_collectable = (iter->second == "yes");
  if ((iter = content.find('A')) != content.end())
    has_alt_catalog_path = (iter->second == "yes");
  if ((iter = content.find('M')) != content.end())
    meta_info = MkFromHexPtr(shash::HexPtr(iter->second),
                             shash::kSuffixMetainfo);

  return new Manifest(catalog_hash, catalog_size, root_path, ttl, revision,
                      micro_catalog_hash, repository_name, certificate,
                      history, publish_timestamp, garbage_collectable,
                      has_alt_catalog_path, meta_info);
}


Manifest::Manifest(const shash::Any &catalog_hash,
                   const uint64_t catalog_size,
                   const string &root_path)
  : catalog_hash_(catalog_hash)
  , catalog_size_(catalog_size)
  , root_path_(shash::Md5(shash::AsciiPtr(root_path)))
  , ttl_(catalog::Catalog::kDefaultTTL)
  , revision_(0)
  , publish_timestamp_(0)
  , garbage_collectable_(false)
  , has_alt_catalog_path_(false)
{ }


/**
 * Creates the manifest string
 */
string Manifest::ExportString() const {
  string manifest =
    "C" + catalog_hash_.ToString() + "\n" +
    "B" + StringifyInt(catalog_size_) + "\n" +
    "R" + root_path_.ToString() + "\n" +
    "D" + StringifyInt(ttl_) + "\n" +
    "S" + StringifyInt(revision_) + "\n" +
    "G" + StringifyBool(garbage_collectable_) + "\n" +
    "A" + StringifyBool(has_alt_catalog_path_) + "\n";

  if (!micro_catalog_hash_.IsNull())
    manifest += "L" + micro_catalog_hash_.ToString() + "\n";
  if (repository_name_ != "")
    manifest += "N" + repository_name_ + "\n";
  if (!certificate_.IsNull())
    manifest += "X" + certificate_.ToString() + "\n";
  if (!history_.IsNull())
    manifest += "H" + history_.ToString() + "\n";
  if (publish_timestamp_ > 0)
    manifest += "T" + StringifyInt(publish_timestamp_) + "\n";
  if (!meta_info_.IsNull())
    manifest += "M" + meta_info_.ToString() + "\n";
  // Reserved: Z -> for identification of channel tips

  return manifest;
}



/**
 * Writes the .cvmfspublished file (unsigned).
 */
bool Manifest::Export(const std::string &path) const {
  FILE *fmanifest = fopen(path.c_str(), "w");
  if (!fmanifest)
    return false;

  string manifest = ExportString();

  if (fwrite(manifest.data(), 1, manifest.length(), fmanifest) !=
      manifest.length())
  {
    fclose(fmanifest);
    unlink(path.c_str());
    return false;
  }
  fclose(fmanifest);

  return true;
}


/**
 * Writes the cvmfschecksum.$repository file.  Atomic store.
 */
bool Manifest::ExportChecksum(const string &directory, const int mode) const {
  string checksum_path = MakeCanonicalPath(directory) + "/cvmfschecksum." +
                         repository_name_;
  string checksum_tmp_path;
  FILE *fchksum = CreateTempFile(checksum_path, mode, "w", &checksum_tmp_path);
  if (fchksum == NULL)
    return false;
  string cache_checksum = catalog_hash_.ToString() + "T" +
                          StringifyInt(publish_timestamp_);
  int written = fwrite(&(cache_checksum[0]), 1, cache_checksum.length(),
                       fchksum);
  fclose(fchksum);
  if (static_cast<unsigned>(written) != cache_checksum.length()) {
    unlink(checksum_tmp_path.c_str());
    return false;
  }
  int retval = rename(checksum_tmp_path.c_str(), checksum_path.c_str());
  if (retval != 0) {
    unlink(checksum_tmp_path.c_str());
    return false;
  }
  return true;
}


/**
 * Read the hash and the last-modified time stamp from the
 * cvmfschecksum.$repository file in the given directory.
 */
bool Manifest::ReadChecksum(
  const std::string &repo_name,
  const std::string &directory,
  shash::Any *hash,
  uint64_t *last_modified)
{
  bool result = false;
  const string checksum_path = directory + "/cvmfschecksum." + repo_name;
  FILE *file_checksum = fopen(checksum_path.c_str(), "r");
  char tmp[128];
  int read_bytes;
  if (file_checksum && (read_bytes = fread(tmp, 1, 128, file_checksum)) > 0) {
    // Separate hash from timestamp
    int separator_pos = 0;
    for (; (separator_pos < read_bytes) && (tmp[separator_pos] != 'T');
         ++separator_pos) { }
    *hash = shash::MkFromHexPtr(shash::HexPtr(string(tmp, separator_pos)),
                                shash::kSuffixCatalog);

    // Get local last modified time
    string str_modified;
    if ((tmp[separator_pos] == 'T') && (read_bytes > (separator_pos+1))) {
      str_modified = string(tmp+separator_pos+1,
                            read_bytes-(separator_pos+1));
      *last_modified = String2Uint64(str_modified);
      result = true;
    }
  }
  if (file_checksum) fclose(file_checksum);

  return result;
}

}  // namespace manifest
