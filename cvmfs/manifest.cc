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

  return new Manifest(catalog_hash, catalog_size, root_path, ttl, revision,
                      micro_catalog_hash, repository_name, certificate,
                      history, publish_timestamp);
}


Manifest::Manifest(const shash::Any &catalog_hash,
                   const uint64_t catalog_size,
                   const string &root_path)
{
  catalog_hash_ = catalog_hash;
  catalog_size_ = catalog_size;
  root_path_ = shash::Md5(shash::AsciiPtr(root_path));
  ttl_ = catalog::Catalog::kDefaultTTL;
  revision_ = 0;
  publish_timestamp_ = 0;
}


/**
 * Creates the manifest string
 */
string Manifest::ExportString() const {
  string manifest =
    "C" + catalog_hash_.ToString() + "\n" +
    "B" + StringifyInt(catalog_size_) + "\n" +
    "R" + root_path_.ToString() + "\n" +
    "D" + StringifyInt(ttl_) + "\n" +
    "S" + StringifyInt(revision_) + "\n";

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

}  // namespace manifest
