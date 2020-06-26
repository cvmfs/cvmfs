/**
 * This file is part of the CernVM File System.
 */

#include "manifest.h"

#include <cstdio>
#include <map>

#include "catalog.h"
#include "util/posix.h"

using namespace std;  // NOLINT

namespace manifest {

Breadcrumb::Breadcrumb(const std::string &from_string) {
  timestamp = 0;
  int len = from_string.length();

  // Separate hash from timestamp
  int separator_pos = 0;
  for (; (separator_pos < len) && (from_string[separator_pos] != 'T');
       ++separator_pos)
  { }
  catalog_hash =
    shash::MkFromHexPtr(shash::HexPtr(from_string.substr(0, separator_pos)),
                                      shash::kSuffixCatalog);

  // Get local last modified time
  if ((from_string[separator_pos] == 'T') && (len > (separator_pos + 1))) {
    timestamp = String2Uint64(from_string.substr(separator_pos + 1));
  }
}

bool Breadcrumb::Export(const string &fqrn, const string &directory,
                        const int mode) const {
  string breadcrumb_path = MakeCanonicalPath(directory) +
                                "/cvmfschecksum." + fqrn;
  string tmp_path;
  FILE *fbreadcrumb = CreateTempFile(breadcrumb_path, mode, "w", &tmp_path);
  if (fbreadcrumb == NULL)
    return false;
  string str_breadcrumb = ToString();
  int written = fwrite(&(str_breadcrumb[0]), 1, str_breadcrumb.length(),
                       fbreadcrumb);
  fclose(fbreadcrumb);
  if (static_cast<unsigned>(written) != str_breadcrumb.length()) {
    unlink(tmp_path.c_str());
    return false;
  }
  int retval = rename(tmp_path.c_str(), breadcrumb_path.c_str());
  if (retval != 0) {
    unlink(tmp_path.c_str());
    return false;
  }
  return true;
}

std::string Breadcrumb::ToString() const {
  return catalog_hash.ToString() + "T" + StringifyInt(timestamp);
}


//------------------------------------------------------------------------------


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
  shash::Any reflog_hash;

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
  if ((iter = content.find('Y')) != content.end()) {
    reflog_hash = MkFromHexPtr(shash::HexPtr(iter->second));
  }

  return new Manifest(catalog_hash, catalog_size, root_path, ttl, revision,
                      micro_catalog_hash, repository_name, certificate,
                      history, publish_timestamp, garbage_collectable,
                      has_alt_catalog_path, meta_info, reflog_hash);
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
  if (!reflog_hash_.IsNull()) {
    manifest += "Y" + reflog_hash_.ToString() + "\n";
  }
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
bool Manifest::ExportBreadcrumb(const string &directory, const int mode) const {
  return Breadcrumb(catalog_hash_, publish_timestamp_).Export(repository_name_,
                                                              directory, mode);
}


/**
 * Read the hash and the last-modified time stamp from the
 * cvmfschecksum.$repository file in the given directory.
 */
Breadcrumb Manifest::ReadBreadcrumb(
  const std::string &repo_name,
  const std::string &directory)
{
  Breadcrumb breadcrumb;
  const string breadcrumb_path = directory + "/cvmfschecksum." + repo_name;
  FILE *fbreadcrumb = fopen(breadcrumb_path.c_str(), "r");
  if (!fbreadcrumb) {
    // Return invalid breadcrumb if not found
    return breadcrumb;
  }
  char tmp[128];
  int read_bytes;
  if ((read_bytes = fread(tmp, 1, 128, fbreadcrumb)) > 0) {
    breadcrumb = Breadcrumb(std::string(tmp, read_bytes));
  }
  fclose(fbreadcrumb);

  return breadcrumb;
}

}  // namespace manifest
