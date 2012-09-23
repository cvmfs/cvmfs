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

static void ParseKeyvalMem(const unsigned char *buffer,
                           const unsigned buffer_size,
                           map<char, string> *content)
{
  string line;
  unsigned pos = 0;
  while (pos < buffer_size) {
    if (static_cast<char>(buffer[pos]) == '\n') {
      if (line == "--")
        return;

      if (line != "") {
        const string tail = (line.length() == 1) ? "" : line.substr(1);
        (*content)[line[0]] = tail;
      }
      line = "";
    } else {
      line += static_cast<char>(buffer[pos]);
    }
    pos++;
  }
}


static bool ParseKeyvalPath(const string &filename,
                            map<char, string> *content)
{
  int fd = open(filename.c_str(), O_RDONLY);
  if (fd < 0)
    return false;

  unsigned char buffer[4096];
  int num_bytes = read(fd, buffer, sizeof(buffer));
  close(fd);

  if ((num_bytes <= 0) || (unsigned(num_bytes) >= sizeof(buffer)))
    return false;

  ParseKeyvalMem(buffer, unsigned(num_bytes), content);
  return true;
}


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
  hash::Any catalog_hash;
  hash::Md5 root_path;
  uint32_t ttl;
  uint64_t revision;

  iter = content.find('C');
  if ((iter = content.find('C')) == content.end())
    return NULL;
  catalog_hash = hash::Any(hash::kSha1, hash::HexPtr(iter->second));
  if ((iter = content.find('R')) == content.end())
    return NULL;
  root_path = hash::Md5(hash::HexPtr(iter->second));
  if ((iter = content.find('D')) == content.end())
    return NULL;
  ttl = String2Uint64(iter->second);
  if ((iter = content.find('S')) == content.end())
    return NULL;
  revision = String2Uint64(iter->second);

  // Optional keys
  hash::Any micro_catalog_hash;
  string repository_name;
  hash::Any certificate;
  uint64_t publish_timestamp = 0;

  if ((iter = content.find('L')) != content.end())
    micro_catalog_hash = hash::Any(hash::kSha1, hash::HexPtr(iter->second));
  if ((iter = content.find('N')) != content.end())
    repository_name = iter->second;
  if ((iter = content.find('X')) != content.end())
    certificate = hash::Any(hash::kSha1, hash::HexPtr(iter->second));
  if ((iter = content.find('T')) != content.end())
    publish_timestamp = String2Uint64(iter->second);

  return new Manifest(catalog_hash, root_path, ttl, revision,
                      micro_catalog_hash, repository_name, certificate,
                      publish_timestamp);
}


Manifest::Manifest(const hash::Any &catalog_hash, const string &root_path) {
  catalog_hash_ = catalog_hash;
  root_path_ = hash::Md5(hash::AsciiPtr(root_path));
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
    "R" + root_path_.ToString() + "\n" +
    "D" + StringifyInt(ttl_) + "\n" +
    "S" + StringifyInt(revision_) + "\n";

  if (!micro_catalog_hash_.IsNull())
    manifest += "L" + micro_catalog_hash_.ToString() + "\n";
  if (repository_name_ != "")
    manifest += "N" + repository_name_ + "\n";
  if (!certificate_.IsNull())
    manifest += "X" + certificate_.ToString() + "\n";
  if (publish_timestamp_ > 0)
    manifest += "T" + StringifyInt(publish_timestamp_) + "\n";

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

}  // namespace manifest
