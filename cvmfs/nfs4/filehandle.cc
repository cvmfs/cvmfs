/**
 * This file is part of the CernVM File System.
 */

#include "nfs4/filehandle.h"

#include "nfs4/xdr.h"

namespace nfs4 {

namespace {
const unsigned char kMagic[4] = {'C', 'V', 'N', '4'};
const unsigned char kVersion = 1;

uint32_t Fnv1a(const unsigned char *data, size_t len) {
  uint32_t h = 2166136261u;
  for (size_t i = 0; i < len; ++i) {
    h ^= data[i];
    h *= 16777619u;
  }
  return h;
}
}  // anonymous namespace

std::string FileHandle::Encode() const {
  XdrWriter w;
  w.PutRaw(kMagic, sizeof(kMagic));
  const unsigned char header[4] = {kVersion, static_cast<unsigned char>(kind),
                                   0, 0};
  w.PutRaw(header, sizeof(header));
  w.PutU64(fsid);
  w.PutU64(ino);
  w.PutU32(aux);
  const std::string &buf = w.buffer();
  w.PutU32(
      Fnv1a(reinterpret_cast<const unsigned char *>(buf.data()), buf.size()));
  return w.buffer();
}

bool FileHandle::Decode(const std::string &raw, FileHandle *fh) {
  if (raw.size() != kSize)
    return false;
  const unsigned char *p = reinterpret_cast<const unsigned char *>(raw.data());
  for (unsigned i = 0; i < sizeof(kMagic); ++i) {
    if (p[i] != kMagic[i])
      return false;
  }
  if (p[4] != kVersion)
    return false;
  const unsigned char kind = p[5];
  if (kind < kObject || kind > kNamedAttr)
    return false;

  XdrReader r(p + 8, kSize - 8);
  uint64_t fsid, ino;
  uint32_t aux, check;
  r.GetU64(&fsid);
  r.GetU64(&ino);
  r.GetU32(&aux);
  r.GetU32(&check);
  if (!r.ok())
    return false;
  if (check != Fnv1a(p, kSize - 4))
    return false;

  fh->kind = static_cast<Kind>(kind);
  fh->fsid = fsid;
  fh->ino = ino;
  fh->aux = aux;
  return true;
}

uint32_t HashName(const std::string &name) {
  return Fnv1a(reinterpret_cast<const unsigned char *>(name.data()),
               name.size());
}

}  // namespace nfs4
