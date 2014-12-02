/**
 * This file is part of the CernVM File System.
 *
 * UUID generation and caching.
 */

#include "uuid.h"

#include <inttypes.h>
#include <uuid/uuid.h>

#include <cassert>
#include <cstdio>
#include <cstring>

#include "util.h"

using namespace std;  // NOLINT

namespace cvmfs {

Uuid *Uuid::Create(const string &store_path) {
  UniquePtr<Uuid> uuid(new Uuid());
  if (store_path == "") {
    uuid->MkUuid();
    return uuid.Release();
  }

  FILE *f = fopen(store_path.c_str(), "r");
  if (f == NULL) {
    // Create and store
    uuid->MkUuid();
    string uuid_str = uuid->uuid();
    f = fopen(store_path.c_str(), "w");
    if (!f)
      return NULL;
    int written = fprintf(f, "%s\n", uuid_str.c_str());
    fclose(f);
    if (written != int(uuid_str.length() + 1))
      return NULL;
    return uuid.Release();
  }

  // Read from file
  bool retval = GetLineFile(f, &uuid->uuid_);
  fclose(f);
  if (!retval)
    return NULL;

  return uuid.Release();
}


/**
 * Creates a new UUID in canonical string representation and overwrites uuid_
 * with the result.
 */
void Uuid::MkUuid() {
  union {
    uuid_t uuid;
    struct __attribute__((__packed__)) {
      uint32_t a;
      uint16_t b;
      uint16_t c;
      uint16_t d;
      uint32_t e1;
      uint16_t e2;
    } split;
  } uuid_presentation;
  uuid_t new_uuid;
  uuid_generate(new_uuid);
  assert(sizeof(new_uuid) == 16);
  memcpy(uuid_presentation.uuid, new_uuid, sizeof(uuid_presentation.uuid));
  // Canonical UUID format, including trailing \0
  unsigned uuid_len = 8+1+4+1+4+1+4+1+12+1;
  char uuid_cstr[uuid_len];
  snprintf(uuid_cstr, uuid_len, "%08x-%04x-%04x-%04x-%08x%04x",
           uuid_presentation.split.a, uuid_presentation.split.b,
           uuid_presentation.split.c, uuid_presentation.split.d,
           uuid_presentation.split.e1, uuid_presentation.split.e2);
  uuid_ = string(uuid_cstr);
}

}  // namespace cvmfs
