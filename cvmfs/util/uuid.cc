/**
 * This file is part of the CernVM File System.
 *
 * UUID generation and caching.
 */

#define __STDC_FORMAT_MACROS

#include "util/uuid.h"

#include <cassert>
#include <cstdio>
#include <cstring>

#include "util/pointer.h"
#include "util/posix.h"
#include "util/string.h"

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
    const string uuid_str = uuid->uuid();
    string path_tmp;
    FILE *f_tmp = CreateTempFile(
        store_path + "_tmp", S_IWUSR | S_IWGRP | S_IRUSR | S_IRGRP | S_IROTH,
        "w", &path_tmp);
    if (!f_tmp)
      return NULL;
    const int written = fprintf(f_tmp, "%s\n", uuid_str.c_str());
    fclose(f_tmp);
    if (written != static_cast<int>(uuid_str.length() + 1)) {
      unlink(path_tmp.c_str());
      return NULL;
    }
    if (rename(path_tmp.c_str(), store_path.c_str()) != 0) {
      unlink(path_tmp.c_str());
      return NULL;
    }
    return uuid.Release();
  }

  // Read from cached file
  const bool retval = GetLineFile(f, &uuid->uuid_);
  fclose(f);
  if (!retval)
    return NULL;
  const int nitems = sscanf(
      uuid->uuid_.c_str(),
      "%08" SCNx32 "-%04" SCNx16 "-%04" SCNx16 "-%04" SCNx16 "-%08" SCNx32
      "%04" SCNx16,
      &uuid->uuid_presentation_.split.a, &uuid->uuid_presentation_.split.b,
      &uuid->uuid_presentation_.split.c, &uuid->uuid_presentation_.split.d,
      &uuid->uuid_presentation_.split.e1, &uuid->uuid_presentation_.split.e2);
  if (nitems != 6)
    return NULL;

  return uuid.Release();
}


string Uuid::CreateOneTime() {
  Uuid uuid;
  uuid.MkUuid();
  return uuid.uuid_;
}


/**
 * Creates a new UUID in canonical string representation and overwrites uuid_
 * with the result.
 */
void Uuid::MkUuid() {
  uuid_t new_uuid;
  uuid_generate(new_uuid);
  assert(sizeof(new_uuid) == 16);
  memcpy(uuid_presentation_.uuid, new_uuid, sizeof(uuid_presentation_.uuid));
  // Canonical UUID format, including trailing \0
  const unsigned uuid_len = 8 + 1 + 4 + 1 + 4 + 1 + 4 + 1 + 12 + 1;
  char uuid_cstr[uuid_len];
  snprintf(uuid_cstr, uuid_len, "%08x-%04x-%04x-%04x-%08x%04x",
           uuid_presentation_.split.a, uuid_presentation_.split.b,
           uuid_presentation_.split.c, uuid_presentation_.split.d,
           uuid_presentation_.split.e1, uuid_presentation_.split.e2);
  uuid_ = string(uuid_cstr);
}


Uuid::Uuid() { memset(&uuid_presentation_, 0, sizeof(uuid_presentation_)); }

}  // namespace cvmfs
