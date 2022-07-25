/**
 * This file is part of the CernVM File System.
 */

#include "authz.h"

#include <cstring>

#include "util/smalloc.h"

AuthzToken *AuthzToken::DeepCopy() {
  AuthzToken *result = new AuthzToken();
  result->type = this->type;
  result->size = this->size;
  if (this->size > 0) {
    result->data = smalloc(result->size);
    memcpy(result->data, this->data, this->size);
  } else {
    result->data = NULL;
  }
  return result;
}
