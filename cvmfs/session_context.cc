/**
 * This file is part of the CernVM File System.
 */

#include "session_context.h"

namespace upload {

SessionContext::SessionContext() {}

SessionContext::~SessionContext() {}

bool SessionContext::FinalizeSession() { return true; }
}
