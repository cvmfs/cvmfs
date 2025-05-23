/**
 * This file is part of the CernVM File System.
 */

#include "publisher.h"

#include <ctime>

#include "util/string.h"

namespace notify {

Publisher::~Publisher() { }

bool Publisher::Init() { return true; }

bool Publisher::Finalize() { return true; }

}  // namespace notify
