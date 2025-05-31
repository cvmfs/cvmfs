/**
 * This file is part of the CernVM File System.
 */

#include "publish/except.h"

#include <execinfo.h>

#include <string>

publish::EPublish::~EPublish() throw() { }

std::string publish::EPublish::GetStacktrace() {
  std::string result;
  void *addr[kMaxBacktrace];
  const int num_addr = backtrace(addr, kMaxBacktrace);
  char **symbols = backtrace_symbols(addr, num_addr);
  for (int i = 0; i < num_addr; ++i)
    result += std::string(symbols[i]) + "\n";
  return result;
}
