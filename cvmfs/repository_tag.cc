/**
 * This file is part of the CernVM File System.
 */

#include "repository_tag.h"

#include "util/platform.h"
#include "util/string.h"

RepositoryTag::RepositoryTag(const std::string &name,
                             const std::string &description)
    : name_(name), description_(description) { }

/**
 * Check if tag name is of the form "generic-*"
 */
bool RepositoryTag::HasGenericName() {
  return HasPrefix(name_, "generic-", false);
}

/**
 * Set a generic tag name of the form "generic-YYYY-MM-DDThh:mm:ss.sssZ"
 */
void RepositoryTag::SetGenericName() {
  const uint64_t nanoseconds = platform_realtime_ns();

  // Use strftime() to format timestamp to one-second resolution
  const time_t seconds = static_cast<time_t>(nanoseconds / 1000000000);
  struct tm timestamp;
  gmtime_r(&seconds, &timestamp);
  char seconds_buffer[32];
  strftime(seconds_buffer, sizeof(seconds_buffer), "generic-%Y-%m-%dT%H:%M:%S",
           &timestamp);

  // Append milliseconds
  const unsigned offset_milliseconds = ((nanoseconds / 1000000) % 1000);
  char name_buffer[48];
  snprintf(name_buffer, sizeof(name_buffer), "%s.%03dZ", seconds_buffer,
           offset_milliseconds);

  name_ = std::string(name_buffer);
}
