/**
 * This file is part of the CernVM File System.
 */

#include "path_filters/inclusion_spec.h"

#include <cstdio>
#include <cstdlib>
#include <string>

#include "util/logging.h"
#include "util/posix.h"
#include "util/string.h"

using namespace catalog;  // NOLINT


InclusionSpec::InclusionSpec() : valid_(false), version_(-1) {}

InclusionSpec::~InclusionSpec() {}


InclusionSpec *InclusionSpec::Create(const std::string &spec_path) {
  InclusionSpec *spec = new InclusionSpec();

  FILE *f = fopen(spec_path.c_str(), "r");
  if (f == NULL) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "InclusionSpec: cannot open spec file '%s'",
             spec_path.c_str());
    delete spec;
    return NULL;
  }

  std::string content;
  char buf[4096];
  size_t nbytes;
  while ((nbytes = fread(buf, 1, sizeof(buf), f)) > 0) {
    content.append(buf, nbytes);
  }
  fclose(f);

  if (!spec->Parse(content)) {
    delete spec;
    return NULL;
  }
  return spec;
}


bool InclusionSpec::Parse(const std::string &spec) {
  valid_ = false;
  content_ = spec;

  // Split into lines and find the version line
  std::vector<std::string> lines = SplitString(spec, '\n');

  // Find the first non-comment, non-blank line — must be "version N"
  bool found_version = false;
  for (size_t i = 0; i < lines.size(); ++i) {
    std::string line = Trim(lines[i]);
    if (line.empty() || line[0] == '#') {
      continue;
    }
    if (!ParseVersion(line)) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "InclusionSpec: first non-comment line must be 'version N', "
               "got '%s'",
               line.c_str());
      return false;
    }
    found_version = true;
    break;
  }

  if (!found_version) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "InclusionSpec: spec file is empty or contains only comments");
    return false;
  }

  if (version_ != kCurrentVersion) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "InclusionSpec: unsupported version %d (expected %d)",
             version_, kCurrentVersion);
    return false;
  }

  // Parse the remaining lines as path rules using RelaxedPathFilter
  const std::string filter_content = StripVersionLine(spec);
  if (!filter_.Parse(filter_content)) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "InclusionSpec: failed to parse path rules");
    return false;
  }

  valid_ = true;
  return true;
}


bool InclusionSpec::IsExcluded(const std::string &path) const {
  if (!valid_)
    return false;

  // Root catalog is never excluded
  if (path.empty() || path == "/")
    return false;

  // The spec is an inclusion list: positive rules mean "include" (replicate)
  // and ! rules mean "exclude". RelaxedPathFilter::IsMatching returns true if a
  // path matches a positive rule (including the parents and sub paths of listed
  // paths) and is not opposed by a negative ! rule.
  // So IsMatching == true means "this path is in the inclusion set", which we
  // replicate; everything else is excluded from object replication.
  return !filter_.IsMatching(path);
}


bool InclusionSpec::ParseVersion(const std::string &line) {
  // Expected format: "version N"
  const std::string trimmed = Trim(line);
  if (trimmed.substr(0, 8) != "version ") {
    return false;
  }
  std::string version_str = Trim(trimmed.substr(8));
  if (version_str.empty()) {
    return false;
  }
  // Check all digits
  for (size_t i = 0; i < version_str.size(); ++i) {
    if (version_str[i] < '0' || version_str[i] > '9') {
      return false;
    }
  }
  version_ = static_cast<int>(String2Uint64(version_str));
  return true;
}


std::string InclusionSpec::StripVersionLine(const std::string &spec) const {
  // Remove everything up to and including the version line
  std::vector<std::string> lines = SplitString(spec, '\n');
  std::string result;
  bool past_version = false;

  for (size_t i = 0; i < lines.size(); ++i) {
    if (!past_version) {
      std::string trimmed = Trim(lines[i]);
      if (!trimmed.empty() && trimmed[0] != '#'
          && trimmed.substr(0, 7) == "version") {
        past_version = true;
        continue;
      }
    }
    if (past_version) {
      result += lines[i] + "\n";
    }
  }
  return result;
}
