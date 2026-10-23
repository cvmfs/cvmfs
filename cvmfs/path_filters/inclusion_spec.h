/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PATH_FILTERS_INCLUSION_SPEC_H_
#define CVMFS_PATH_FILTERS_INCLUSION_SPEC_H_

#include <string>

#include "path_filters/relaxed_path_filter.h"

namespace catalog {

/**
 * InclusionSpec implements a versioned inclusion specification for partial
 * replication of CVMFS Stratum-1 servers.
 *
 * The spec file format:
 *   version 1
 *   # comment
 *   /path/to/include
 *   !/path/to/exclude
 *
 * Paths listed (without !) are INCLUDED in object replication. Paths prefixed
 * with ! are excluded (negating a parent inclusion). Anything not covered by
 * an inclusion rule is excluded. All catalogs are always replicated; only data
 * object downloads are skipped for excluded paths.
 *
 * Internally, this wraps a RelaxedPathFilter: positive rules in the spec mean
 * "include" (replicate objects, also covering parent and sub paths), while !
 * rules mean "exclude" (skip objects). IsExcluded() returns the negation of the
 * filter match.
 *
 * Only paths that correspond to nested catalog transition points are
 * meaningful. Paths that don't align with catalog boundaries will trigger
 * warnings during snapshot and be rounded up to the enclosing catalog.
 */
class InclusionSpec {
 public:
  static const int kCurrentVersion = 1;

  InclusionSpec();
  ~InclusionSpec();

  /**
   * Creates an InclusionSpec from a file on disk.
   * Returns NULL on failure (file not found, parse error).
   * Caller takes ownership.
   */
  static InclusionSpec *Create(const std::string &spec_path);

  /**
   * Parse a spec string. Returns true on success.
   */
  bool Parse(const std::string &spec);

  /**
   * Returns true if the given path should have its data objects
   * EXCLUDED from replication (i.e., objects should NOT be downloaded).
   *
   * The root path "" or "/" is never excluded.
   */
  bool IsExcluded(const std::string &path) const;

  /**
   * Returns true if parsing succeeded and version is supported.
   */
  bool IsValid() const { return valid_; }

  /**
   * Returns the parsed version number, or -1 if not parsed.
   */
  int version() const { return version_; }

  /**
   * Returns the original spec content for upload to backend storage.
   */
  const std::string &content() const { return content_; }

 private:
  bool ParseVersion(const std::string &line);
  std::string StripVersionLine(const std::string &spec) const;

  bool valid_;
  int version_;
  std::string content_;
  RelaxedPathFilter filter_;
};

}  // namespace catalog

#endif  // CVMFS_PATH_FILTERS_INCLUSION_SPEC_H_
