/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PATH_FILTERS_RELAXED_PATH_FILTER_H_
#define CVMFS_PATH_FILTERS_RELAXED_PATH_FILTER_H_

#include <string>

#include "dirtab.h"

namespace catalog {

/**
 * A RelaxedPathFilter works similar to a Dirtab but it matches more generously:
 * in addition to the actual paths it represents, all parent paths are matched.
 * Sub paths of given paths are matched, too.  In contrast to Dirtab, trailing
 * slashes of path specifications are ignored.
 *
 * For instance:
 *   /software/releases
 *   ! /software/releases/misc
 *   ! /software/releases/experimental/misc
 *
 * Results in the following positive matches
 *   /software, /software/releases, /software/releases/v1,
 *   /software/releases/experimental
 * and in the following non-matches
 *   /software/apps, /software/releases/misc, /software/releases/misc/external,
 *   /software/releases/experimental/misc,
 *   /software/releases/experimental/misc/foo
 *
 * It is used by cvmfs_preload as a specification of a partial subtree for
 * synchronization with a cache directory.
 */
class RelaxedPathFilter : public Dirtab {
 public:
  static RelaxedPathFilter* Create(const std::string &dirtab_path);
  virtual bool Parse(const std::string &dirtab);
  virtual bool IsMatching(const std::string &path) const;
  virtual bool IsOpposing(const std::string &path) const;

 protected:
  virtual bool ParsePathspec(const std::string &pathspec_str, bool negation);

 private:
  /**
   * Represents the entries in the provided dirtab file without parent paths.
   * It is necessary to match sub paths against the provided dirtab.
   */
  Dirtab exact_dirtab_;
};

}  // namespace catalog

#endif  // CVMFS_PATH_FILTERS_RELAXED_PATH_FILTER_H_
