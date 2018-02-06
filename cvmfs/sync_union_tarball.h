/**
 * This file is part of the CernVM File System
 *
 * This file defines a class which derives `SyncUnion` to
 * provide support for tarballs
 *
 */

#ifndef CVMFS_SYNC_UNION_TARBALL_H_
#define CVMFS_SYNC_UNION_TARBALL_H_

#include "sync_union.h"

#include <set>
#include <string>

namespace publish {

class SyncUnionTarball : public SyncUnion {
 public:
  SyncUnionTarball(SyncMediator *mediator, const std::string &tarball_path,
                   const std::string &base_directory);

  /*
   * Check that the tarball is actually valid and that can be open.
   * Similarly it creates the base directory or check that it has the ability to
   * write in it.
   * Finally it untar the tarball, recursively.
   */
  bool Initialize();

  /*
   * Simply create the SyncItem from the basedirectory opened
   */
  void Traverse();

 private:
  const std::string tarball_path_;
  const std::string base_directory_;
  std::set<std::string> to_recur_;

  /*
   * Actually untar the several elements in the tar inside the base directory,
   * it returns all the recursive tars find in this operation
   */
  std::set<std::string> untarPath(const std::string &tarball_path,
                                  const std::string &base_directory);

};  // class SyncUnionTarball

}  // namespace publish

#endif  // CVMFS_SYNC_UNION_TARBALL_H_
