/**
 * This file is part of the CernVM File System
 */

#define __STDC_FORMAT_MACROS

#include "sync_union.h"
#include "sync_union_tarball.h"

#include <set>
#include <string>

namespace publish {
/*
 * Still have to underastand how the madiator, scratch, read and union path
 * works together.
 */
SyncUnionTarball::SlyncUnionTarball(SyncMediator *mediator,
                                    const std::string &tarball_path,
                                    const std::string &base_directory)
    : tarball_path_(tarball_path), base_directory_(base_directory) {}

/*
 * We traferse the tar and then we keep track (the path inside a set) of each
 * .tar we find, then we go back and repeat the procedure. Should we be worry by
 * tar-bombs? Maybe, we will think about it later.
 */
bool SyncUnionTarball::Initialize() {}

std::set<std::string> untarPath(const std::string &tarball_path,
                                const std::string &base_directory);

}  // namespace publish
