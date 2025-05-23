/**
 * This file is part of the CernVM File System.
 */
#include "data_dir_mgmt.h"

#include <errno.h>
#include <stdio.h>

#include "helpers.h"
#include "shrinkwrap/fs_traversal_interface.h"
#include "util/posix.h"

/**
 * Method which recursively creates the .data subdirectories
 */
void PosixCheckDirStructure(std::string cur_path,
                            mode_t mode,
                            unsigned depth = 1) {
  std::string max_dir_name = std::string(kDigitsPerDirLevel, 'f');
  // Build current base path
  if (depth == 1) {
    bool res1 = MkdirDeep(cur_path.c_str(), mode);
    assert(res1);
  } else {
    int res = mkdir(cur_path.c_str(), mode);
    assert(res == 0 || errno == EEXIST);
  }
  // Build template for directory names:
  assert(kDigitsPerDirLevel <= 99);
  char hex[kDigitsPerDirLevel + 1];
  char dir_name_template[5];
  snprintf(dir_name_template,
           sizeof(dir_name_template),
           "%%%02ux",
           kDigitsPerDirLevel);
  // Go through all levels:
  for (; depth <= kDirLevels; depth++) {
    if (!DirectoryExists(cur_path + "/" + max_dir_name)) {
      // Directories in this level not yet fully created...
      for (unsigned int i = 0;
           i < (((unsigned int)1) << 4 * kDigitsPerDirLevel);
           i++) {
        // Go through directories 0^kDigitsPerDirLevel to f^kDigitsPerDirLevel
        snprintf(hex, sizeof(hex), dir_name_template, i);
        std::string this_path = cur_path + "/" + std::string(hex);
        int res = mkdir(this_path.c_str(), mode);
        assert(res == 0 || errno == EEXIST);
        // Once directory created: Prepare substructures
        PosixCheckDirStructure(this_path, mode, depth + 1);
      }
      break;
    } else {
      // Directories on this level fully created; check ./
      PosixCheckDirStructure(cur_path + "/" + max_dir_name, mode, depth + 1);
    }
  }
}

void InitializeDataDirectory(struct fs_traversal_context *ctx) {
  // NOTE(steuber): Can we do this in parallel?
  PosixCheckDirStructure(ctx->data, 0700);
}
