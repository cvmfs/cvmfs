/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SYNC_H_
#define CVMFS_SYNC_H_

#include <string>

typedef struct {
	std::string dir_union;
  std::string dir_scratch;
	std::string dir_rdonly;
	std::string dir_data;
	std::string dir_catalogs;
  std::string paths_out;
  std::string hashes_in;
	bool print_changeset;
	bool dry_run;
	bool mucatalogs;
  bool process_locally;
  bool new_repository;
} SyncParameters;

#endif  // CVMFS_SYNC_H_
