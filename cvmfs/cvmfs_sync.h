/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SYNC_H_
#define CVMFS_SYNC_H_

#include <string>
#include "upload.h"

struct SyncParameters {
  SyncParameters() { 
    forklift = NULL; 
    print_changeset = false;
    dry_run = false;
    mucatalogs = false;
    process_locally = false;
    new_repository = false;
  }
  
	std::string dir_union;
  std::string dir_scratch;
	std::string dir_rdonly;
  std::string dir_temp;
  std::string base_hash;
  std::string stratum0;
  std::string paths_out;
  std::string hashes_in;
  upload::Forklift *forklift;
	bool print_changeset;
	bool dry_run;
	bool mucatalogs;
  bool process_locally;
  bool new_repository;
};

#endif  // CVMFS_SYNC_H_
