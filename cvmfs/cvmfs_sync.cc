/**
 * This file is part of the CernVM File System
 *
 * This tool figures out the changes made to a cvmfs repository by means
 * of a union file system mounted on top of a cvmfs volume.
 * We take all three volumes (namely union, overlay and repository) into
 * account to sync the changes back into the repository.
 *
 * On the repository side we have a catalogs directory that mimicks the
 * shadow directory structure and stores compressed and uncompressed
 * versions of all catalogs.  The raw data are stored in the data
 * subdirectory in zlib-compressed form.  They are named with their SHA-1
 * hash of the compressed file (like in CVMFS client cache, but with a
 * 2-level cache hierarchy).  Symlinks from the catalog directory to the
 * data directory form the connection. If necessary, add a .htaccess file
 * to allow Apache to follow the symlinks.
 */

#define _FILE_OFFSET_BITS 64

#include "cvmfs_config.h"
#include "cvmfs_sync.h"

#include <cstdlib>

#include <string>

#include "platform.h"
#include "sync_union.h"
#include "SyncMediator.h"
#include "catalog_mgr_rw.h"
#include "util.h"
#include "logging.h"
#include "monitor.h"

using namespace std;  // NOLINT

static void Usage() {
  LogCvmfs(kLogCvmfs, kLogStdout,
    "CernVM-FS push changes from scratch area back to repository\n"
    "Version %s\n"
    "Usage (normally called from cvmfs_server):\n"
    "  cvmfs_sync -u <union volume> -s <scratch directory> -c <r/o volume>\n"
    "             -r <repository store>\n"
    "             [-p(rint change set)] [-d(ry run)] [-m(ucatalogs)\n"
    "             [EXPERIMENTAL: -x paths_out (pipe)  -y hashes_in (pipe) -z (compress locally)]\n\n",
    VERSION);
}


bool ParseParams(int argc, char **argv, SyncParameters *params) {
	if ((argc < 2) || (string(argv[1]) == "-h") || (string(argv[1]) == "--help")
      || (string(argv[1]) == "-v") || (string(argv[1]) == "--version"))
  {
		Usage();
		return false;
	}

	// set defaults
	params->print_changeset = false;
	params->dry_run = false;
	params->mucatalogs = false;
  params->process_locally = false;

	// Parse the parameters
	char c;
	while ((c = getopt(argc, argv, "u:s:c:r:pdmx:y:z")) != -1) {
		switch (c) {
      // Directories
      case 'u':
        params->dir_union = MakeCanonicalPath(optarg);
        break;
      case 's':
        params->dir_scratch = MakeCanonicalPath(optarg);
        break;
      case 'c':
        params->dir_rdonly = MakeCanonicalPath(optarg);
        break;
      case 'r': {
        const string path = MakeCanonicalPath(optarg);
        params->dir_data = path + "/data";
        params->dir_catalogs = path + "/catalogs";
        break;
      }
      case 'x':
        params->paths_out = optarg;
        break;
      case 'y':
        params->hashes_in = optarg;
        break;

      // Switches
      case 'p':
        params->print_changeset = true;
        break;
      case 'd':
        params->dry_run = true;
        break;
      case 'm':
        params->mucatalogs = true;
        break;
      case 'z':
        params->process_locally = true;
        break;

      case '?':
      default:
        Usage();
        return false;
		}
	}

	return true;
}


bool CheckParams(SyncParameters *p) {
  if (!DirectoryExists(p->dir_scratch)) {
    PrintError("overlay (copy on write) directory does not exist");
    return false;
  }

  if (not DirectoryExists(p->dir_union)) {
    PrintError("union volume does not exist");
    return false;
  }

	if (not DirectoryExists(p->dir_rdonly)) {
		PrintError("cvmfs read/only repository does not exist");
		return false;
	}

  if (not DirectoryExists(p->dir_data)) {
    PrintError("data store directory does not exist");
    return false;
  }

  if (not DirectoryExists(p->dir_catalogs)) {
    PrintError("catalog store directory does not exist");
    return false;
  }

	return true;
}


// TODO: special option to create new empty repository
int main(int argc, char **argv) {
	SyncParameters params;

  umask(022);

  if (!monitor::Init(".", false)) {
		PrintError("Failed to init watchdog");
		return 1;
	}
	monitor::Spawn();

	// Initialization
	if (!ParseParams(argc, argv, &params)) return 1;
	if (!CheckParams(&params)) return 2;
	if (!MakeCacheDirectories(params.dir_data, 0755)) {
		PrintError("could not initialize data store");
		return 3;
	}

  catalog::WritableCatalogManager catalog_manager(params.dir_catalogs,
                                                  params.dir_data);
  publish::SyncMediator mediator(&catalog_manager, &params);
  publish::SyncUnionAufs sync(&mediator, params.dir_rdonly, params.dir_union,
                              params.dir_scratch);

	if (!sync.Traverse()) {
		PrintError("something went wrong during sync");
		return 4;
	}

  monitor::Fini();

	return 0;
}
