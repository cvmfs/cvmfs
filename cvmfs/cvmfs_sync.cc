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
#include "SyncUnionAufs.h"
#include "SyncMediator.h"
#include "WritableCatalogManager.h"
#include "util.h"
#include "logging.h"
#include "monitor.h"

using namespace std;
using namespace cvmfs;


static void Usage() {
  LogCvmfs(kLogCvmfs, kLogStdout,
    "CernVM-FS push changes from scratch area back to repository\n"
    "Version %s\n"
    "Usage:\n"
    "  cvmfs_sync -u <union volume> -s <scratch directory> -c <r/o volume>\n"
    "             -r <repository store>\n"
    "             [-p(rint change set)] [-d(ry run)] [-m(ucatalogs)\n\n"
    "Make sure that a 'data' and a 'catalogs' subdirectory exist in your repository store.\n"
    "Also, your webserver must be able to follow symlinks in the catalogs subdirectory.\n"
    "For Apache, you can add 'Options +FollowSymLinks' to a '.htaccess' file.\n\n", VERSION);
}


bool ParseParameters(int argc, char **argv, SyncParameters *params) {
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

	// Parse the parameters
	char c;
	while ((c = getopt(argc, argv, "u:s:c:r:pdm")) != -1) {
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

      case '?':
      default:
        Usage();
        return false;
		}
	}

	return true;
}

// TODO monitor::fini
bool initWatchdog() {
	umask(022);

	if (!monitor::Init(".", false)) {
		PrintError("Failed to init watchdog");
		return false;
	}
	monitor::Spawn();

	return true;
}

bool doSanityChecks(SyncParameters *p) {
  if (not DirectoryExists(p->dir_scratch)) {
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

bool createCacheDir(SyncParameters *p) {
	if (!MakeCacheDirectories(p->dir_data, 0755)) {
		PrintError("could not initialize data store");
		return false;
	}

	return true;
}

catalog::WritableCatalogManager* createWritableCatalogManager(const SyncParameters &p) {
  return new catalog::WritableCatalogManager(MakeCanonicalPath(p.dir_catalogs),
                                             MakeCanonicalPath(p.dir_data));
}

SyncMediator* createSyncMediator(catalog::WritableCatalogManager* catalogManager,
                                 const SyncParameters &p) {
  return new SyncMediator(catalogManager,
                          MakeCanonicalPath(p.dir_data),
                          p.dry_run,
                          p.print_changeset);
}

SyncUnion* createSynchronisationEngine(SyncMediator* mediator,
                                       const SyncParameters &p) {
  return new SyncUnionAufs(mediator,
                           MakeCanonicalPath(p.dir_rdonly),
                           MakeCanonicalPath(p.dir_union),
                           MakeCanonicalPath(p.dir_scratch));
}

int main(int argc, char **argv) {
	SyncParameters parameters;

	// do some initialization
	if (not ParseParameters(argc, argv, &parameters)) return 1;
	if (not initWatchdog()) return 1;
	if (not doSanityChecks(&parameters)) return 2;
	if (not createCacheDir(&parameters)) return 3;

	// create worker objects
  catalog::WritableCatalogManager *catalogManager = createWritableCatalogManager(parameters);
  SyncMediator *mediator = createSyncMediator(catalogManager, parameters);
  SyncUnion *sync = createSynchronisationEngine(mediator, parameters);

	// sync
	if (not sync->DoYourMagic()) {
		PrintError("something went wrong during sync");
		return 4;
	}

	// clean up
	delete mediator;
	delete catalogManager;
  delete sync;

  std::cout << "done" << std::endl;

	return 0;
}
