/**
 * \file cvmfs_sync.cc
 *
 * This tool makes the changes to a repository based on the cvmfsflt
 * kernel module log.
 * We call the user's working directoy "shadow directory".  This shadow 
 * directory is synchronized with a CVMFS2 repository.  The .cvmfscatalog
 * magic file is translated into nested catalogs.
 *
 * On the repository side we have a catalogs directory that mimicks the
 * shadow directory structure and stores compressed and uncompressed
 * versions of all catalogs.  The raw data are stored in the data 
 * subdirectory in zlib-compressed form.  They are named with their SHA1
 * hash of the compressed file (like in CVMFS client cache, but with a 
 * 2-level cache hierarchy).  Symlinks from the catalog directory to the 
 * data directory form the connection. If necessary, add a .htaccess file 
 * to allow Apache to follow the symlinks.
 *
 * Developed by Jakob Blomer 2010 at CERN
 * jakob.blomer@cern.ch
 */


#define _FILE_OFFSET_BITS 64

#include "cvmfs_config.h"

#include "cvmfs_sync_aufs.h"
#include "cvmfs_sync.h"

#include <string>
#include <iostream>
#include <sstream>
#include <set>

#include "util.h"
#include "monitor.h"

//#include "cvmfs_sync_catalog.h"

#include "compat.h"

using namespace std;
using namespace cvmfs;

static void usage() {
   cout << "CernVM-FS sync shadow tree with repository" << endl;
   cout << "Usage: cvmfs_sync -s <union volume> -o <overlay directory> -c <cvmfs mounted volume> -r <repository store>" << endl
        << "                  [-p(rint change set)] [-d(ry run)] [-i <immutable dir(,dir)*>]" << endl 
        << "                  [-k(ey file)] [-z (lazy attach of catalogs)] [-b(ookkeeping of dirty catalogs)]" << endl
        << "                  [-t <threads>] [-m(ucatalogs)]" << endl << endl
        << "Make sure that a 'data' and a 'catalogs' subdirectory exist in your repository store." << endl
        << "Also, your webserver must be able to follow symlinks in the catalogs subdirectory." << endl
        << "For Apache, you can add 'Options +FollowSymLinks' to a '.htaccess' file." 
        << endl << endl;
}

bool parseParameters(int argc, char **argv, SyncParameters *p) {
	// print some help if needed
	if ((argc < 2) || (string(argv[1]) == "-h") || (string(argv[1]) == "--help") ||
		(string(argv[1]) == "-v") || (string(argv[1]) == "--version")){
		usage();
		return false;
	}
	
	// set defaults
	p->print_changeset = false;
	p->dry_run = false;
	p->lazy_attach = false;
	p->sync_threads = 0;
	p->mucatalogs = false;

	// read the parameters
	char c;
	while ((c = getopt(argc, argv, "s:o:c:r:pdi:k:zbt:m")) != -1) {
		switch (c) {

		// directories
		case 's':
			p->dir_shadow = canonical_path(optarg);
			break;
		case 'c':
			p->dir_cvmfs = canonical_path(optarg);
			break;
		case 'r': {
			const string path = canonical_path(optarg);
			p->dir_data = path + "/data";
			p->dir_catalogs = path + "/catalogs";
			break;
		}
		case 'o':
			p->dir_overlay = canonical_path(optarg);
			break;

		// switches
		case 'p':
			p->print_changeset = true;
			break;
		case 'd':
			p->dry_run = true;
			break;
		case 'z':
			p->lazy_attach = true;
			break;
		case 'm':
			p->mucatalogs = true;
			break;

		// misc
		case 'i': {
			char *token = strtok(optarg, ",");
			while (token != NULL) {
				p->immutables.insert(canonical_path(token));
				token = strtok(NULL, ",");
			}
			break;
		}
		case 'k':
			p->keyfile = optarg;
			break;
		case 't':
			p->sync_threads = atoi(optarg);
			break;
		case '?':
		default:
			usage();
			return false;
		}
	}
	
	return true;
}

bool initWatchdog() {
	umask(022);

	if (!monitor::init(".", false)) {
		printError("Failed to init watchdog");
		return false;
	}
	monitor::spawn();
	
	return true;
}

bool doSanityChecks(SyncParameters *p) {
	if (not directory_exists(p->dir_overlay)) {
		printError("overlay (copy on write) directory does not exist");
		return false;
	}
	
	if (not directory_exists(p->dir_shadow)) {
		printError("shadow directory does not exist");
		return false;
	}

	if (not directory_exists(p->dir_cvmfs)) {
		printError("mounted cvmfs repository does not exist");
		return false;
	}
	
	if (not directory_exists(p->dir_data)) {
		printError("data store directory does not exist");
		return false;
	}
	
	if (not directory_exists(p->dir_catalogs)) {
		printError("catalog store directory does not exist");
		return false;
	}
	
	return true;
}

bool createCacheDir(SyncParameters *p) {
	if (!make_cache_dir(p->dir_data, 0755)) {
		printError("could not initialize data store");
		return false;
	}
	
	return true;
} 

int main(int argc, char **argv) {
	SyncParameters parameters;
	
	// do some initialization
	if (not parseParameters(argc, argv, &parameters)) return 1;
	if (not initWatchdog()) return 1;
	if (not doSanityChecks(&parameters)) return 2;
	if (not createCacheDir(&parameters)) return 3;
	
	// create worker objects
	CatalogHandler *catalogHandler = new CatalogHandler(&parameters);
	SyncMediator *mediator = new SyncMediator(catalogHandler, &parameters);
	SyncAufs1::initialize(mediator, &parameters);
	
	// sync
	if (not cvmfs::UnionSync::sharedInstance()->doYourMagic()) {
		printError("something went wrong during sync");
		return 4;
	}
	
	// clean up
	UnionSync::sharedInstance()->fini();
	delete mediator;
	delete catalogHandler;
	monitor::fini();

	return 0;
}
