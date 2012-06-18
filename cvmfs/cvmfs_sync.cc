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

#include <fcntl.h>

#include <cstdlib>
#include <cstdio>

#include <string>

#include "platform.h"
#include "sync_union.h"
#include "sync_mediator.h"
#include "catalog_mgr_rw.h"
#include "util.h"
#include "logging.h"
#include "monitor.h"
#include "download.h"
#include "manifest.h"

using namespace std;  // NOLINT

static void Usage() {
  LogCvmfs(kLogCvmfs, kLogStdout,
    "CernVM-FS push changes from scratch area back to repository\n"
    "Version %s\n"
    "Usage (normally called from cvmfs_server):\n"
    "  cvmfs_sync -u <union volume> -s <scratch directory> -c <r/o volume>\n"
    "             -t <temporary storage> -b <base hash> -r <upstream storage>\n"
    "             -w <stratum 0 base url> -o <manifest output>\n"
    "             -n(new, requires only -t, -u, and -o)\n"
    "             [-p(rint change set)] [-d(ry run)] [-m(ucatalogs)\n"
    "             [EXPERIMENTAL: -n new -x paths_out (pipe)  -y hashes_in (pipe) -z (compress locally)]\n\n"
    "  Upstream storage might be:\n"
    "    local:<local path>\n"
    "    pipe:<pipe for the cvmfs distributed backend>\n\n",       
    VERSION);
}


bool ParseParams(int argc, char **argv, SyncParameters *params) {
	if ((argc < 2) || (string(argv[1]) == "-h") || (string(argv[1]) == "--help")
      || (string(argv[1]) == "-v") || (string(argv[1]) == "--version"))
  {
		Usage();
		return false;
	}

	// Parse the parameters
	char c;
	while ((c = getopt(argc, argv, "u:s:c:t:b:r:w:pdmno:x:y:z")) != -1) {
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
      case 't': {
        const string path = MakeCanonicalPath(optarg);
        params->dir_temp = path;
        break;
      }
      case 'b':
        params->base_hash = optarg;
        break;
      case 'r':
        params->forklift = upload::CreateForklift(optarg);
        if (!params->forklift) {
          Usage();
          return false;
        }
        break;
      case 'o':
        params->manifest_path = optarg;
        break;
      case 'w':
        params->stratum0 = optarg;
        break;
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
      case 'n':
        params->new_repository = true;
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
  if (!p->new_repository) {
    if (!DirectoryExists(p->dir_scratch)) {
      PrintError("overlay (copy on write) directory does not exist");
      return false;
    }
    if (!DirectoryExists(p->dir_union)) {
      PrintError("union volume does not exist");
      return false;
    }
    if (!DirectoryExists(p->dir_rdonly)) {
      PrintError("cvmfs read/only repository does not exist");
      return false;
    }
    if (p->stratum0 == "") {
      PrintError("Stratum0 url missing");
      return false;
    }
  }
  
  if (p->manifest_path == "") {
    PrintError("manifest output required");
    return false;
  }
  if (!p->forklift) {
    PrintError("no upstream storage defined");
    return false;
  }
  if (!p->forklift->Connect()) {
    PrintError("failed to connect to upstream storage (" + 
               p->forklift->GetLastError() + ")");
    return false;
  }
  if (!DirectoryExists(p->dir_temp)) {
    PrintError("data store directory does not exist");
    return false;
  }

	return true;
}


int main(int argc, char **argv) {
	SyncParameters params;

  umask(022);
  
  int pid = fork();
  assert(pid >= 0);
  if (pid == 0) {
    return upload::MainLocalSpooler("spool", "digests", "/tmp/dest");
  }
  
  upload::Spooler *myspooler = new upload::Spooler("spool", "digests");
  bool retval = myspooler->Connect();
  printf("spooler connected: %d\n", retval);
  myspooler->Spool("/tmp/spool/bla", "", true);
  while (!myspooler->IsIdle()) {
    sleep(1);
  }
  
  delete myspooler;  
  return 0;

  if (!monitor::Init(".", false)) {
		PrintError("Failed to init watchdog");
		return 1;
	}

	// Initialization
	if (!ParseParams(argc, argv, &params)) return 1;
	if (!CheckParams(&params)) return 2;
   
  // Create a new root hash.  As a side effect, upload new files and catalogs.
  Manifest *manifest = NULL;
  if (params.new_repository) {
    manifest = 
      catalog::WritableCatalogManager::CreateRepository(params.dir_temp,
                                                        *params.forklift);
    if (!manifest) {
      PrintError("Failed to create new repository");
      return 1;
    }
  } else {
    monitor::Spawn();
    download::Init(1);
  
    catalog::WritableCatalogManager 
      catalog_manager(hash::Any(hash::kSha1, hash::HexPtr(params.base_hash)),
                      params.stratum0, params.dir_temp, params.forklift);
    publish::SyncMediator mediator(&catalog_manager, &params);
    publish::SyncUnionAufs sync(&mediator, params.dir_rdonly, params.dir_union,
                                params.dir_scratch);

    sync.Traverse();
    manifest = mediator.Commit();

    download::Fini();
    monitor::Fini();
    
    if (!manifest) {
      PrintError("something went wrong during sync");
      return 4;
    }
  }
  
  if (!manifest->Export(params.manifest_path)) {
    PrintError("Failed to create new repository");
    return 5;
  }
  delete manifest;

	return 0;
}
