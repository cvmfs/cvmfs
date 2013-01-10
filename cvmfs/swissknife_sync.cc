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
#include "swissknife_sync.h"

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
#include "download.h"
#include "manifest.h"

using namespace std;  // NOLINT


bool CheckParams(SyncParameters *p) {
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

  if (p->manifest_path == "") {
    PrintError("manifest output required");
    return false;
  }
  if (!DirectoryExists(p->dir_temp)) {
    PrintError("data store directory does not exist");
    return false;
  }

	return true;
}


int swissknife::CommandCreate::Main(const swissknife::ArgumentList &args) {
  const string manifest_path = *args.find('o')->second;
  const string dir_temp = *args.find('t')->second;
  const string spooler_definition = *args.find('r')->second;
  if (args.find('l') != args.end()) {
    unsigned log_level =
      1 << (kLogLevel0 + String2Uint64(*args.find('l')->second));
    if (log_level > kLogNone) {
      swissknife::Usage();
      return 1;
    }
    SetLogVerbosity(static_cast<LogLevels>(log_level));
  }

  upload::Spooler *spooler = upload::MakeSpoolerEnsemble(spooler_definition);
  assert(spooler);

  // TODO: consider using the unique pointer to come in Github Pull Request 46
  manifest::Manifest *manifest =
    catalog::WritableCatalogManager::CreateRepository(dir_temp, spooler);
  if (!manifest) {
    PrintError("Failed to create new repository");
    return 1;
  }
  delete spooler;

  if (!manifest->Export(manifest_path)) {
    PrintError("Failed to create new repository");
    delete manifest;
    return 5;
  }
  delete manifest;

  return 0;
}


int swissknife::CommandUpload::Main(const swissknife::ArgumentList &args) {
  const string source = *args.find('i')->second;
  const string dest = *args.find('o')->second;
  const string spooler_definition = *args.find('r')->second;

  upload::Spooler *spooler = upload::MakeSpoolerEnsemble(spooler_definition);
  assert(spooler);
  spooler->SpoolCopy(source, dest);
  spooler->EndOfTransaction();
  spooler->WaitFor();
  if (spooler->num_errors() > 0) {
    LogCvmfs(kLogCatalog, kLogStderr, "failed to upload %s", source.c_str());
    return 1;
  }

  return 0;
}


int swissknife::CommandSync::Main(const swissknife::ArgumentList &args) {
	SyncParameters params;

  // Initialization
  params.dir_union = MakeCanonicalPath(*args.find('u')->second);
  params.dir_scratch = MakeCanonicalPath(*args.find('s')->second);
  params.dir_rdonly = MakeCanonicalPath(*args.find('c')->second);
  params.dir_temp = MakeCanonicalPath(*args.find('t')->second);
  params.base_hash = *args.find('b')->second;
  params.stratum0 = *args.find('w')->second;
  params.manifest_path = *args.find('o')->second;
  params.spooler_definition = *args.find('r')->second;

  if (args.find('x') != args.end()) params.print_changeset = true;
  if (args.find('y') != args.end()) params.dry_run = true;
  if (args.find('m') != args.end()) params.mucatalogs = true;
  if (args.find('z') != args.end()) {
    unsigned log_level =
    1 << (kLogLevel0 + String2Uint64(*args.find('z')->second));
    if (log_level > kLogNone) {
      swissknife::Usage();
      return 1;
    }
    SetLogVerbosity(static_cast<LogLevels>(log_level));
  }

	if (!CheckParams(&params)) return 2;

  // Start spooler
  params.spooler = upload::MakeSpoolerEnsemble(params.spooler_definition);
  download::Init(1);

  catalog::WritableCatalogManager
    catalog_manager(hash::Any(hash::kSha1, hash::HexPtr(params.base_hash)),
                    params.stratum0, params.dir_temp, params.spooler);
  publish::SyncMediator mediator(&catalog_manager, &params);
  publish::SyncUnionAufs sync(&mediator, params.dir_rdonly, params.dir_union,
                              params.dir_scratch);

  sync.Traverse();
  // TODO: consider using the unique pointer to come in Github Pull Request 46
  manifest::Manifest *manifest = mediator.Commit();

  download::Fini();

  LogCvmfs(kLogCvmfs, kLogStdout, "Exporting repository manifest");
  if (!manifest) {
    PrintError("something went wrong during sync");
    return 4;
  }

  delete params.spooler;

  if (!manifest->Export(params.manifest_path)) {
    PrintError("Failed to create new repository");
    delete manifest;
    return 5;
  }
  delete manifest;

  return 0;
}
