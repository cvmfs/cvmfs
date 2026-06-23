/**
 * This file is part of the CernVM File System.
 *
 * Implementation of the overlay swissknife command that merges multiple
 * CVMFS subdirectory catalogs using overlay semantics (similar to OverlayFS)
 * and publishes the result as a repository subdirectory.
 */

#include "swissknife_overlay.h"

#include <fcntl.h>
#include <inttypes.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <ctime>
#include <map>
#include <string>
#include <vector>

#include "catalog.h"
#include "catalog_mgr_rw.h"
#include "catalog_rw.h"
#include "catalog_sql.h"
#include "compression/compression.h"
#include "crypto/hash.h"
#include "directory_entry.h"
#include "ingestion/ingestion_source.h"
#include "json_document.h"
#include "manifest.h"
#include "network/download.h"
#include "network/sink_path.h"
#include "repository_tag.h"
#include "shortstring.h"
#include "statistics.h"
#include "upload.h"
#include "upload_spooler_definition.h"
#include "upload_spooler_result.h"
#include "util/logging.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/string.h"
#include "xattr.h"

using namespace std;  // NOLINT

namespace swissknife {

ParameterList CommandOverlay::GetParams() const {
  ParameterList r;
  // Publish workflow parameters (same convention as ingest/sync)
  r.push_back(Parameter::Mandatory('r', "upstream storage definition"));
  r.push_back(Parameter::Mandatory('w', "stratum 0 URL"));
  r.push_back(Parameter::Mandatory('t', "temporary directory"));
  r.push_back(Parameter::Mandatory('o', "manifest output path"));
  r.push_back(Parameter::Mandatory('b', "base hash of current root catalog"));
  r.push_back(Parameter::Mandatory('K', "public key path"));
  r.push_back(Parameter::Mandatory('N', "repository name"));
  // Gateway publishing (same convention as ingest): when the upstream is a
  // repository gateway these carry the lease so the merge can be committed
  // without a local FUSE mount (mountless publishing).
  r.push_back(Parameter::Optional('P', "session token file (gateway)"));
  r.push_back(Parameter::Optional('H', "gateway key file"));

  // Overlay-specific parameters
  r.push_back(Parameter::Mandatory('l', "comma-separated layer paths "
               "(bottom-to-top order)"));
  r.push_back(Parameter::Mandatory('d', "destination subdirectory path in "
               "repository for the merged overlay"));
  r.push_back(Parameter::Optional('e', "hash algorithm (default: sha1)"));
  r.push_back(Parameter::Optional('Z', "compression algorithm "
               "(default: zlib)"));
  r.push_back(Parameter::Optional('@', "proxy URL"));
  r.push_back(Parameter::Switch('L', "follow HTTP redirects"));
  r.push_back(Parameter::Optional('c', "OCI image config JSON file path "
               "(when provided, Singularity .singularity.d dotfiles are "
               "injected into the merged overlay)"));
  r.push_back(Parameter::Switch('S', "skip Singularity dotfile injection "
               "even when an OCI config is provided"));
  return r;
}


bool CommandOverlay::IsWhiteoutFile(const string &name) {
  return HasPrefix(name, ".wh.", false) && !IsOpaqueMarker(name);
}


string CommandOverlay::GetWhiteoutTarget(const string &name) {
  // ".wh." is 4 characters
  if (name.length() <= 4) return "";
  return name.substr(4);
}


bool CommandOverlay::IsOpaqueMarker(const string &name) {
  return name == ".wh..wh..opq";
}


bool CommandOverlay::ReadCatalogEntries(
    catalog::Catalog *catalog,
    const string &catalog_root_path,
    const string &relative_prefix,
    const string &repo_base,
    const string &temp_dir,
    map<string, OverlayEntry> *entries) {
  // List entries at this path in the catalog
  catalog::DirectoryEntryList listing;
  const PathString ps_path(catalog_root_path.data(),
                           catalog_root_path.length());
  const bool has_entries = catalog->ListingPath(ps_path, &listing);

  if (!has_entries && catalog_root_path != catalog->mountpoint().ToString()) {
    // No entries at this path - it might be a file, not a directory
    return true;
  }

  for (size_t i = 0; i < listing.size(); ++i) {
    const catalog::DirectoryEntry &dirent = listing[i];
    const string name = dirent.name().ToString();

    // Skip CVMFS bookkeeping files — they are internal metadata and must not
    // be carried over into the merged overlay.  PublishMergedEntries() will
    // create its own .cvmfscatalog marker for the destination catalog.
    if (name == ".cvmfscatalog" || name == ".cvmfsdirtab"
        || name == ".cvmfsautocatalog") {
      continue;
    }

    const string child_catalog_path =
        catalog_root_path.empty() ? "/" + name : catalog_root_path + "/" + name;
    const string child_relative =
        relative_prefix.empty() ? name : relative_prefix + "/" + name;

    OverlayEntry oe;
    oe.entry = dirent;
    oe.path = child_relative;
    oe.parent = relative_prefix;
    oe.is_whiteout = IsWhiteoutFile(name);
    oe.is_opaque_dir = false;

    // Look up xattrs for this entry
    XattrList xattrs;
    const PathString ps_child(child_catalog_path.data(),
                              child_catalog_path.length());
    catalog->LookupXattrsPath(ps_child, &xattrs);
    oe.xattrs = xattrs;

    (*entries)[child_relative] = oe;

    if (dirent.IsDirectory()) {
      if (dirent.IsNestedCatalogMountpoint() && !repo_base.empty()) {
        // Load the nested catalog and recurse into it
        shash::Any nested_hash;
        uint64_t nested_size;
        if (!catalog->FindNested(ps_child, &nested_hash, &nested_size)) {
          LogCvmfs(kLogCvmfs, kLogStderr,
                   "Failed to find nested catalog hash for %s",
                   child_catalog_path.c_str());
          return false;
        }

        catalog::Catalog *nested = LoadCatalogForPath(
            repo_base, child_catalog_path, temp_dir, nested_hash);
        if (nested == NULL) {
          LogCvmfs(kLogCvmfs, kLogStderr,
                   "Failed to load nested catalog for %s",
                   child_catalog_path.c_str());
          return false;
        }

        // Check for opaque marker in the nested catalog root
        catalog::DirectoryEntryList sub_listing;
        nested->ListingPath(ps_child, &sub_listing);
        for (size_t j = 0; j < sub_listing.size(); ++j) {
          if (IsOpaqueMarker(sub_listing[j].name().ToString())) {
            (*entries)[child_relative].is_opaque_dir = true;
            break;
          }
        }

        if (!ReadCatalogEntries(nested, child_catalog_path,
                                child_relative, repo_base, temp_dir, entries)) {
          delete nested;
          return false;
        }
        delete nested;
      } else if (!dirent.IsNestedCatalogMountpoint()) {
        // Regular directory — check for opaque marker among children
        catalog::DirectoryEntryList sub_listing;
        catalog->ListingPath(ps_child, &sub_listing);
        for (size_t j = 0; j < sub_listing.size(); ++j) {
          if (IsOpaqueMarker(sub_listing[j].name().ToString())) {
            (*entries)[child_relative].is_opaque_dir = true;
            break;
          }
        }

        if (!ReadCatalogEntries(catalog, child_catalog_path,
                                child_relative, repo_base, temp_dir, entries)) {
          return false;
        }
      }
      // else: nested catalog mountpoint but no repo_base — skip (e.g. cache)
    }
  }

  return true;
}


void CommandOverlay::MergeLayer(
    const map<string, OverlayEntry> &layer_entries,
    map<string, OverlayEntry> *merged) const {
  // First pass: collect whiteouts and opaque directories
  vector<string> whiteout_targets;
  vector<string> opaque_dirs;

  for (map<string, OverlayEntry>::const_iterator it = layer_entries.begin();
       it != layer_entries.end(); ++it) {
    const OverlayEntry &oe = it->second;
    const string &path = it->first;

    if (oe.is_whiteout) {
      // Whiteout: mark the target for deletion from lower layers
      const string target_name = GetWhiteoutTarget(
          GetFileName(path));
      const string target_path =
          oe.parent.empty() ? target_name : oe.parent + "/" + target_name;
      whiteout_targets.push_back(target_path);
      continue;
    }

    if (IsOpaqueMarker(GetFileName(path))) {
      // Don't add the opaque marker itself to the merged output
      continue;
    }

    if (oe.is_opaque_dir) {
      opaque_dirs.push_back(path);
    }
  }

  // Apply opaque directory semantics: remove all entries from lower layers
  // that are under opaque directories
  for (size_t i = 0; i < opaque_dirs.size(); ++i) {
    const string &opaque_path = opaque_dirs[i];
    const string prefix = opaque_path + "/";

    // Remove children of this directory from merged (lower layer entries)
    vector<string> to_remove;
    for (map<string, OverlayEntry>::iterator it = merged->begin();
         it != merged->end(); ++it) {
      if (HasPrefix(it->first, prefix, false)) {
        to_remove.push_back(it->first);
      }
    }
    for (size_t j = 0; j < to_remove.size(); ++j) {
      merged->erase(to_remove[j]);
    }
  }

  // Apply whiteout semantics: remove targeted entries and their children
  for (size_t i = 0; i < whiteout_targets.size(); ++i) {
    const string &target = whiteout_targets[i];
    const string prefix = target + "/";

    // Remove the target entry itself
    merged->erase(target);

    // Remove all children of the target
    vector<string> to_remove;
    for (map<string, OverlayEntry>::iterator it = merged->begin();
         it != merged->end(); ++it) {
      if (HasPrefix(it->first, prefix, false)) {
        to_remove.push_back(it->first);
      }
    }
    for (size_t j = 0; j < to_remove.size(); ++j) {
      merged->erase(to_remove[j]);
    }
  }

  // Second pass: add/override entries from this layer
  for (map<string, OverlayEntry>::const_iterator it = layer_entries.begin();
       it != layer_entries.end(); ++it) {
    const OverlayEntry &oe = it->second;
    const string &path = it->first;

    // Skip whiteout files and opaque markers - they are control files
    if (oe.is_whiteout || IsOpaqueMarker(GetFileName(path))) {
      continue;
    }

    // Upper layer overrides lower layer for the same path
    (*merged)[path] = oe;
  }
}


// ---------------------------------------------------------------------------
// Singularity dotfile generation
// ---------------------------------------------------------------------------

// Static file contents for /.singularity.d — these mirror the Go
// constants in singularity/dotfiles.go (originally from Sylabs/Singularity).

static const char *const kSingExec =
    "#!/bin/sh\n"
    "for script in /.singularity.d/env/*.sh; do\n"
    "    if [ -f \"$script\" ]; then\n"
    "        . \"$script\"\n"
    "    fi\n"
    "done\n"
    "exec \"$@\"\n";

static const char *const kSingRun =
    "#!/bin/sh\n"
    "for script in /.singularity.d/env/*.sh; do\n"
    "    if [ -f \"$script\" ]; then\n"
    "        . \"$script\"\n"
    "    fi\n"
    "done\n"
    "if test -n \"${SINGULARITY_APPNAME:-}\"; then\n"
    "    if test -x \"/scif/apps/${SINGULARITY_APPNAME:-}/scif/runscript\"; then\n"
    "        exec \"/scif/apps/${SINGULARITY_APPNAME:-}/scif/runscript\" \"$@\"\n"
    "    else\n"
    "        echo \"No Singularity runscript for contained app: ${SINGULARITY_APPNAME:-}\"\n"
    "        exit 1\n"
    "    fi\n"
    "elif test -x \"/.singularity.d/runscript\"; then\n"
    "    exec \"/.singularity.d/runscript\" \"$@\"\n"
    "else\n"
    "    echo \"No Singularity runscript found, executing /bin/sh\"\n"
    "    exec /bin/sh \"$@\"\n"
    "fi\n";

static const char *const kSingShell =
    "#!/bin/sh\n"
    "for script in /.singularity.d/env/*.sh; do\n"
    "    if [ -f \"$script\" ]; then\n"
    "        . \"$script\"\n"
    "    fi\n"
    "done\n"
    "if test -n \"$SINGULARITY_SHELL\" -a -x \"$SINGULARITY_SHELL\"; then\n"
    "    exec $SINGULARITY_SHELL \"$@\"\n"
    "    echo \"ERROR: Failed running shell as defined by '\\$SINGULARITY_SHELL'\" 1>&2\n"
    "    exit 1\n"
    "elif test -x /bin/bash; then\n"
    "    SHELL=/bin/bash\n"
    "    PS1=\"Singularity $SINGULARITY_NAME:\\w> \"\n"
    "    export SHELL PS1\n"
    "    exec /bin/bash --norc \"$@\"\n"
    "elif test -x /bin/sh; then\n"
    "    SHELL=/bin/sh\n"
    "    export SHELL\n"
    "    exec /bin/sh \"$@\"\n"
    "else\n"
    "    echo \"ERROR: /bin/sh does not exist in container\" 1>&2\n"
    "fi\n"
    "exit 1\n";

static const char *const kSingStart =
    "#!/bin/sh\n"
    "# if we are here start notify PID 1 to continue\n"
    "# DON'T REMOVE\n"
    "kill -CONT 1\n"
    "for script in /.singularity.d/env/*.sh; do\n"
    "    if [ -f \"$script\" ]; then\n"
    "        . \"$script\"\n"
    "    fi\n"
    "done\n"
    "if test -x \"/.singularity.d/startscript\"; then\n"
    "    exec \"/.singularity.d/startscript\"\n"
    "fi\n";

static const char *const kSingTest =
    "#!/bin/sh\n"
    "for script in /.singularity.d/env/*.sh; do\n"
    "    if [ -f \"$script\" ]; then\n"
    "        . \"$script\"\n"
    "    fi\n"
    "done\n"
    "if test -n \"${SINGULARITY_APPNAME:-}\"; then\n"
    "    if test -x \"/scif/apps/${SINGULARITY_APPNAME:-}/scif/test\"; then\n"
    "        exec \"/scif/apps/${SINGULARITY_APPNAME:-}/scif/test\" \"$@\"\n"
    "    else\n"
    "        echo \"No tests for contained app: ${SINGULARITY_APPNAME:-}\"\n"
    "        exit 1\n"
    "    fi\n"
    "elif test -x \"/.singularity.d/test\"; then\n"
    "    exec \"/.singularity.d/test\" \"$@\"\n"
    "else\n"
    "    echo \"No test found in container, executing /bin/sh -c true\"\n"
    "    exec /bin/sh -c true\n"
    "fi\n";

static const char *const kSingEnv01Base =
    "#!/bin/sh\n"
    "# \n"
    "# Copyright (c) 2017, SingularityWare, LLC. All rights reserved.\n"
    "# Copyright (c) 2015-2017, Gregory M. Kurtzer. All rights reserved.\n"
    "# \n"
    "# Copyright (c) 2016-2017, The Regents of the University of California,\n"
    "# through Lawrence Berkeley National Laboratory (subject to receipt of any\n"
    "# required approvals from the U.S. Dept. of Energy).  All rights reserved.\n"
    "# \n";

static const char *const kSingEnv90 =
    "#!/bin/sh\n"
    "# Custom environment shell code should follow\n";

static const char *const kSingEnv95Apps =
    "#!/bin/sh\n"
    "#\n"
    "# Copyright (c) 2017, SingularityWare, LLC. All rights reserved.\n"
    "#\n"
    "if test -n \"${SINGULARITY_APPNAME:-}\"; then\n"
    "    # The active app should be exported\n"
    "    export SINGULARITY_APPNAME\n"
    "    if test -d \"/scif/apps/${SINGULARITY_APPNAME:-}/\"; then\n"
    "        SCIF_APPS=\"/scif/apps\"\n"
    "        SCIF_APPROOT=\"/scif/apps/${SINGULARITY_APPNAME:-}\"\n"
    "        export SCIF_APPROOT SCIF_APPS\n"
    "        PATH=\"/scif/apps/${SINGULARITY_APPNAME:-}:$PATH\"\n"
    "        if test -d \"/scif/apps/${SINGULARITY_APPNAME:-}/bin\"; then\n"
    "            PATH=\"/scif/apps/${SINGULARITY_APPNAME:-}/bin:$PATH\"\n"
    "        fi\n"
    "        if test -d \"/scif/apps/${SINGULARITY_APPNAME:-}/lib\"; then\n"
    "            LD_LIBRARY_PATH=\"/scif/apps/${SINGULARITY_APPNAME:-}/lib:$LD_LIBRARY_PATH\"\n"
    "            export LD_LIBRARY_PATH\n"
    "        fi\n"
    "        if [ -f \"/scif/apps/${SINGULARITY_APPNAME:-}/scif/env/01-base.sh\" ]; then\n"
    "            . \"/scif/apps/${SINGULARITY_APPNAME:-}/scif/env/01-base.sh\"\n"
    "        fi\n"
    "        if [ -f \"/scif/apps/${SINGULARITY_APPNAME:-}/scif/env/90-environment.sh\" ]; then\n"
    "            . \"/scif/apps/${SINGULARITY_APPNAME:-}/scif/env/90-environment.sh\"\n"
    "        fi\n"
    "        export PATH\n"
    "    else\n"
    "        echo \"Could not locate the container application: ${SINGULARITY_APPNAME}\"\n"
    "        exit 1\n"
    "    fi\n"
    "fi\n";

static const char *const kSingEnv99Base =
    "#!/bin/sh\n"
    "# \n"
    "# Copyright (c) 2017, SingularityWare, LLC. All rights reserved.\n"
    "# Copyright (c) 2015-2017, Gregory M. Kurtzer. All rights reserved.\n"
    "# \n"
    "if [ -z \"$LD_LIBRARY_PATH\" ]; then\n"
    "    LD_LIBRARY_PATH=\"/.singularity.d/libs\"\n"
    "else\n"
    "    LD_LIBRARY_PATH=\"$LD_LIBRARY_PATH:/.singularity.d/libs\"\n"
    "fi\n"
    "PS1=\"Singularity> \"\n"
    "export LD_LIBRARY_PATH PS1\n";

static const char *const kSingEnv99Runtimevars =
    "#!/bin/sh\n"
    "if [ -n \"${SING_USER_DEFINED_PREPEND_PATH:-}\" ]; then\n"
    "\tPATH=\"${SING_USER_DEFINED_PREPEND_PATH}:${PATH}\"\n"
    "fi\n"
    "if [ -n \"${SING_USER_DEFINED_APPEND_PATH:-}\" ]; then\n"
    "\tPATH=\"${PATH}:${SING_USER_DEFINED_APPEND_PATH}\"\n"
    "fi\n"
    "if [ -n \"${SING_USER_DEFINED_PATH:-}\" ]; then\n"
    "\tPATH=\"${SING_USER_DEFINED_PATH}\"\n"
    "fi\n"
    "unset SING_USER_DEFINED_PREPEND_PATH \\\n"
    "\t  SING_USER_DEFINED_APPEND_PATH \\\n"
    "\t  SING_USER_DEFINED_PATH\n"
    "export PATH\n";

static const char *const kSingStartscript =
    "#!/bin/sh\n";


string CommandOverlay::ShellEscape(const string &s) {
  string escaped = ReplaceAll(s, "\\", "\\\\");
  escaped = ReplaceAll(escaped, "\"", "\\\"");
  escaped = ReplaceAll(escaped, "`", "\\`");
  escaped = ReplaceAll(escaped, "$", "\\$");
  return escaped;
}


string CommandOverlay::ArgsQuoted(const vector<string> &args) {
  string quoted;
  for (size_t i = 0; i < args.size(); ++i) {
    if (i > 0) quoted += " ";
    quoted += "\"" + ShellEscape(args[i]) + "\"";
  }
  return quoted;
}


string CommandOverlay::GenerateRunscript(
    const vector<string> &entrypoint,
    const vector<string> &cmd) {
  string script = "#!/bin/sh\n";
  if (!entrypoint.empty()) {
    script += "OCI_ENTRYPOINT='" + ArgsQuoted(entrypoint) + "'\n";
  } else {
    script += "OCI_ENTRYPOINT=''\n";
  }
  if (!cmd.empty()) {
    script += "OCI_CMD='" + ArgsQuoted(cmd) + "'\n";
  } else {
    script += "OCI_CMD=''\n";
  }
  script +=
    "CMDLINE_ARGS=\"\"\n"
    "# prepare command line arguments for evaluation\n"
    "for arg in \"$@\"; do\n"
    "    CMDLINE_ARGS=\"${CMDLINE_ARGS} \\\"$arg\\\"\"\n"
    "done\n"
    "# ENTRYPOINT only - run entrypoint plus args\n"
    "if [ -z \"$OCI_CMD\" ] && [ -n \"$OCI_ENTRYPOINT\" ]; then\n"
    "    if [ $# -gt 0 ]; then\n"
    "        SINGULARITY_OCI_RUN=\"${OCI_ENTRYPOINT} ${CMDLINE_ARGS}\"\n"
    "    else\n"
    "        SINGULARITY_OCI_RUN=\"${OCI_ENTRYPOINT}\"\n"
    "    fi\n"
    "fi\n"
    "# CMD only - run CMD or override with args\n"
    "if [ -n \"$OCI_CMD\" ] && [ -z \"$OCI_ENTRYPOINT\" ]; then\n"
    "    if [ $# -gt 0 ]; then\n"
    "        SINGULARITY_OCI_RUN=\"${CMDLINE_ARGS}\"\n"
    "    else\n"
    "        SINGULARITY_OCI_RUN=\"${OCI_CMD}\"\n"
    "    fi\n"
    "fi\n"
    "# ENTRYPOINT and CMD - run ENTRYPOINT with CMD as default args\n"
    "# override with user provided args\n"
    "if [ $# -gt 0 ]; then\n"
    "    SINGULARITY_OCI_RUN=\"${OCI_ENTRYPOINT} ${CMDLINE_ARGS}\"\n"
    "else\n"
    "    SINGULARITY_OCI_RUN=\"${OCI_ENTRYPOINT} ${OCI_CMD}\"\n"
    "fi\n"
    "# Evaluate shell expressions first and set arguments accordingly,\n"
    "# then execute final command as first container process\n"
    "eval \"set ${SINGULARITY_OCI_RUN}\"\n"
    "exec \"$@\"\n";
  return script;
}


string CommandOverlay::GenerateEnvScript(const vector<string> &env) {
  string script = "#!/bin/sh\n";
  for (size_t i = 0; i < env.size(); ++i) {
    const string &element = env[i];
    const size_t eq = element.find('=');
    if (eq == string::npos) {
      // No '=' — just export empty default
      script += "export " + element + "=\"${" + element + ":-}\"\n";
    } else {
      const string key = element.substr(0, eq);
      const string val = element.substr(eq + 1);
      if (key == "PATH") {
        script += "export PATH=\"" + ShellEscape(val) + "\"\n";
      } else {
        script += "export " + key + "=\"${" + key + ":-\""
                + ShellEscape(val) + "\"}\"\n";
      }
    }
  }
  return script;
}


OverlayEntry CommandOverlay::MakeDirEntry(const string &path,
                                          const string &parent) {
  OverlayEntry oe;
  oe.path = path;
  oe.parent = parent;
  oe.is_whiteout = false;
  oe.is_opaque_dir = false;
  oe.entry.name_ = NameString(GetFileName(path));
  oe.entry.mode_ = S_IFDIR | 0755;
  oe.entry.uid_ = 0;
  oe.entry.gid_ = 0;
  oe.entry.size_ = 4096;
  oe.entry.mtime_ = time(NULL);
  oe.entry.linkcount_ = 2;
  return oe;
}


/**
 * Helper class to collect spooler results for singularity dotfiles.
 * Registered as a listener on the spooler, it stores the content hash
 * for each processed file keyed by path.
 */
class SingularitySpoolerSink {
 public:
  void OnFileProcessed(const upload::SpoolerResult &result) {
    hashes_[result.local_path] = result.content_hash;
  }

  bool GetHash(const string &path, shash::Any *hash) const {
    const map<string, shash::Any>::const_iterator it = hashes_.find(path);
    if (it == hashes_.end()) return false;
    *hash = it->second;
    return true;
  }

 private:
  map<string, shash::Any> hashes_;
};


OverlayEntry CommandOverlay::MakeFileEntry(const string &path,
                                           const string &parent,
                                           const string &content,
                                           upload::Spooler *spooler) {
  // Process the content through the spooler to get a content hash.
  // We use a StringIngestionSource so no temp file is needed.
  // The spooler is used in a synchronous fashion: process one file,
  // wait, then read the result via a temporary listener.
  SingularitySpoolerSink sink;
  typename upload::Spooler::CallbackPtr cb = spooler->RegisterListener(
      &SingularitySpoolerSink::OnFileProcessed, &sink);

  spooler->Process(
      new StringIngestionSource(content, path),
      false /* no chunking */);
  spooler->WaitForUpload();
  spooler->UnregisterListener(cb);

  shash::Any content_hash;
  if (!sink.GetHash(path, &content_hash)) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Failed to get content hash for singularity file %s",
             path.c_str());
  }

  OverlayEntry oe;
  oe.path = path;
  oe.parent = parent;
  oe.is_whiteout = false;
  oe.is_opaque_dir = false;
  oe.entry.name_ = NameString(GetFileName(path));
  oe.entry.mode_ = S_IFREG | 0755;
  oe.entry.uid_ = 0;
  oe.entry.gid_ = 0;
  oe.entry.size_ = content.size();
  oe.entry.mtime_ = time(NULL);
  oe.entry.linkcount_ = 1;
  oe.entry.checksum_ = content_hash;
  return oe;
}


OverlayEntry CommandOverlay::MakeSymlinkEntry(const string &path,
                                              const string &parent,
                                              const string &target) {
  OverlayEntry oe;
  oe.path = path;
  oe.parent = parent;
  oe.is_whiteout = false;
  oe.is_opaque_dir = false;
  oe.entry.name_ = NameString(GetFileName(path));
  oe.entry.mode_ = S_IFLNK | 0777;
  oe.entry.uid_ = 0;
  oe.entry.gid_ = 0;
  oe.entry.size_ = target.size();
  oe.entry.mtime_ = time(NULL);
  oe.entry.linkcount_ = 1;
  oe.entry.symlink_ = LinkString(target);
  return oe;
}


bool CommandOverlay::InjectSingularityDotfiles(
    const string &oci_config_path,
    upload::Spooler *spooler,
    map<string, OverlayEntry> *merged) {
  // ---------------------------------------------------------------
  // 1.  Parse the OCI image config JSON
  // ---------------------------------------------------------------
  const int fd = open(oci_config_path.c_str(), O_RDONLY);
  if (fd < 0) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Failed to open OCI config file %s", oci_config_path.c_str());
    return false;
  }
  string config_json;
  if (!SafeReadToString(fd, &config_json)) {
    close(fd);
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Failed to read OCI config from %s", oci_config_path.c_str());
    return false;
  }
  close(fd);

  const UniquePtr<JsonDocument> json(JsonDocument::Create(config_json));
  if (!json.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Failed to parse OCI config JSON from %s",
             oci_config_path.c_str());
    return false;
  }

  // Extract config.Env, config.Entrypoint, config.Cmd
  vector<string> entrypoint;
  vector<string> cmd;
  vector<string> env;

  const JSON *config_obj =
      JsonDocument::SearchInObject(json->root(), "config", JSON_OBJECT);
  if (config_obj != NULL) {
    const JSON *ep_arr =
        JsonDocument::SearchInObject(config_obj, "Entrypoint", JSON_ARRAY);
    if (ep_arr != NULL) {
      for (JSON::const_iterator it = ep_arr->begin();
           it != ep_arr->end(); ++it) {
        if (it->is_string()) entrypoint.push_back(it->get<string>());
      }
    }
    const JSON *cmd_arr =
        JsonDocument::SearchInObject(config_obj, "Cmd", JSON_ARRAY);
    if (cmd_arr != NULL) {
      for (JSON::const_iterator it = cmd_arr->begin();
           it != cmd_arr->end(); ++it) {
        if (it->is_string()) cmd.push_back(it->get<string>());
      }
    }
    const JSON *env_arr =
        JsonDocument::SearchInObject(config_obj, "Env", JSON_ARRAY);
    if (env_arr != NULL) {
      for (JSON::const_iterator it = env_arr->begin();
           it != env_arr->end(); ++it) {
        if (it->is_string()) env.push_back(it->get<string>());
      }
    }
  }

  LogCvmfs(kLogCvmfs, kLogStdout,
           "Injecting Singularity dotfiles (Entrypoint: %zu, Cmd: %zu, "
           "Env: %zu entries)",
           entrypoint.size(), cmd.size(), env.size());

  // ---------------------------------------------------------------
  // 2.  Create directory entries
  // ---------------------------------------------------------------
  (*merged)[".singularity.d"] =
      MakeDirEntry(".singularity.d", "");
  (*merged)[".singularity.d/libs"] =
      MakeDirEntry(".singularity.d/libs", ".singularity.d");
  (*merged)[".singularity.d/actions"] =
      MakeDirEntry(".singularity.d/actions", ".singularity.d");
  (*merged)[".singularity.d/env"] =
      MakeDirEntry(".singularity.d/env", ".singularity.d");

  // Also create common FHS directories if missing
  const char *fhs_dirs[] = {
      "dev", "proc", "root", "var", "var/tmp", "tmp", "etc", "sys", "home",
      NULL};
  for (int i = 0; fhs_dirs[i] != NULL; ++i) {
    const string d = fhs_dirs[i];
    if (merged->find(d) == merged->end()) {
      const string par = (d.find('/') != string::npos)
                             ? GetParentPath(d)
                             : "";
      (*merged)[d] = MakeDirEntry(d, par);
    }
  }

  // ---------------------------------------------------------------
  // 3.  Create file entries (content is uploaded via spooler)
  // ---------------------------------------------------------------
  // Action scripts
  (*merged)[".singularity.d/actions/exec"] =
      MakeFileEntry(".singularity.d/actions/exec",
                    ".singularity.d/actions", kSingExec, spooler);
  (*merged)[".singularity.d/actions/run"] =
      MakeFileEntry(".singularity.d/actions/run",
                    ".singularity.d/actions", kSingRun, spooler);
  (*merged)[".singularity.d/actions/shell"] =
      MakeFileEntry(".singularity.d/actions/shell",
                    ".singularity.d/actions", kSingShell, spooler);
  (*merged)[".singularity.d/actions/start"] =
      MakeFileEntry(".singularity.d/actions/start",
                    ".singularity.d/actions", kSingStart, spooler);
  (*merged)[".singularity.d/actions/test"] =
      MakeFileEntry(".singularity.d/actions/test",
                    ".singularity.d/actions", kSingTest, spooler);

  // Environment scripts
  (*merged)[".singularity.d/env/01-base.sh"] =
      MakeFileEntry(".singularity.d/env/01-base.sh",
                    ".singularity.d/env", kSingEnv01Base, spooler);
  (*merged)[".singularity.d/env/90-environment.sh"] =
      MakeFileEntry(".singularity.d/env/90-environment.sh",
                    ".singularity.d/env", kSingEnv90, spooler);
  (*merged)[".singularity.d/env/91-environment.sh"] =
      MakeFileEntry(".singularity.d/env/91-environment.sh",
                    ".singularity.d/env", kSingEnv90, spooler);
  (*merged)[".singularity.d/env/95-apps.sh"] =
      MakeFileEntry(".singularity.d/env/95-apps.sh",
                    ".singularity.d/env", kSingEnv95Apps, spooler);
  (*merged)[".singularity.d/env/99-base.sh"] =
      MakeFileEntry(".singularity.d/env/99-base.sh",
                    ".singularity.d/env", kSingEnv99Base, spooler);
  (*merged)[".singularity.d/env/99-runtimevars.sh"] =
      MakeFileEntry(".singularity.d/env/99-runtimevars.sh",
                    ".singularity.d/env", kSingEnv99Runtimevars, spooler);

  // OCI-config-dependent files
  const string runscript = GenerateRunscript(entrypoint, cmd);
  (*merged)[".singularity.d/runscript"] =
      MakeFileEntry(".singularity.d/runscript",
                    ".singularity.d", runscript, spooler);
  (*merged)[".singularity.d/startscript"] =
      MakeFileEntry(".singularity.d/startscript",
                    ".singularity.d", kSingStartscript, spooler);

  const string env_script = GenerateEnvScript(env);
  (*merged)[".singularity.d/env/10-docker2singularity.sh"] =
      MakeFileEntry(".singularity.d/env/10-docker2singularity.sh",
                    ".singularity.d/env", env_script, spooler);

  // ---------------------------------------------------------------
  // 4.  Create symlinks
  // ---------------------------------------------------------------
  // Only create if not already present from a layer
  if (merged->find("singularity") == merged->end()) {
    (*merged)["singularity"] =
        MakeSymlinkEntry("singularity", "",
                         ".singularity.d/runscript");
  }
  if (merged->find(".run") == merged->end()) {
    (*merged)[".run"] =
        MakeSymlinkEntry(".run", "",
                         ".singularity.d/actions/run");
  }
  if (merged->find(".shell") == merged->end()) {
    (*merged)[".shell"] =
        MakeSymlinkEntry(".shell", "",
                         ".singularity.d/actions/shell");
  }
  if (merged->find(".exec") == merged->end()) {
    (*merged)[".exec"] =
        MakeSymlinkEntry(".exec", "",
                         ".singularity.d/actions/exec");
  }
  if (merged->find(".test") == merged->end()) {
    (*merged)[".test"] =
        MakeSymlinkEntry(".test", "",
                         ".singularity.d/actions/test");
  }
  if (merged->find("environment") == merged->end()) {
    (*merged)["environment"] =
        MakeSymlinkEntry("environment", "",
                         ".singularity.d/env/90-environment.sh");
  }

  LogCvmfs(kLogCvmfs, kLogStdout,
           "Injected Singularity dotfiles into merged overlay");
  return true;
}


bool CommandOverlay::PublishMergedEntries(
    catalog::WritableCatalogManager *catalog_mgr,
    const map<string, OverlayEntry> &merged,
    const string &dest_path) const {
  // dest_path starts with '/' for LookupPath, but AddDirectory/AddFile
  // expect parent_directory without leading '/' because MakeRelativePath
  // (called internally) prepends it.  Create a stripped copy for add calls.
  const string dest_path_rel = (!dest_path.empty() && dest_path[0] == '/')
      ? dest_path.substr(1) : dest_path;

  // Ensure the destination directory itself exists in the catalog.
  // Check if dest_path already exists; if not, create it.
  catalog::DirectoryEntry dest_dirent;
  if (!catalog_mgr->LookupPath(dest_path, catalog::kLookupDefault,
                                &dest_dirent)) {
    // Create the destination directory (and any missing parents)
    // Walk up to find the deepest existing ancestor
    vector<string> dirs_to_create;
    string check_path = dest_path;
    while (!check_path.empty() && check_path != "/") {
      catalog::DirectoryEntry check_dirent;
      if (catalog_mgr->LookupPath(check_path, catalog::kLookupDefault,
                                   &check_dirent)) {
        break;
      }
      dirs_to_create.push_back(check_path);
      check_path = GetParentPath(check_path);
    }

    // Create directories from outermost to innermost
    for (int i = static_cast<int>(dirs_to_create.size()) - 1; i >= 0; --i) {
      const string &dir = dirs_to_create[i];
      string parent = GetParentPath(dir);
      const string name = GetFileName(dir);

      // Strip leading '/' — AddDirectory calls MakeRelativePath which adds it
      if (!parent.empty() && parent[0] == '/') {
        parent = parent.substr(1);
      }

      catalog::DirectoryEntryBase new_dir;
      new_dir.name_.Assign(name.data(), name.length());
      new_dir.mode_ = S_IFDIR | 0755;
      new_dir.uid_ = 0;
      new_dir.gid_ = 0;
      new_dir.size_ = 4096;
      new_dir.mtime_ = time(NULL);
      new_dir.linkcount_ = 2;

      catalog_mgr->AddDirectory(new_dir, XattrList(), parent);
      LogCvmfs(kLogCvmfs, kLogDebug,
               "Created destination directory: %s", dir.c_str());
    }
  }

  // Add entries in sorted order. The map is sorted lexicographically,
  // so parent directories appear before their children.
  for (map<string, OverlayEntry>::const_iterator it = merged.begin();
       it != merged.end(); ++it) {
    const OverlayEntry &oe = it->second;

    // Build parent path without leading '/' for AddDirectory/AddFile
    // (MakeRelativePath inside those functions adds it back)
    const string parent_path = oe.parent.empty()
        ? dest_path_rel
        : dest_path_rel + "/" + oe.parent;

    if (oe.entry.IsDirectory()) {
      catalog_mgr->AddDirectory(oe.entry, oe.xattrs, parent_path);
    } else {
      catalog_mgr->AddFile(
          static_cast<const catalog::DirectoryEntryBase &>(oe.entry),
          oe.xattrs, parent_path);
    }
  }

  // Turn the destination directory into a nested catalog so that the overlay
  // content lives in its own catalog database file.

  // Add a .cvmfscatalog marker file 
  catalog::DirectoryEntryBase catalog_marker;
  catalog_marker.name_ = NameString(".cvmfscatalog");
  catalog_marker.mode_ = (S_IFREG | 0666);
  catalog_marker.size_ = 0;
  catalog_marker.mtime_ = time(NULL);
  catalog_marker.uid_ = 0;
  catalog_marker.gid_ = 0;
  catalog_marker.linkcount_ = 1;
  // Hash of the compressed empty file
  catalog_marker.checksum_ = shash::MkFromHexPtr(
            shash::HexPtr("e8ec3d88b62ebf526e4e5a4ff6162a3aa48a6b78"),
            shash::kSuffixNone);  // hash of ""
  catalog_mgr->AddFile(catalog_marker, XattrList(), dest_path_rel);
  // CreateNestedCatalog calls MakeRelativePath internally which prepends '/'.
  // Pass the stripped version to avoid a double leading slash.
  catalog_mgr->CreateNestedCatalog(dest_path_rel);

  LogCvmfs(kLogCvmfs, kLogStdout,
           "Published %zu entries under %s (nested catalog)",
           merged.size(), dest_path.c_str());
  return true;
}


catalog::Catalog *CommandOverlay::LoadCatalogForPath(
    const string &repo_base,
    const string &subdirectory,
    const string &temp_dir,
    const shash::Any &root_hash) {
  // Fetch the root catalog from the repository
  const string hash_path = "data/" + root_hash.MakePath();
  string catalog_path;

  if (IsHttpUrl(repo_base)) {
    // Download and decompress from remote
    const string url = repo_base + "/" + hash_path;
    catalog_path = temp_dir + "/" + root_hash.ToString();

    cvmfs::PathSink pathsink(catalog_path);
    download::JobInfo download_job(&url, true, false, &root_hash, &pathsink);
    const download::Failures retval = download_manager()->Fetch(&download_job);
    if (retval != download::kFailOk) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to download catalog %s (%d)",
               root_hash.ToString().c_str(), retval);
      return NULL;
    }
  } else {
    // Local repository: decompress the catalog
    const string source_path = repo_base + "/" + hash_path;
    catalog_path = temp_dir + "/" + root_hash.ToString();

    if (!zlib::DecompressPath2Path(source_path, catalog_path)) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "Failed to decompress catalog %s from %s",
               root_hash.ToString().c_str(), source_path.c_str());
      return NULL;
    }
  }

  catalog::Catalog *catalog = catalog::Catalog::AttachFreely(
      subdirectory, catalog_path, root_hash);
  if (catalog == NULL) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Failed to attach catalog for path %s",
             subdirectory.c_str());
    unlink(catalog_path.c_str());
    return NULL;
  }

  catalog->TakeDatabaseFileOwnership();
  return catalog;
}


catalog::Catalog *CommandOverlay::FindCatalogForLayer(
    const string &repo_base,
    const string &temp_dir,
    catalog::Catalog *catalog,
    const string &layer_path,
    vector<catalog::Catalog *> *loaded_catalogs) {
  // First try a direct lookup in the given catalog
  catalog::DirectoryEntry test_entry;
  const PathString ps_layer(layer_path.data(), layer_path.length());
  if (catalog->LookupPath(ps_layer, &test_entry)) {
    return catalog;
  }

  // The path was not found directly.  Walk the path components *below*
  // this catalog's mountpoint to find a nested catalog mountpoint that
  // is an ancestor of layer_path.
  const string mountpoint = catalog->mountpoint().ToString();

  // Verify layer_path starts with the mountpoint (or mountpoint is empty
  // for the root catalog)
  if (!mountpoint.empty() && layer_path.substr(0, mountpoint.length())
                                                            != mountpoint) {
    return NULL;
  }

  // Get the suffix of layer_path below the mountpoint
  const string suffix = mountpoint.empty() ? layer_path
                                           : layer_path.substr(
                                                 mountpoint.length());
  const vector<string> components = SplitString(suffix, '/');
  string prefix = mountpoint;
  for (size_t i = 0; i < components.size(); ++i) {
    if (components[i].empty()) continue;
    prefix += "/" + components[i];

    catalog::DirectoryEntry dir_entry;
    const PathString ps_prefix(prefix.data(), prefix.length());
    if (!catalog->LookupPath(ps_prefix, &dir_entry)) {
      break;
    }

    if (dir_entry.IsNestedCatalogMountpoint()) {
      shash::Any nested_hash;
      uint64_t nested_size;
      if (!catalog->FindNested(ps_prefix, &nested_hash, &nested_size)) {
        LogCvmfs(kLogCvmfs, kLogStderr,
                 "Failed to find nested catalog hash for %s", prefix.c_str());
        return NULL;
      }

      catalog::Catalog *nested = LoadCatalogForPath(
          repo_base, prefix, temp_dir, nested_hash);
      if (nested == NULL) {
        LogCvmfs(kLogCvmfs, kLogStderr,
                 "Failed to load nested catalog at %s", prefix.c_str());
        return NULL;
      }
      loaded_catalogs->push_back(nested);

      // Recurse: the layer path may be directly in this nested catalog
      // or in an even deeper nested catalog
      return FindCatalogForLayer(
          repo_base, temp_dir, nested, layer_path, loaded_catalogs);
    }
  }

  LogCvmfs(kLogCvmfs, kLogStderr, "Layer path not found: %s",
           layer_path.c_str());
  return NULL;
}


int CommandOverlay::Main(const ArgumentList &args) {
  // Parse publish workflow parameters
  const string spooler_definition_str = *args.find('r')->second;
  const string stratum0 = *args.find('w')->second;
  const string temp_dir = MakeCanonicalPath(*args.find('t')->second);
  const string manifest_path = *args.find('o')->second;
  const shash::Any base_hash =
      shash::MkFromHexPtr(shash::HexPtr(*args.find('b')->second),
                          shash::kSuffixCatalog);
  const string public_keys = *args.find('K')->second;
  const string repo_name = *args.find('N')->second;

  // Parse overlay-specific parameters
  const string layers_str = *args.find('l')->second;
  string dest_path = MakeCanonicalPath(*args.find('d')->second);
  // Ensure dest_path starts with exactly one '/'
  while (dest_path.length() > 1 && dest_path[0] == '/' && dest_path[1] == '/') {
    dest_path = dest_path.substr(1);
  }
  if (dest_path.empty() || dest_path[0] != '/') {
    dest_path = "/" + dest_path;
  }
  shash::Algorithms hash_algorithm = shash::kSha1;
  if (args.find('e') != args.end()) {
    hash_algorithm = shash::ParseHashAlgorithm(*args.find('e')->second);
    if (hash_algorithm == shash::kAny) {
      PrintError("unknown hash algorithm");
      return 1;
    }
  }
  zlib::Algorithms compression_alg = zlib::kZlibDefault;
  if (args.find('Z') != args.end()) {
    compression_alg = zlib::ParseCompressionAlgorithm(
        *args.find('Z')->second);
  }

  const string oci_config_path =
      (args.count('c') > 0) ? *args.find('c')->second : "";
  const bool skip_singularity = (args.count('S') > 0);

  // Gateway lease: empty for a directly-writable (S3/local) upstream, set when
  // committing through a repository gateway (mountless publishing).
  const string session_token_file =
      (args.count('P') > 0) ? *args.find('P')->second : "";
  const string key_file =
      (args.count('H') > 0) ? *args.find('H')->second : "";

  // Parse comma-separated layer paths
  const vector<string> layers = SplitString(layers_str, ',');
  if (layers.empty()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "No layers specified");
    return 1;
  }

  LogCvmfs(kLogCvmfs, kLogStdout, "Overlay merge of %zu layers into %s",
           layers.size(), dest_path.c_str());
  for (size_t i = 0; i < layers.size(); ++i) {
    LogCvmfs(kLogCvmfs, kLogStdout, "  Layer %zu: %s", i, layers[i].c_str());
  }

  // Set up spoolers (following the ingest pattern)
  perf::StatisticsTemplate publish_statistics("publish", this->statistics());

  const upload::SpoolerDefinition spooler_definition(
      spooler_definition_str, hash_algorithm, compression_alg,
      false /* generate_legacy_bulk_chunks */,
      false /* use_file_chunking */,
      0, 0, 0 /* chunk sizes: unused */,
      session_token_file, key_file);

  const upload::SpoolerDefinition spooler_definition_catalogs(
      spooler_definition.Dup2DefaultCompression());

  const UniquePtr<upload::Spooler> spooler_files(
      upload::Spooler::Construct(spooler_definition, &publish_statistics));
  if (!spooler_files.IsValid()) {
    PrintError("Failed to create file spooler");
    return 3;
  }
  const UniquePtr<upload::Spooler> spooler_catalogs(
      upload::Spooler::Construct(spooler_definition_catalogs,
                                 &publish_statistics));
  if (!spooler_catalogs.IsValid()) {
    PrintError("Failed to create catalog spooler");
    return 3;
  }

  // Initialize download manager and signature manager
  const bool follow_redirects = (args.count('L') > 0);
  const string proxy = (args.count('@') > 0) ? *args.find('@')->second : "";
  if (!InitDownloadManager(follow_redirects, proxy)) {
    PrintError("Failed to initialize download manager");
    return 3;
  }
  if (!InitSignatureManager(public_keys)) {
    PrintError("Failed to initialize signature manager");
    return 3;
  }

  // Fetch repository manifest
  const UniquePtr<manifest::Manifest> manifest(
      FetchRemoteManifest(stratum0, repo_name, base_hash));
  if (!manifest.IsValid()) {
    PrintError("Failed to load repository manifest");
    return 3;
  }

  const string old_root_hash = manifest->catalog_hash().ToString(true);
  LogCvmfs(kLogCvmfs, kLogStdout, "Root catalog hash: %s",
           old_root_hash.c_str());

  // Load root catalog for reading layer entries
  map<string, OverlayEntry> merged;
  catalog::Catalog *root_catalog = LoadCatalogForPath(
      stratum0, "", temp_dir, manifest->catalog_hash());
  if (root_catalog == NULL) {
    PrintError("Failed to load root catalog");
    return 1;
  }

  // Process layers bottom-to-top
  for (size_t i = 0; i < layers.size(); ++i) {
    string layer_path = MakeCanonicalPath(layers[i]);
    // Ensure layer path starts with exactly one '/'
    while (layer_path.length() > 1
           && layer_path[0] == '/' && layer_path[1] == '/') {
      layer_path = layer_path.substr(1);
    }
    if (layer_path.empty() || layer_path[0] != '/') {
      layer_path = "/" + layer_path;
    }

    LogCvmfs(kLogCvmfs, kLogStdout, "Processing layer %zu: %s",
             i, layer_path.c_str());

    map<string, OverlayEntry> layer_entries;

    // Find the catalog that contains this layer path (may be nested)
    vector<catalog::Catalog *> loaded_catalogs;
    catalog::Catalog *layer_catalog = FindCatalogForLayer(
        stratum0, temp_dir, root_catalog, layer_path, &loaded_catalogs);
    if (layer_catalog == NULL) {
      for (size_t j = 0; j < loaded_catalogs.size(); ++j)
        delete loaded_catalogs[j];
      delete root_catalog;
      return 1;
    }

    catalog::DirectoryEntry subdir_entry;
    const PathString ps_layer_path(layer_path.data(), layer_path.length());
    if (!layer_catalog->LookupPath(ps_layer_path, &subdir_entry)) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "Unexpected: layer path not found after catalog resolution: %s",
               layer_path.c_str());
      for (size_t j = 0; j < loaded_catalogs.size(); ++j)
        delete loaded_catalogs[j];
      delete root_catalog;
      return 1;
    }

    // Check if the layer path itself is a nested catalog mountpoint;
    // if so, load that catalog and read its entries.
    if (subdir_entry.IsNestedCatalogMountpoint()) {
      shash::Any nested_hash;
      uint64_t nested_size;
      if (!layer_catalog->FindNested(ps_layer_path, &nested_hash,
                                     &nested_size)) {
        LogCvmfs(kLogCvmfs, kLogStderr,
                 "Failed to find nested catalog for %s",
                 layer_path.c_str());
        for (size_t j = 0; j < loaded_catalogs.size(); ++j)
          delete loaded_catalogs[j];
        delete root_catalog;
        return 1;
      }

      catalog::Catalog *nested_catalog = LoadCatalogForPath(
          stratum0, layer_path, temp_dir, nested_hash);
      if (nested_catalog == NULL) {
        LogCvmfs(kLogCvmfs, kLogStderr,
                 "Failed to load nested catalog for %s",
                 layer_path.c_str());
        for (size_t j = 0; j < loaded_catalogs.size(); ++j)
          delete loaded_catalogs[j];
        delete root_catalog;
        return 1;
      }

      ReadCatalogEntries(nested_catalog, layer_path, "",
                         stratum0, temp_dir, &layer_entries);
      delete nested_catalog;
    } else {
      ReadCatalogEntries(layer_catalog, layer_path, "",
                         stratum0, temp_dir, &layer_entries);
    }

    // Clean up any intermediate catalogs loaded during hierarchy walk
    for (size_t j = 0; j < loaded_catalogs.size(); ++j)
      delete loaded_catalogs[j];

    LogCvmfs(kLogCvmfs, kLogStdout, "  Read %zu entries from layer %s",
             layer_entries.size(), layer_path.c_str());

    MergeLayer(layer_entries, &merged);

    LogCvmfs(kLogCvmfs, kLogStdout, "  Merged total: %zu entries",
             merged.size());
  }

  delete root_catalog;

  // Inject Singularity dotfiles if requested
  if (!oci_config_path.empty() && !skip_singularity) {
    if (!InjectSingularityDotfiles(oci_config_path,
                                   spooler_files.weak_ref(), &merged)) {
      PrintError("Failed to inject Singularity dotfiles");
      return 4;
    }
  }

  // Set up WritableCatalogManager and publish merged entries
  LogCvmfs(kLogCvmfs, kLogStdout,
           "Publishing %zu merged entries under %s",
           merged.size(), dest_path.c_str());

  catalog::WritableCatalogManager catalog_manager(
      base_hash, stratum0, temp_dir,
      spooler_catalogs.weak_ref(), download_manager(),
      false /* enforce_limits */,
      0 /* nested_kcatalog_limit */,
      0 /* root_kcatalog_limit */,
      0 /* file_mbyte_limit */,
      statistics(),
      false /* is_balanceable */,
      0 /* max_weight */, 0 /* min_weight */);
  catalog_manager.Init();

  if (!PublishMergedEntries(&catalog_manager, merged, dest_path)) {
    PrintError("Failed to publish merged entries");
    return 5;
  }

  // Commit catalog changes and produce updated manifest
  catalog_manager.PrecalculateListings();
  if (!catalog_manager.Commit(false, 0, manifest.weak_ref())) {
    PrintError("Failed to commit catalog changes");
    return 5;
  }

  // Finalize spoolers
  LogCvmfs(kLogCvmfs, kLogStdout, "Waiting for uploads to finish...");
  spooler_files->WaitForUpload();
  spooler_catalogs->WaitForUpload();
  spooler_files->FinalizeSession(false);

  const string new_root_hash = manifest->catalog_hash().ToString(true);
  if (!spooler_catalogs->FinalizeSession(true, old_root_hash, new_root_hash,
                                         RepositoryTag())) {
    PrintError("Failed to finalize session");
    return 5;
  }

  // Export manifest
  if (!manifest->Export(manifest_path)) {
    PrintError("Failed to export manifest");
    return 6;
  }

  LogCvmfs(kLogCvmfs, kLogStdout,
           "Overlay published successfully to %s", dest_path.c_str());
  return 0;
}

}  // namespace swissknife
