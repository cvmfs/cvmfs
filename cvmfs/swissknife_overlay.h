/**
 * This file is part of the CernVM File System.
 *
 * Swissknife command for performing catalog-level overlays of multiple
 * subdirectories from a CVMFS repository, similar to OverlayFS or how
 * container engines merge container image layers.
 *
 * The overlay tool merges the catalog entries of multiple layer subdirectories,
 * typically corresponding to container layers, and publishes the result as 
 * a new subdirectory in the CVMFS repository, typically corresponding to the flat 
 * root file system of the container image. 
 * 
 * The publish workflow is taken from swissknife ingest.
 */

#ifndef CVMFS_SWISSKNIFE_OVERLAY_H_
#define CVMFS_SWISSKNIFE_OVERLAY_H_

#include <map>
#include <string>
#include <vector>

#include "catalog.h"
#include "catalog_mgr_rw.h"
#include "directory_entry.h"
#include "swissknife.h"
#include "upload.h"
#include "xattr.h"

namespace swissknife {

/**
 * Represents a single entry in the merged overlay catalog, tracking
 * which layer it came from and its metadata.
 */
struct OverlayEntry {
  catalog::DirectoryEntry entry;
  XattrList xattrs;
  std::string path;       // relative path in the merged view
  std::string parent;     // parent directory relative path in the merged view
  bool is_whiteout;       // true if this is a whiteout marker
  bool is_opaque_dir;     // true if directory has opaque marker

  OverlayEntry()
      : is_whiteout(false), is_opaque_dir(false) {}
};

class CommandOverlay : public Command {
 public:
  virtual ~CommandOverlay() {}

  virtual std::string GetName() const { return "overlay"; }
  virtual std::string GetDescription() const {
    return "Merge multiple CVMFS subdirectory catalogs using overlay "
           "semantics and publish the result as a repository subdirectory";
  }
  virtual ParameterList GetParams() const;
  virtual int Main(const ArgumentList &args);

 private:
  /**
   * Recursively read all entries from a catalog rooted at the given path.
   * The entries are stored with paths relative to the layer root.
   * When a nested catalog mountpoint is encountered and repo_base/temp_dir
   * are non-empty, the nested catalog is loaded and recursed into.
   */
  bool ReadCatalogEntries(
      catalog::Catalog *catalog,
      const std::string &catalog_root_path,
      const std::string &relative_prefix,
      const std::string &repo_base,
      const std::string &temp_dir,
      std::map<std::string, OverlayEntry> *entries);

  /**
   * Check if a filename represents a whiteout file (.wh.<name>).
   */
  static bool IsWhiteoutFile(const std::string &name);

  /**
   * Get the original filename from a whiteout filename.
   * E.g., ".wh.foo" -> "foo"
   */
  static std::string GetWhiteoutTarget(const std::string &name);

  /**
   * Check if a filename represents an opaque directory marker (.wh..wh..opq).
   */
  static bool IsOpaqueMarker(const std::string &name);

  /**
   * Merge entries from a higher layer into the accumulated overlay map.
   * Implements overlay semantics: whiteout handling, opaque dirs, overrides.
   */
  void MergeLayer(
      const std::map<std::string, OverlayEntry> &layer_entries,
      std::map<std::string, OverlayEntry> *merged) const;

  /**
   * Publish the merged overlay entries into the repository under dest_path
   * using the WritableCatalogManager.  This adds directory and file entries
   * to the live catalog, then commits the changes to produce a new manifest.
   */
  bool PublishMergedEntries(
      catalog::WritableCatalogManager *catalog_mgr,
      const std::map<std::string, OverlayEntry> &merged,
      const std::string &dest_path) const;

  /**
   * Load a catalog from the repository for a given subdirectory path.
   * Handles both local and remote repositories.
   */
  catalog::Catalog *LoadCatalogForPath(
      const std::string &repo_base,
      const std::string &subdirectory,
      const std::string &temp_dir,
      const shash::Any &root_hash);

  /**
   * Find the catalog that contains the given layer path by walking the
   * nested catalog hierarchy.  The layer path may be deep inside a nested
   * catalog (e.g. /.layers/ab/abcdef.../layerfs where /.layers is a nested
   * catalog mountpoint).  Returns the catalog that can look up layer_path,
   * or NULL on failure.  The caller takes ownership of any additionally
   * loaded catalogs returned via loaded_catalogs.
   */
  catalog::Catalog *FindCatalogForLayer(
      const std::string &repo_base,
      const std::string &temp_dir,
      catalog::Catalog *root_catalog,
      const std::string &layer_path,
      std::vector<catalog::Catalog *> *loaded_catalogs);

  /**
   * Parse an OCI image config JSON file and inject Singularity
   * compatibility dotfiles (.singularity.d/) into the merged overlay
   * entries.  The generated files include the base environment,
   * action scripts, runscript (from Entrypoint/Cmd) and environment
   * variables (from Env).
   *
   * File content is uploaded through the spooler so that content hashes
   * are available for the catalog entries.
   *
   * Returns false on error.
   */
  bool InjectSingularityDotfiles(
      const std::string &oci_config_path,
      upload::Spooler *spooler,
      std::map<std::string, OverlayEntry> *merged);

  /**
   * Helper: create an OverlayEntry for a directory.
   */
  static OverlayEntry MakeDirEntry(const std::string &path,
                                   const std::string &parent);

  /**
   * Helper: create an OverlayEntry for a regular file, uploading
   * its content through the spooler and blocking until the content
   * hash is available.
   */
  static OverlayEntry MakeFileEntry(const std::string &path,
                                    const std::string &parent,
                                    const std::string &content,
                                    upload::Spooler *spooler);

  /**
   * Helper: create an OverlayEntry for a symlink.
   */
  static OverlayEntry MakeSymlinkEntry(const std::string &path,
                                       const std::string &parent,
                                       const std::string &target);

  /**
   * Shell-escape a string (escape backslash, double-quote, backtick, $).
   */
  static std::string ShellEscape(const std::string &s);

  /**
   * Quote a list of shell args.
   */
  static std::string ArgsQuoted(const std::vector<std::string> &args);

  /**
   * Generate the content of the runscript from OCI Entrypoint and Cmd.
   */
  static std::string GenerateRunscript(
      const std::vector<std::string> &entrypoint,
      const std::vector<std::string> &cmd);

  /**
   * Generate the content of env/10-docker2singularity.sh from OCI Env.
   */
  static std::string GenerateEnvScript(
      const std::vector<std::string> &env);
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_OVERLAY_H_

