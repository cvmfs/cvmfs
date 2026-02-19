/**
 * This file is part of the CernVM File System.
 *
 * Swissknife command for performing catalog-level overlays of multiple
 * subdirectories from a CVMFS repository, similar to how OverlayFS or
 * container engines merge container image layers.
 *
 * The overlay tool merges the catalog entries of multiple layer subdirectories
 * and publishes the result as a new subdirectory in the CVMFS repository,
 * following the same publish workflow as swissknife sync/ingest.
 */

#ifndef CVMFS_SWISSKNIFE_OVERLAY_H_
#define CVMFS_SWISSKNIFE_OVERLAY_H_

#include <map>
#include <string>
#include <vector>

#include "catalog.h"
#include "catalog_mgr_rw.h"
#include "directory_entry.h"
#include "shortstring.h"
#include "swissknife.h"
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
   * Compute SHA-256 cache key from the ordered list of layer paths.
   */
  std::string ComputeCacheKey(const std::vector<std::string> &layers) const;

  /**
   * Check if a valid cached catalog exists for the given cache key.
   * The cached catalog is stored as a SQLite database file containing
   * the merged overlay entries, so that re-running the same layer
   * combination can skip the merge computation.
   */
  bool CheckCachedMerge(const std::string &cache_dir,
                        const std::string &cache_key,
                        std::map<std::string, OverlayEntry> *merged) const;

  /**
   * Store the merged entry map in the cache with the given key.
   * Creates a SQLite catalog database file containing all merged entries.
   */
  bool StoreMergeInCache(
      const std::string &cache_dir,
      const std::string &cache_key,
      const std::map<std::string, OverlayEntry> &merged) const;

  /**
   * Recursively read all entries from a catalog rooted at the given path.
   * The entries are stored with paths relative to the layer root.
   */
  bool ReadCatalogEntries(
      catalog::Catalog *catalog,
      const std::string &catalog_root_path,
      const std::string &relative_prefix,
      std::map<std::string, OverlayEntry> *entries) const;

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
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_OVERLAY_H_

