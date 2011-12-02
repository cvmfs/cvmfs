/** DEPRECATED but still needed for code reusage... will go away soon */

/**
 *  This class is responsible to actually change the file system
 *  information in the CVMFS catalogs
 *  It is basically just a wrapper around the catalog API of CVMFS.
 *  Furthermore it handles catalog loading and snapshotting.
 *
 *  TODO: lazy attach, and creation of nested catalogs currently
 *        screws up things. I did not develop this to stable as
 *        this mechanics probably change with the pending structural
 *        changes (René)
 *
 *  Developed by René Meusel 2011 at CERN
 *  based on code written by Jakob Blomer 2009 at CERN
 */

#ifndef CVMFS_SYNC_CATALOG_H
#define CVMFS_SYNC_CATALOG_H 1

#include "catalog.h"
#include "cvmfs_sync_recursion.h"
#include "cvmfs_sync.h"

#include <string>
#include <map>
#include <set>

namespace cvmfs {
	typedef struct {
	   bool dirty;
	   int id;
	   int parent_id;
	} CatalogInfo;
	typedef std::map<std::string, CatalogInfo> CatalogMap;
	
	class CatalogHandler {
	private:
		CatalogMap mOpenCatalogs;
		std::string mCatalogDirectory;
		std::string mDataDirectory;
		std::string mUnionDirectory;
		std::set<std::string> mImmutables;
		
		std::string mKeyfile;
		bool mLazyAttach;
		bool mDryRun;
		
		std::set<std::string> mPrelistingUpdates;
		
	public:
		CatalogHandler(const SyncParameters *parameters);
		virtual ~CatalogHandler();
		
		void mergeCatalog(const std::string &path);
		
		bool createNestedCatalog(const std::string &relativeCatalogPath);
		void removeNestedCatalog(const std::string &relativeCatalogPath);
		
		inline bool isLoaded(const std::string &path) const { return mOpenCatalogs.find(path) != mOpenCatalogs.end(); }
		
		bool removeFile(DirEntry *entry);
		bool removeDirectory(DirEntry *entry);
		
		bool addDirectory(DirEntry *entry);
		bool addFile(DirEntry *entry);
		bool addHardlinkGroup(DirEntryList group);
		
		bool touchFile(DirEntry *entry);
		bool touchDirectory(DirEntry *entry);
		
		bool precalculateListings();
		void commit();
		
		void setDirty(const std::string &path);
		
	private:
		bool initCatalogs();
		
		bool attachCatalog(const std::string &path, int parentCatalogId);
		bool attachNestedCatalogsRecursively(const unsigned cat_id, const bool dirty);
      bool checkOrAttachCatalogs(const std::string &relativePath);
      bool attachCatalogsForPath(const std::string &relativePath);
      
      typedef enum {
         ppNotPresent,
         ppPresent,
         ppNotAttachedNestedCatalog
      } PathPresent;
      
      /**
       *  This method is only used if catalogs are attached lazily.
       *  it checks if the catalog containing the given path is already loaded and returns some
       *  status information if yes.
       *  @param path the path to check (only parent path for files EXCLUDING the file name)
       *  @param catalogId [out] this pointer is set to the found catalog id or the parent catalog id
       *                         in case we found the root of a not loaded nested catalog
       *  @return a status flag declaring the result of the catalog search
       */
      PathPresent isCatalogForPathPresent(const std::string &path, int *catalogId) const;
		
		bool closeCatalog(const std::string &path);
		
		void createCatalogSnapshots();
		void createCatalogSnapshot(const std::string &path, const bool compat_catalog, const CatalogInfo &ci);
		
		void updatePrelistingBookkeeping(const std::string &path);
		
		bool removeEntry(DirEntry *entry);
		
		bool addEntry(DirEntry *entry, unsigned int hardlinkGroupId = 0); // 0 means: no hardlink group
		
		unsigned int getNextFreeHardlinkGroupId(DirEntry *entry);
		
		bool lookup(const DirEntry *entry, catalog::t_dirent &cdirent) const;
		
		inline std::string relativeToCatalogPath(const std::string &relativePath) const { return "/" + relativePath; }
		inline std::string getCatalogPathFromAbsolutePath(const std::string &absolutePath) const { return absolutePath.substr(mUnionDirectory.length()); }
		inline std::string getCatalogPath(const std::string &relativePath) const { return (relativePath.empty()) ? mCatalogDirectory + "/.cvmfscatalog.working" : mCatalogDirectory + "/" + relativePath + "/.cvmfscatalog.working"; }
		inline std::string getAbsolutePath(const std::string &relativePath) const { return mUnionDirectory + "/" + relativePath; }
	};
}

#endif
