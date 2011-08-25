#ifndef CVMFS_SYNC_CATALOG_H
#define CVMFS_SYNC_CATALOG_H 1

#include "catalog.h"
#include "cvmfs_sync_recursion.h"

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
		
		std::set<std::string> mPrelistingUpdates;
		
	public:
		CatalogHandler(const std::string &catalogDirectory, const std::string &unionDirectory, const std::string &dataDirectory, bool attachAll, std::set<std::string> immutables, const std::string &keyfile);
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
		
		bool precalculateListings();
		void commit();
		
		void setDirty(const std::string &path);
		
	private:
		bool initCatalogs(bool attach_all);
		
		bool attachCatalog(const std::string &path, int parentCatalogId);
		bool attachNestedCatalogsRecursively(const unsigned cat_id, const bool dirty);
		
		bool closeCatalog(const std::string &path);
		
		void createCatalogSnapshots();
		void createCatalogSnapshot(const std::string &path, const bool compat_catalog, const CatalogInfo &ci);
		
		void updatePrelistingBookkeeping(const std::string &path);
		
		bool removeEntry(DirEntry *entry);
		
		bool addEntry(DirEntry *entry);
		bool addEntry(DirEntry *entry, unsigned int hardlinkGroupId);
		
		unsigned int getNextFreeHardlinkGroupId(DirEntry *entry);
		
		inline std::string relativeToCatalogPath(const std::string &relativePath) const { return "/" + relativePath; }
		inline std::string getCatalogPathFromAbsolutePath(const std::string &absolutePath) const { return absolutePath.substr(mUnionDirectory.length()); }
		inline std::string getCatalogPath(const std::string &relativePath) const { return (relativePath.empty()) ? mCatalogDirectory + "/.cvmfscatalog.working" : mCatalogDirectory + "/" + relativePath + "/.cvmfscatalog.working"; }
		inline std::string getAbsolutePath(const std::string &relativePath) const { return mUnionDirectory + "/" + relativePath; }
	};
}

#endif
