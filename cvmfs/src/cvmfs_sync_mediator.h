#ifndef CVMFS_SYNC_MEDIATOR_H
#define CVMFS_SYNC_MEDIATOR_H

#include <list>
#include <string>
#include <map>
#include <stack>

#include "compat.h"
#include "cvmfs_sync_catalog.h"

namespace cvmfs {

class DirEntry;

typedef struct {
	DirEntry *masterFile;
	DirEntryList hardlinks;
} HardlinkGroup;

typedef std::map<uint64_t, HardlinkGroup> HardlinkGroupMap;

class SyncMediator {
private:
	typedef std::stack<HardlinkGroupMap> HardlinkGroupMapStack;
	typedef std::list<HardlinkGroup> HardlinkGroupList;
	
private:
	std::string mDataDirectory;
	
	HardlinkGroupMapStack mHardlinkStack;
	CatalogHandler *mCatalogHandler;
	
	DirEntryList mFileQueue;
	HardlinkGroupList mHardlinkQueue;
	
public:
	SyncMediator(CatalogHandler *catalogHandler, std::string dataDirectory);
	virtual ~SyncMediator();
	
	void add(DirEntry *entry);
	void touch(DirEntry *entry);
	void remove(DirEntry *entry);
	void replace(DirEntry *entry);
	
	void enterDirectory(DirEntry *entry);
	void leaveDirectory(DirEntry *entry);
	
	void commit();
	
private:
	void addDirectoryRecursively(DirEntry *entry);
	void removeDirectoryRecursively(DirEntry *entry);
	bool addDirectoryCallback(DirEntry *entry);
	
	void insertHardlink(DirEntry *entry);
	void insertExistingHardlink(DirEntry *entry);
	void completeHardlinks(DirEntry *entry);
	
	void addFile(DirEntry *entry);
	void removeFile(DirEntry *entry);
	void touchFile(DirEntry *entry);
	
	void addDirectory(DirEntry *entry);
	void removeDirectory(DirEntry *entry);
	void touchDirectory(DirEntry *entry);
	
	void createNestedCatalog(DirEntry *requestFile);
	void removeNestedCatalog(DirEntry *requestFile);
	
	void leaveAddedDirectory(DirEntry *entry);
	
	void compressAndHashFileQueue();
	void addFileQueueToCatalogs();
	
	inline bool addFileToDatastore(DirEntry *entry, hash::t_sha1 &hash) { return addFileToDatastore(entry, "", hash); }
	bool addFileToDatastore(DirEntry *entry, const std::string &suffix, hash::t_sha1 &hash);
	
	inline HardlinkGroupMap& getHardlinkMap() { return mHardlinkStack.top(); }
	
	void addHardlinkGroup(const HardlinkGroupMap &hardlinks);
};

}

#endif
