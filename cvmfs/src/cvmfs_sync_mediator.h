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
typedef std::list<DirEntry*> DirEntryList;

typedef struct {
	DirEntry *masterFile;
	DirEntryList hardlinks;
} HardlinkGroup;

typedef std::map<uint64_t, HardlinkGroup> HardlinkGroupMap;

// typedef struct {
// 	std::list<std::list<std::string> > addedFiles;
// 	std::list<std::string> removedFiles;
// 	
// 	std::list<std::string> addedDirectories;
// 	std::list<std::string> touchedDirectories;
// 	std::list<std::string> removedDirectories;
// 	
// 	std::list<std::string> addedNestedCatalogs;
// 	std::list<std::string> removedNestedCatalogs;
// } Changeset;

class SyncMediator {
private:
	typedef std::stack<HardlinkGroupMap> HardlinkGroupMapStack;
	
private:
	HardlinkGroupMapStack mHardlinkStack;
	CatalogHandler *mCatalogs;
	
public:
	SyncMediator();
	virtual ~SyncMediator();
	
	void add(DirEntry *entry);
	void touch(DirEntry *entry);
	void remove(DirEntry *entry);
	void replace(DirEntry *entry);
	
	void enterDirectory(DirEntry *entry);
	void leaveDirectory(DirEntry *entry);
	
	inline Changeset getChangeset() const { return mChangeset; }
	
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
	
	inline HardlinkGroupMap& getHardlinkMap() { return mHardlinkStack.top(); }
	
	void addHardlinkGroup(const HardlinkGroupMap &hardlinks);
};

}

#endif
