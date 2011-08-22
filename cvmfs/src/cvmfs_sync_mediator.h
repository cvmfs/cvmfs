#ifndef CVMFS_SYNC_MEDIATOR_H
#define CVMFS_SYNC_MEDIATOR_H

#include <list>
#include <string>
#include <map>
#include <stack>

#include "compat.h"

//#include "cvmfs_sync_mediator"

namespace cvmfs {

class DirEntry;
	
typedef struct {
	DirEntry *masterFile;
	std::list<DirEntry*> hardlinks;
} HardlinkGroup;

typedef std::map<uint64_t, HardlinkGroup> HardlinkGroupMap;
typedef std::stack<HardlinkGroupMap> HardlinkGroupMapStack;

class SyncMediator {
private:
	HardlinkGroupMapStack mHardlinkStack;
	
public:
	SyncMediator();
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
	bool removeDirectoryCallback(DirEntry *entry);
	
	void insertHardlink(DirEntry *entry);
	void insertExistingHardlink(DirEntry *entry);
	void completeHardlinks(DirEntry *entry);
	
	void addFile(DirEntry *entry);
	void removeFile(DirEntry *entry);
	void touchFile(DirEntry *entry);
	
	void addDirectory(DirEntry *entry);
	void removeDirectory(DirEntry *entry);
	void touchDirectory(DirEntry *entry);
	
	void addHardlinkGroup(const HardlinkGroupMap &hardlinks);
};

}

#endif
