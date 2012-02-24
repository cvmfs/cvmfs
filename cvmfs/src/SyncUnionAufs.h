/**
 *  This is a concrete implementation of the abstract class SyncUnion
 *  You can use it to sync a CVMFS repository by the help of an AUFS
 *  union file system.
 *
 *  Developed by René Meusel 2011 at CERN
 *  rene@renemeusel.de
 */

#ifndef SYNC_UNION_AUFS_H
#define SYNC_UNION_AUFS_H

#include <string>
#include <set>
#include <list>
#include <map>

#include "platform.h"

#include "SyncUnion.h"
#include "SyncMediator.h"

namespace cvmfs {

/**
 *  syncing a CVMFS repository by the help of an overlayed AUFS 1.x read-write volume
 *  this class basically implements the interface defined by UnionSync::
 */
class SyncUnionAufs :
 	public SyncUnion {
 private:
	std::set<std::string> mIgnoredFilenames;
	std::string mWhiteoutPrefix;

 public:
	SyncUnionAufs(SyncMediator *mediator,
  	            const std::string &repository_path,
  	            const std::string &union_path,
                const std::string &overlay_path);

	bool DoYourMagic();

 protected:
	inline bool IsWhiteoutEntry(const SyncItem &entry) const {
	  return (entry.GetFilename().substr(0, mWhiteoutPrefix.length()) == mWhiteoutPrefix);
	}

	inline bool IsOpaqueDirectory(const SyncItem *directory) const {
	  return file_exists(directory->GetOverlayPath() + "/.wh..wh..opq");
	}

	inline std::string UnwindWhiteoutFilename(const std::string &filename) const {
	  return filename.substr(mWhiteoutPrefix.length());
	}

	inline std::set<std::string> GetIgnoredFilenames() const { return mIgnoredFilenames; };
};

} // namespace cvmfs

#endif /* SYNC_UNION_AUFS_H */
