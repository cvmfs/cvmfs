#include "cvmfs_sync_catalog.h"

#include <iostream>
#include <sstream>
#include <stdlib.h>

#include "cvmfs_sync_aufs.h"

#include "hash.h"
#include "util.h"

extern "C" {
   #include "compression.h"
}

using namespace std;
using namespace cvmfs;

CatalogHandler::CatalogHandler(const SyncParameters *parameters) {
	mCatalogDirectory = canonical_path(parameters->dir_catalogs);
	mUnionDirectory   = canonical_path(parameters->dir_shadow);
	mDataDirectory    = canonical_path(parameters->dir_data);
	mImmutables       = parameters->immutables;
	mKeyfile          = parameters->keyfile;
	mLazyAttach       = parameters->lazy_attach;
	mDryRun           = parameters->dry_run;
	
	// initialize the preListing bookkeeping set
	mPrelistingUpdates.insert("");
	
	// load catalogs
	initCatalogs();
}

CatalogHandler::~CatalogHandler() {
	catalog::fini();
}

bool CatalogHandler::initCatalogs() {

	const string clg_path = "";
	
	// init the database
	if (!catalog::init(getuid(), getgid())) {
		printError("could not init SQLite");
		exit(1);
	}
	
	// attach root catalog (no parent [-1])
	attachCatalog(clg_path, -1);

	/* If this is a new catalog, insert root node */
	hash::t_md5 rhash(catalog::mangled_path(""));
	catalog::t_dirent d;
	if (!catalog::lookup(rhash, d)) {
		cout << "creating root entry" << endl;

		PortableStat64 info;
		if (!get_file_info(mUnionDirectory, &info)) {
			return false;
		}

		d = catalog::t_dirent(0, "", "", catalog::DIR, 0, info.st_mode, info.st_size, 
		info.st_mtime, hash::t_sha1());
		if (!catalog::insert(rhash, hash::t_md5(), d)) {
			cerr << "could not insert root hash" << endl;
			return false;
		}
	}

	if (not mLazyAttach) {
		if (!attachNestedCatalogsRecursively(0, false)) {
			printError("could not init all nested catalogs");
			return false;
		}
	}

	return true;
}

bool CatalogHandler::attachCatalog(const string &relativePath, int parentCatalogId) {
	std::string catalogPath = getCatalogPath(relativePath);
	std::string unionPath = getAbsolutePath(relativePath);
	
	cout << "Attaching " << catalogPath << endl;
	if (!catalog::attach(catalogPath, "", mDryRun, false))
	{
		stringstream ss;
		ss << "unable to attach catalog " << catalogPath;
		printError(ss.str());
		return false;
	}

	CatalogInfo ci;
	ci.dirty = false;
	ci.id = catalog::get_num_catalogs() - 1;
	ci.parent_id = parentCatalogId;
	mOpenCatalogs[relativePath] = ci;
	
	return true;
}

bool CatalogHandler::attachNestedCatalogsRecursively(const unsigned cat_id, const bool dirty) {
	vector<string> ls;
	if (not catalog::ls_nested(cat_id, ls)) {
		return false;
	}
	
	vector<string>::iterator i;
	for (i = ls.begin(); i != ls.end(); ++i) {
		bool skip = false;
		for (set<string>::const_iterator j = mImmutables.begin(), jEnd = mImmutables.end(); j != jEnd; ++j) {
			size_t cut = mUnionDirectory.length();
			if ((j->length() >= cut) && (i->find(j->substr(cut), 0) == 0)) {
				skip = true;
				break;
			}
		}
		if (skip) {
			cout << "Skipping catalog in immutable directory " << *i << endl;
			continue;
		}
		
		attachCatalog(i->substr(catalog::get_root_prefix().length()), cat_id);

		if (!attachNestedCatalogsRecursively(catalog::get_num_catalogs() - 1, dirty)) {
			return false;
		}
	}
	
	return true;
}

bool CatalogHandler::closeCatalog(const std::string &path) {
	return (mOpenCatalogs.erase(path) == 1); // erase returns 1 if something was deleted
}


void CatalogHandler::mergeCatalog(const string &path) {
	// check if catalog is loaded
	if (not isLoaded(path)) {
		stringstream ss;
		ss << "there is no catalog loaded at " << path;
		printWarning(ss.str());
		return;
	}
	
	CatalogInfo catalogToDelete = mOpenCatalogs[path];
	string mimick_path = mCatalogDirectory + path;
	if (catalog::merge(catalog::mangled_path(path))) {
		unlink((mimick_path + "/.cvmfscatalog").c_str());
		unlink((mimick_path + "/.cvmfscatalog.working").c_str());
		unlink((mimick_path + "/.cvmfschecksum").c_str());
		unlink((mimick_path + "/.cvmfschecksum.sig").c_str());
		unlink((mimick_path + "/.cvmfscatalog.publisher.x509.pem").c_str());
		unlink((mimick_path + "/.growfschecksum").c_str());
		unlink((mimick_path + "/.growfsdir").c_str());
		unlink((mimick_path + "/.growfsdir.zgfs").c_str());
		unlink((mimick_path + "/data").c_str());
		unlink((mimick_path + "/.cvmfswhitelist").c_str());
		unlink((mimick_path + "/.cvmfspublished").c_str());
		
		closeCatalog(path);

		/* Fix mOpenCatalog ids */
		for (CatalogMap::iterator i = mOpenCatalogs.begin(), iEnd = mOpenCatalogs.end(); i != iEnd; ++i) {
			bool isDirty = false;
			
			if (i->second.id > catalogToDelete.id) {
				i->second.id = i->second.id - 1;
				isDirty = true;
			}

			if (i->second.parent_id > catalogToDelete.id) {
				i->second.parent_id = i->second.parent_id - 1;
				isDirty = true;
			} else if (i->second.parent_id == catalogToDelete.id) {
				i->second.parent_id = catalogToDelete.parent_id;
				isDirty = true;
			}
			
			if (isDirty) {
				setDirty(i->first);
			}
		}

		/* Remove mimick directories */
		while (is_empty_dir(mimick_path)) {
			if (rmdir(mimick_path.c_str()) != 0) {
				stringstream ss;
				ss << "could not delete empty path " << mimick_path;
				printWarning(ss.str());
				return;
			}
			mimick_path = get_parent_path(mimick_path);
		}
	} else {
		stringstream ss;
		ss << "could not merge catalogs at " << path;
		printWarning(ss.str());
	}
}

bool CatalogHandler::createNestedCatalog(const string &relativeCatalogPath) {
	/* Mimick directory structure in /pub/catalogs/ */
	if (!mkdir_deep(mCatalogDirectory + "/" + relativeCatalogPath, plain_dir_mode)) {
		stringstream ss;
		ss << "cannot create catalog directory structure " << relativeCatalogPath;
		printWarning(ss.str());
		return false;
	}

	const string clg_path = relativeToCatalogPath(relativeCatalogPath);
	const hash::t_md5 md5(catalog::mangled_path(clg_path));
	const hash::t_md5 p_md5(catalog::mangled_path(get_parent_path(clg_path)));
	catalog::t_dirent d;
	catalog::t_dirent n;

	/* Find the path in current catalogs */
	if (!catalog::lookup_unprotected(md5, d)) {
		stringstream ss;
		ss << "cannot create nested catalog in " << relativeCatalogPath << ", directory is dangling";
		printWarning(ss.str());
		return false;
	}
	n = d;
	d.flags |= catalog::DIR_NESTED;
	n.catalog_id = catalog::get_num_catalogs();
	n.flags |= catalog::DIR_NESTED_ROOT;
	const string cat_path = mCatalogDirectory + clg_path + "/.cvmfscatalog.working";
	cout << "Creating new nested catalog " << cat_path << endl;

	/* Move entries in nested catalog */
	if (!catalog::update_unprotected(md5, d) ||
		!catalog::attach(cat_path, "", false, true) ||
		!catalog::set_root_prefix(clg_path, n.catalog_id),
		!catalog::insert_unprotected(md5, p_md5, n) ||
		!catalog::relink_unprotected(catalog::mangled_path(clg_path), catalog::mangled_path(clg_path)) ||
		!catalog::register_nested(d.catalog_id, catalog::mangled_path(clg_path)))
	{
		stringstream ss;
		ss << "error while creating nested catalog " << cat_path;
		printWarning(ss.str());
		return false;
	}

	/* New one is dirty, we want to snapshot it later */
	CatalogInfo ci;
	ci.dirty = true;
	ci.id = catalog::get_num_catalogs()-1;
	ci.parent_id = d.catalog_id;

	/* Move registerd catalogs from parent to nested */
	vector<string> parent_nested;
	if (!catalog::ls_nested(ci.parent_id, parent_nested)) {
		printWarning("failed to list nested catalogs of parent catalog");
		return false;
	}
	
	for (unsigned j = 0; j < parent_nested.size(); ++j) {
		if (parent_nested[j].find(relativeCatalogPath + "/", 0) == 0) {
			hash::t_sha1 nested_sha1;
			if (!catalog::lookup_nested_unprotected(ci.parent_id, parent_nested[j], nested_sha1)) {
				printWarning("failed to lookup nested catalog of parent catalog");
				continue;
			}
			if (!catalog::register_nested(ci.id, parent_nested[j]) || 
				!catalog::update_nested_sha1(ci.id, parent_nested[j], nested_sha1) ||
				!catalog::unregister_nested(ci.parent_id, parent_nested[j]))
			{
				printWarning("failed to relink nested catalog");
				continue;
			}
		}
	}

	/* Update open catalogs */
	for (CatalogMap::iterator j = mOpenCatalogs.begin(), jEnd = mOpenCatalogs.end(); j != jEnd; ++j) 
	{
		if ((j->second.parent_id == ci.parent_id) && j->first.find(relativeCatalogPath + "/", 0) == 0)
		{
			j->second.parent_id = ci.id;
		}
	}
	
	mOpenCatalogs[clg_path] = ci;
	setDirty(get_parent_path(clg_path));
	
	return true;
}

void CatalogHandler::removeNestedCatalog(const std::string &relativeCatalogPath) {
	const string clg_path = relativeToCatalogPath(relativeCatalogPath);
	
	cout << "Merging catalogs at " << clg_path << endl;
	mergeCatalog(clg_path);

	for (int i = 0; i < catalog::get_num_catalogs(); ++i) {
		catalog::transaction(i);
	}
}

void CatalogHandler::setDirty(const string& path) {
	/* find hosting catalog of path (and all parent ones on the way) */
	bool found = false;
	for (CatalogMap::iterator i = mOpenCatalogs.begin(), iEnd = mOpenCatalogs.end(); i != iEnd; ++i)
	{
		if (path.find(i->first) == 0) {
			i->second.dirty = true;
			found = true;
		}
	}

	if (!found) {
		stringstream ss;
		ss << "path " << path << " is not on any open catalog";
		printWarning(ss.str());
	}
}

bool CatalogHandler::removeFile(DirEntry *entry) {
	return removeEntry(entry);
}

bool CatalogHandler::removeDirectory(DirEntry *entry) {
	bool success = removeEntry(entry);
	
	if (success) {
		// remove the deleted directory from the prelisting update log
		mPrelistingUpdates.erase(relativeToCatalogPath(entry->getRelativePath()));
	}
	
	return success;
}

bool CatalogHandler::removeEntry(DirEntry *entry) {
	const string catalogPath = relativeToCatalogPath(entry->getRelativePath());
	const string parentPath = relativeToCatalogPath(entry->getParentPath());
	
	catalog::t_dirent d;
	hash::t_md5 md5(catalog::mangled_path(catalogPath));
	if (!catalog::lookup_unprotected(md5, d)) {
		stringstream ss;
		ss << catalogPath << " is not in the catalogs";
		printWarning(ss.str());
		return false;
	} else {
		if (!catalog::unlink_unprotected(md5, d.catalog_id)) {
			stringstream ss;
			ss << "could not remove " << catalogPath << " from catalog";
			printWarning(ss.str());
			return false;
		}
	}

	updatePrelistingBookkeeping(parentPath);	
	setDirty(parentPath);
	
	return true;
}

bool CatalogHandler::addDirectory(DirEntry *entry) {
	return addEntry(entry);
}

bool CatalogHandler::addFile(DirEntry *entry) {
	return addEntry(entry);
}

bool CatalogHandler::addHardlinkGroup(DirEntryList group) {
	// sanity check
	if (group.size() == 0) {
		printWarning("cannot add empty hardlink group");
		return false;
	}
	
	// get a valid hardlink group id for the catalog the group will end up in
	unsigned int hardlinkGroupId = getNextFreeHardlinkGroupId(group.front());
	if (hardlinkGroupId < 1) {
		return false;
	}
	
	// add the file entries to the catalog
	DirEntryList::iterator i;
	DirEntryList::const_iterator end;
	for (i = group.begin(), end = group.end(); i != end; ++i) {
		addEntry(*i, hardlinkGroupId);
	}
	
	return true;
}

bool CatalogHandler::touchFile(DirEntry *entry) {
	// TODO: implement this (should not be called at the momemt)
	return true;
}

bool CatalogHandler::touchDirectory(DirEntry *entry) {
	// TODO: implement this (has no real effect at the moment)
	return true;
}

unsigned int CatalogHandler::getNextFreeHardlinkGroupId(DirEntry *entry) {
	hash::t_md5 p_md5(catalog::mangled_path(relativeToCatalogPath(entry->getParentPath())));
	
	catalog::t_dirent d;
	if (not catalog::lookup_unprotected(p_md5, d)) {
		stringstream ss;
		ss << "cannot find a catalog for directory " << entry->getParentPath();
		printWarning(ss.str());
		return 0;
	}
	
	unsigned int maxGroupId;
	if (not catalog::getMaximalHardlinkGroupId(d.catalog_id, maxGroupId)) {
		stringstream ss;
		ss << "could not retrieve next hardlink group id from catalog " << d.catalog_id;
		printWarning(ss.str());
		return 0;
	}
	
	return maxGroupId + 1;
}

bool CatalogHandler::isPartOfHardlinkGroup(const DirEntry *entry) const {
	catalog::t_dirent d;
	lookup(entry, d);
	
	// check if file is part of hard link group
	return (d.inode != 0);
}

uint64_t CatalogHandler::getHardlinkGroup(const DirEntry *entry) const {
	catalog::t_dirent d;
	lookup(entry, d);
	
	// check if file is part of hard link group
	return d.inode != 0;
}

bool CatalogHandler::lookup(const DirEntry *entry, catalog::t_dirent &cdirent) const {
	hash::t_md5 md5(catalog::mangled_path(relativeToCatalogPath(entry->getRelativePath())));
	
	if (not catalog::lookup_unprotected(md5, cdirent)) {
		stringstream ss;
		ss << "failed to lookup " << entry->getRelativePath() << " in catalogs";
		printWarning(ss.str());
		return false;
	}
	
	return true;
}

bool CatalogHandler::addEntry(DirEntry *entry) {
	return addEntry(entry, 0); // 0 means: no hardlink group
}

bool CatalogHandler::addEntry(DirEntry *entry, unsigned int hardlinkGroupId) {
	const string catalogPath = relativeToCatalogPath(entry->getRelativePath());
	const string parentPath = relativeToCatalogPath(entry->getParentPath());
	
	// compute MD5 hashes of pathes
	catalog::t_dirent d;
	hash::t_md5 md5(catalog::mangled_path(catalogPath));
	hash::t_md5 p_md5(catalog::mangled_path(parentPath));

	// check if new entry exists
	if (catalog::lookup_unprotected(md5, d)) {
		stringstream ss;
		ss << catalogPath << " is already in catalog";
		printWarning(ss.str());
		return false;
	}
	
	// check that parent directory exists
	if (not catalog::lookup_unprotected(p_md5, d)) {
		stringstream ss;
		ss << catalogPath << " is dangling";
		printWarning(ss.str());
		return false;
	}
	
	// build up new entry in catalog
	int parentCatalogId = d.catalog_id;
	int flags = 0;
	string symlink = ""; 
	PortableStat64 info = entry->getUnionStat();
	
	if (entry->isDirectory()) {
		flags = catalog::DIR;
	} else
	
	if (entry->isSymlink()) {
		flags = catalog::FILE | catalog::FILE_LINK;
		char slnk[PATH_MAX+1];
		ssize_t l = readlink((entry->getUnionPath()).c_str(), slnk, PATH_MAX);
		if (l >= 0) {
			slnk[l] = '\0';
			symlink = slnk;
		} else {
			stringstream ss;
			ss << "could not read link " << catalogPath;
			printWarning(ss.str());
			return false;
		}
	} else
	
	if (entry->isRegularFile()) {
		flags = catalog::FILE;
		if (not entry->hasContentHash()) {
			stringstream ss;
			ss << "no content hash for file " << catalogPath << " provided";
			printWarning(ss.str());
			return false;
		}
		
	} else {
		stringstream ss;
		ss << "cannot add unregcognized file format at " << catalogPath;
		printWarning(ss.str());
		return false;
	}
	
	// create t_dirent structure to insert into catalog
	flags = catalog::setLinkcountInFlags(flags, info.st_nlink);
	catalog::t_dirent new_d(parentCatalogId, entry->getFilename(), symlink, flags, hardlinkGroupId, info.st_mode, info.st_size, info.st_mtime, entry->getContentHash());
	
	// add the stuff to the catalog
	if (not catalog::insert_unprotected(md5, p_md5, new_d)) {
		stringstream ss;
		ss << "could not insert directory " << catalogPath;
		printWarning(ss.str());
		return false;
	}
	
	updatePrelistingBookkeeping(catalogPath);
	setDirty(catalogPath);
	
	return true;
}

void CatalogHandler::updatePrelistingBookkeeping(const string &path) {
	string workingPath = path;
	
	while (mPrelistingUpdates.insert(workingPath).second) {
		workingPath = get_parent_path(workingPath);
	}
}

bool CatalogHandler::precalculateListings() {
   // TODO: implement me
	return true;
}

void CatalogHandler::commit() {
	for (int i = 0; i < catalog::get_num_catalogs(); ++i) {
		catalog::commit(i);
	}
	
	createCatalogSnapshots();
}

void CatalogHandler::createCatalogSnapshots() {
	cout << "creating snapshots... " << endl;
	
	for (CatalogMap::const_reverse_iterator i = mOpenCatalogs.rbegin(), iend = mOpenCatalogs.rend(); i != iend; ++i)
	{
		if (i->second.dirty) {
			createCatalogSnapshot(i->first, false, i->second);
			
			
			// if (fbookkeeping.is_open()) {
			// 				const string real_path = dir_catalogs + abs2clg_path(i->first, dir_shadow) + "/.cvmfscatalog";
			// 				if (bookkeeping.find(real_path) == bookkeeping.end()) {
			// 					fbookkeeping << real_path << endl;
			// 				}
			// 			}
		}
	}
	
	cout << endl;

}

void CatalogHandler::createCatalogSnapshot(const string &path, const bool compat_catalog, const CatalogInfo &ci) {
	cout << "Creating catalog snapshot at " << path << endl;
	const string clg_path = path;
	const string cat_path = mCatalogDirectory + "/" + clg_path;

	/* Data symlink, whitelist symlink */  
	string backlink = "../";
	string parent = get_parent_path(cat_path);
	while (parent != get_parent_path(mDataDirectory)) {
		if (parent == "") {
			printWarning("cannot find data dir");
			break;
		}
		parent = get_parent_path(parent);
		backlink += "../";
	}
   
	const string lnk_path_data = cat_path + "/data";
	const string lnk_path_whitelist = cat_path + "/.cvmfswhitelist";
	const string backlink_data = backlink + get_file_name(mDataDirectory);
	const string backlink_whitelist = backlink + get_file_name(mCatalogDirectory) + "/.cvmfswhitelist";

	PortableStat64 info;
	if (portableLinkStat64(lnk_path_data.c_str(), &info) != 0) 
	{
		if (symlink(backlink_data.c_str(), lnk_path_data.c_str()) != 0) {
			printWarning("cannot create catalog store -> data store symlink");
		}
	}
	
	/* Don't make the symlink for the root catalog */
	if ((portableLinkStat64(lnk_path_whitelist.c_str(), &info) != 0) && (get_parent_path(cat_path) != get_parent_path(mDataDirectory)))
	{
		if (symlink(backlink_whitelist.c_str(), lnk_path_whitelist.c_str()) != 0) {
			printWarning("cannot create whitelist symlink");
		}
	}

   
		//    /* Compat catalog */
		//    if (compat_catalog) {
		//       cout << "Creating growfscatalog..." << endl;
		//       if (!catalog::create_compat(cat_path, clg_path)) {
		// stringstream ss;
		// ss << "could not create CVMFS1 catalog at " << cat_path;
		//          printWarning(ss.str());
		//       } else {
		//          /* Copy compat catalog into data store */
		//          const string gfs_files[3] = {"/.growfsdir", "/.growfsdir.zgfs", "/.growfschecksum"};
		//          hash::t_sha1 sha1;
		//          for (unsigned i = 0; i < 3; ++i) {
		//             const string src_path = cat_path + gfs_files[i];
		//             if (sha1_file(src_path.c_str(), sha1.digest) == 0) {
		//                const string dst_path = dir_data + "/" + sha1.to_string().substr(0, 2) + "/" +
		//                sha1.to_string().substr(2);
		//                if (file_copy(src_path.c_str(), dst_path.c_str()) != 0) {
		// 			stringstream ss;
		// 			ss << "could not store " << src_path << " in " << dst_path;
		// 	         printWarning(ss.str());
		//                }
		//             } else {
		// 		stringstream ss;
		// 		ss << "could not checksum " << src_path;
		//          printWarning(ss.str());
		//             }
		//          }
		//       }
		//    }

	/* Last-modified time stamp */
	if (!catalog::update_lastmodified(ci.id)) {
		printWarning("Warning, failed to update last modified time stamp");
	}
	
	/* Current revision */
	if (!catalog::inc_revision(ci.id)) {
		printWarning("Warning, failed to increase revision");
	}
	
	/* Previous revision */
	map<char, string> ext_chksum;
	if (parse_keyval(cat_path + "/.cvmfspublished", ext_chksum)) {
		map<char, string>::const_iterator i = ext_chksum.find('C');
		if (i != ext_chksum.end()) {
			hash::t_sha1 sha1_previous;
			sha1_previous.from_hash_str(i->second);
			if (!catalog::set_previous_revision(ci.id, sha1_previous)) {
				stringstream ss;
				ss << "failed store previous catalog revision " << sha1_previous.to_string();
				printWarning(ss.str());
			}
		} else {
			printWarning("failed to find catalog SHA1 key in .cvmfspublished");
		}
	}

	/* Compress catalog */
	const string src_path = cat_path + "/.cvmfscatalog.working";
	const string dst_path = mDataDirectory + "/txn/compressing.catalog";
	//const string dst_path = cat_path + "/.cvmfscatalog";
	hash::t_sha1 sha1;
	FILE *fsrc = NULL, *fdst = NULL;
	int fd_dst;
	if (!(fsrc = fopen(src_path.c_str(), "r")) ||
		((fd_dst = open(dst_path.c_str(), O_CREAT | O_TRUNC | O_RDWR, plain_file_mode)) < 0) ||
		!(fdst = fdopen(fd_dst, "w")) ||
		(compress_file_fp_sha1(fsrc, fdst, sha1.digest) != 0))
	{
		stringstream ss;
		ss << "could not compress catalog " << src_path;
		printWarning(ss.str());

	} else {
		const string sha1str = sha1.to_string();
		const string hash_name = sha1str.substr(0, 2) + "/" + sha1str.substr(2) + "C";
		const string cache_path = mDataDirectory + "/" + hash_name;
		if (rename(dst_path.c_str(), cache_path.c_str()) != 0) {
			stringstream ss;
			ss << "could not store catalog in data store as " << cache_path;
			printWarning(ss.str());
		}
		const string entry_path = cat_path + "/.cvmfscatalog"; 
		unlink(entry_path.c_str());
		if (symlink(("data/" + hash_name).c_str(), entry_path.c_str()) != 0) {
			stringstream ss;
			ss << "could not create symlink to catalog " << cache_path;
			printWarning(ss.str());
		}
	}
	if (fsrc) fclose(fsrc);
	if (fdst) fclose(fdst);

	/* Remove pending certificate */
	unlink((cat_path + "/.cvmfspublisher.x509").c_str());   

	/* Create extended checksum */
	FILE *fpublished = fopen((cat_path + "/.cvmfspublished").c_str(), "w");
	if (fpublished) {
		string fields = "C" + sha1.to_string() + "\n";
		hash::t_md5 md5(catalog::mangled_path(clg_path));
		fields += "R" + md5.to_string() + "\n";

		/* Mucro catalogs */
		catalog::t_dirent d;
		if (!catalog::lookup_unprotected(md5, d)) {
			printWarning("failed to find root entry");
		}
		fields += "L" + d.checksum.to_string() + "\n";
		const uint64_t ttl = catalog::get_ttl(catalog::lookup_catalogid_unprotected(md5));
		ostringstream strm_ttl;
		strm_ttl << ttl;
		fields += "D" + strm_ttl.str() + "\n";

		/* Revision */
		ostringstream strm_revision;
		strm_revision << catalog::get_revision();
		fields += "S" + strm_revision.str() + "\n";

		if (fwrite(&(fields[0]), 1, fields.length(), fpublished) != fields.length()) {
			printWarning("failed to write extended checksum");
		}
		fclose(fpublished);
		
	} else {
		printWarning("failed to write extended checksum");
	}
   
	/* Update registered catalog SHA1 in nested catalog */
	if (ci.parent_id >= 0) {
		cout << "updating nested catalog link" << endl;
		
		if (!catalog::update_nested_sha1(ci.parent_id, catalog::mangled_path(clg_path), sha1)) {
			stringstream ss;
			ss << "failed to register modified catalog at " << clg_path << " in parent catalog";
			printWarning(ss.str());
		}
	}

	/* Compress and write SHA1 checksum */
	char chksum[40];
	int lchksum = 40;
	memcpy(chksum, &((sha1.to_string())[0]), 40);
	void *compr_buf = NULL;
	size_t compr_size;
	if (compress_mem(chksum, lchksum, &compr_buf, &compr_size) != 0) {
		printWarning("could not compress catalog checksum");
	}

	FILE *fsha1 = NULL;
	int fd_sha1;
	if (((fd_sha1 = open((cat_path + "/.cvmfschecksum").c_str(), O_CREAT | O_TRUNC | O_RDWR, plain_file_mode)) < 0) ||
		!(fsha1 = fdopen(fd_sha1, "w")) ||
		(fwrite(compr_buf, 1, compr_size, fsha1) != compr_size))
	{			
		stringstream ss;
		ss << "could not store checksum at " << cat_path;
		printWarning(ss.str());
	}

	if (fsha1) fclose(fsha1);
	if (compr_buf) free(compr_buf);
}
