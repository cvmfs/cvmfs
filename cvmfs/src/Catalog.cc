#include "Catalog.h"

#include <iostream>

#include "AbstractCatalogManager.h"
#include "util.h"

extern "C" {
  #include "debug.h"
}

using namespace std;

namespace cvmfs {
  
const uint64_t Catalog::GROW_EPOCH        = 1199163600;
const int      Catalog::SQLITE_THREAD_MEM = 4; ///< SQLite3 heap limit per thread

Catalog::Catalog(const string &path, Catalog *parent) {
  parent_ = parent;
  path_ = path;
  
  max_row_id_ = 0;
}

bool Catalog::OpenDatabase(const string &db_file) {
  int flags = DatabaseOpenFlags();
  database_file_ = db_file;

  // open database file (depending on the flags read-only or read-write)
  pmesg(D_CATALOG, "opening database file %s", database_file_.c_str());
  if (SQLITE_OK != sqlite3_open_v2(database_file_.c_str(), &database_, flags, NULL)) {
    pmesg(D_CATALOG, "Cannot open catalog database file %s", database_file_.c_str());
    return false;
  }
  sqlite3_extended_result_codes(database_, 1);

  InitPreparedStatements();
  
  // find out the max row id of this database file
  SqlStatement max_row_id_query(database_, "SELECT MAX(rowid) FROM catalog;");
  if (not max_row_id_query.FetchRow()) {
    pmesg(D_CATALOG, "Cannot retrieve maximal row id for database file %s (SqliteErrorcode: %d)", database_file_.c_str(), max_row_id_query.GetLastError());
    return false;
  }
  max_row_id_ = max_row_id_query.RetrieveInt64(0);

  // get root prefix
  if (IsRoot()) {
    SqlStatement root_prefix_query(database_, "SELECT value FROM properties WHERE key='root_prefix';");
    if (root_prefix_query.FetchRow()) {
      root_prefix_ = string((char *)root_prefix_query.RetrieveText(0));
      pmesg(D_CATALOG, "found root prefix %s in root catalog file %s", root_prefix_.c_str(), database_file_.c_str());
    } else {
      pmesg(D_CATALOG, "Cannot retrieve root prefix for root catalog file %s", database_file_.c_str());
    }
  }
  
  // everything went well, notify parent about our existance
  if (not IsRoot()) {
    parent_->AddChild(this);
  }
  
  return true;
}

Catalog::~Catalog() {
  FinalizePreparedStatements();
  
  sqlite3_close(database_);
}

void Catalog::InitPreparedStatements() {
  listing_statement_ = new ListingLookupSqlStatement(database_);
  path_hash_lookup_statement_ = new PathHashLookupSqlStatement(database_);
  inode_lookup_statement_ = new InodeLookupSqlStatement(database_);
  find_nested_catalog_statement_ = new FindNestedCatalogSqlStatement(database_);
  list_nested_catalogs_statement_ = new ListNestedCatalogsSqlStatement(database_);
}

void Catalog::FinalizePreparedStatements() {
  delete listing_statement_;
  delete path_hash_lookup_statement_;
  delete inode_lookup_statement_;
  delete find_nested_catalog_statement_;
  delete list_nested_catalogs_statement_;
}

void Catalog::AddChild(Catalog *child) {
  children_.push_back(child);
  child->set_parent(this);
}

void Catalog::RemoveChild(const Catalog *child) {
  CatalogList::iterator i;
  CatalogList::const_iterator end;
  for (i = children_.begin(), end = children_.end(); i != end; ++i) {
    if (*i == child) {
      children_.erase(i);
      (*i)->set_parent(NULL);
      break;
    }
  }
}

CatalogList Catalog::GetChildrenRecursively() const {
  CatalogList result;
  
  CatalogList::const_iterator i, end;
  for(i = children_.begin(), end = children_.end(); i != end; ++i) {
    CatalogList grand_children = (*i)->GetChildrenRecursively();
    result.push_back(*i);
    result.insert(result.end(), grand_children.begin(), grand_children.end());
  }
  
  return result;
}

bool Catalog::Lookup(const inode_t inode, DirectoryEntry *entry, hash::t_md5 *parent_hash) const { 
  assert (IsInitialized());
  
  bool found = false;
  uint64_t row_id = GetRowIdFromInode(inode);
  
  {
    // do the actual lookup
    inode_lookup_statement_->BindRowId(row_id);
    found = inode_lookup_statement_->FetchRow();
    
    // retrieve the DirectoryEntry if needed
    if (found && NULL != entry) *entry = inode_lookup_statement_->GetDirectoryEntry(this);

    // retrieve the path_hash of the parent path if needed
    if (NULL != parent_hash) *parent_hash = inode_lookup_statement_->GetParentPathHash();

    inode_lookup_statement_->Reset();
  }
  
  return found;
}

bool Catalog::Lookup(const hash::t_md5 &path_hash, DirectoryEntry *entry) const {
  assert (IsInitialized());

  path_hash_lookup_statement_->BindPathHash(path_hash);
  bool found = path_hash_lookup_statement_->FetchRow();
	if (found && NULL != entry) {
    *entry = path_hash_lookup_statement_->GetDirectoryEntry((Catalog*)this);
    found = EnsureCoherenceOfInodes(path_hash, entry);
	}
  path_hash_lookup_statement_->Reset();
  
  return found;
}

bool Catalog::Listing(const inode_t inode, DirectoryEntryList *listing) const {
  assert (IsInitialized());

  assert (false); // TODO: currently not implemented (not needed though)
  return false;
}

bool Catalog::Listing(const hash::t_md5 &path_hash, DirectoryEntryList *listing) const {
  assert (IsInitialized());

  listing_statement_->BindPathHash(path_hash);
  while (listing_statement_->FetchRow()) {
    DirectoryEntry entry = listing_statement_->GetDirectoryEntry((Catalog*)this);
    EnsureCoherenceOfInodes(path_hash, &entry);
    listing->push_back(entry);
  }
  listing_statement_->Reset();
  
  return true;
}

bool Catalog::EnsureCoherenceOfInodes(const hash::t_md5 &path_hash, DirectoryEntry *entry) const {
  // ensure coherence of inodes after a nested catalog is loaded
  // <nested catalog mountpoint> == <nested catalog root>
  // BUT: inodes of mountpoint and root differ.
  //      must lookup the mountpoint inode and use it for the root entry as well
	if (entry->IsNestedCatalogRoot() && not this->IsRoot()) {
    DirectoryEntry nestedRootMountpoint;
    bool foundMountpoint = parent_->Lookup(path_hash, &nestedRootMountpoint);

    if (not foundMountpoint) {
      pmesg(D_CATALOG, "FATAL: mount point of nested catalog root could not be found in parent catalog");
      return false;
    } else {  
      entry->set_inode(nestedRootMountpoint.inode());
    }
	}
  
  return true;
}

inode_t Catalog::GetInodeFromRowIdAndHardlinkGroupId(uint64_t row_id, uint64_t hardlink_group_id) {
  assert (IsInitialized());

  inode_t inode = row_id + inode_chunk_.offset;
	
	// hardlinks are encoded in catalog-wide unique hard link group ids
	// these ids must be resolved to actual inode relationships at runtime
	if (hardlink_group_id > 0) {
    HardlinkGroupIdMap::const_iterator inodeItr = hardlink_groups_.find(hardlink_group_id);
  
    // create a new hardlink group map entry or use the inode already saved there
    if (inodeItr == hardlink_groups_.end()) {
      hardlink_groups_[hardlink_group_id] = inode;
    } else {
      inode = inodeItr->second;
    }
  }
	
  return inode;
}

uint64_t Catalog::GetTTL() const {
  const string sql = "SELECT value FROM properties WHERE key='TTL';";
  SqlStatement stmt(database(), sql);
  
  return (stmt.FetchRow()) ? stmt.RetrieveInt64(0) : kDefaultTTL;
}

uint64_t Catalog::GetRevision() const {
  const string sql = "SELECT value FROM properties WHERE key='revision';";
  SqlStatement stmt(database(), sql);
  
  return (stmt.FetchRow()) ? stmt.RetrieveInt64(0) : 0;
}

Catalog* Catalog::FindChildWithMountpoint(const std::string &mountpoint) const {
  CatalogList::const_iterator i,iend;
  for (i = children_.begin(), iend = children_.end(); i != iend; ++i) {
    if ((*i)->path() == mountpoint) {
      return *i;
    }
  }
  
  return NULL;
}

Catalog::NestedCatalogReferenceList Catalog::ListNestedCatalogReferences() const {
  NestedCatalogReferenceList result;
  while (list_nested_catalogs_statement_->FetchRow()) {
    NestedCatalogReference ref;
    ref.path = list_nested_catalogs_statement_->GetMountpoint();
    ref.content_hash = list_nested_catalogs_statement_->GetContentHash();
    result.push_back(ref);
  }
  
  list_nested_catalogs_statement_->Reset();
  
  return result;
}

} // namespace cvmfs
