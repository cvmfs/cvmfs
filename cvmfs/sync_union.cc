/**
 * This file is part of the CernVM File System
 */

#define __STDC_FORMAT_MACROS

#include "sync_union.h"

#include "sync_mediator.h"
#include "util/shared_ptr.h"
#include "util/fs_traversal.h"

namespace publish {

SyncUnion::SyncUnion(AbstractSyncMediator *mediator,
                     const std::string &rdonly_path,
                     const std::string &union_path,
                     const std::string &scratch_path)
    : rdonly_path_(rdonly_path),
      scratch_path_(scratch_path),
      union_path_(union_path),
      mediator_(mediator),
      initialized_(false) {}

bool SyncUnion::Initialize() {
  mediator_->RegisterUnionEngine(this);
  initialized_ = true;
  return true;
}

SharedPtr<SyncItem> SyncUnion::CreateSyncItem(
    const std::string &relative_parent_path, const std::string &filename,
    const SyncItemType entry_type) const {
  SharedPtr<SyncItem> entry = SharedPtr<SyncItem>(
      new SyncItemNative(relative_parent_path, filename, this, entry_type));
  LogCvmfs(kLogUnionFs, kLogStdout, "Relative parent path: %s, filename: %s", relative_parent_path.c_str(), filename.c_str());
  PreprocessSyncItem(entry);
  if (entry_type == kItemFile) {
    entry->SetExternalData(mediator_->IsExternalData());
    entry->SetDirectIo(mediator_->IsDirectIo());
    if (!(entry->IsValidGraft() && entry->HasCompressionAlgorithm())) {
      entry->SetCompressionAlgorithm(mediator_->GetCompressionAlgorithm());
    }
  }
  return entry;
}

void SyncUnion::PreprocessSyncItem(SharedPtr<SyncItem> entry) const { 
  // Incorrectly to say already processed here, but leave for simplicity
  if (IsWhiteoutEntry(entry)) 
  {
    if (IsAlreadyProcessed(entry))
    {
      LogCvmfs(kLogUnionFs, kLogStdout, "[ALREADY PROCESSED] PreprocessSyncItem." 
                                        "Detected a whiteout which a renamed dir: %s", 
                                         entry->GetRelativePath().c_str());
      entry->MarkAsAlreadyProcessed();
      return;
    }
    LogCvmfs(kLogUnionFs, kLogStdout, "PreprocessSyncItem. Mark as a whiteout: %s", entry->GetRelativePath().c_str());
    entry->MarkAsWhiteout(UnwindWhiteoutFilename(entry));
  }
  // else if (entry->IsRegularFile()) 
  // { 
  //   if (IsUpdatedFile(entry)) 
  //   {
  //     LogCvmfs(kLogCvmfs, kLogStdout, "Updated file detected: %s", entry->GetRelativePath().c_str());
  //     entry->MarkAsUpdatedFile();
  //   }
  // }
  else 
  {
    // if (IsMetadataOnlyEntry(entry)) {
    //   LogCvmfs(kLogUnionFs, kLogStdout, "PreprocessSyncItem. Metadata-only filesystem entry: %s", entry->GetRelativePath().c_str());
    //   entry->MarkAsMetadataOnlyEntry();
    // }
    
    if (IsOpaqueDirectory(entry))
    {
      LogCvmfs(kLogCvmfs, kLogStdout, "Opaque directory detected: %s", entry->GetRelativePath().c_str());
      entry->MarkAsOpaqueDirectory();
    }
    else if (IsMarkedDirectory(entry)) 
    {
      entry->MarkAsMarkedDirectory();
    }
    else if (IsRenamedDirectory(entry))
    {
      entry->MarkAsRenamedDirectory();
    }
  }
}

bool SyncUnion::IgnoreFilePredicate(const std::string &parent_dir,
                                    const std::string &filename) {
  return false;
}

bool SyncUnion::ProcessDirectory(const string &parent_dir,
                                 const string &dir_name) {
  LogCvmfs(kLogUnionFs, kLogDebug, "SyncUnion::ProcessDirectory(%s, %s)",
           parent_dir.c_str(), dir_name.c_str());
  SharedPtr<SyncItem> entry = CreateSyncItem(parent_dir, dir_name, kItemDir);
  return ProcessDirectory(entry);
}

bool SyncUnion::ProcessDirectory(SharedPtr<SyncItem> entry) {
  if (entry->IsMarkedDirectory()) {
    return true;
  }
  if (entry->IsNew() && !entry->IsRenamedDirectory()) {
    // Currently the approach fails when the file inside the renamed directory was updated
    // We have a situation when /renamed_dir and /renamed_dir/touched_file.txt end up in a scratch area
    // On traversal we rename directory and corresponding entry in catalog for touched_file.txt
    // Upon encountering touched_file.txt it is considered as a new one since we don't mark it
    // And there is no corresponding file in rdonly directory
    // We try adding new entry but there is an entry with exactly the same hash after renaming stage
    // TODO: Implement a way to mark such entries and avoid adding them, update content hash only 
    mediator_->Add(entry);
    // Recursion stops here. All content of new directory
    // is added later by the SyncMediator
    return false;
  } 
  // directory already existed...
  if (entry->IsOpaqueDirectory()) {  // was directory completely overwritten?
    mediator_->Replace(entry);
    return false;  // <-- replace does not need any further recursion
  }
  
  // directory was just changed internally... only touch needed
  mediator_->Touch(entry);
  return true;
  
}

// We don't have the directory that we are processing in the fs, we
// cannot recurse inside the directory.
// If the directory already exists, we simply remove it and we put it back the
// new one (some attributes may change)
// If it does not exists we simply add it.
bool SyncUnion::ProcessUnmaterializedDirectory(SharedPtr<SyncItem> entry) {
  if (entry->IsNew()) {
    mediator_->AddUnmaterializedDirectory(entry);
  }
  return true;
}

void SyncUnion::ProcessRegularFile(const string &parent_dir,
                                   const string &filename) {
  LogCvmfs(kLogUnionFs, kLogDebug, "SyncUnion::ProcessRegularFile(%s, %s)",
           parent_dir.c_str(), filename.c_str());
  
  SharedPtr<SyncItem> entry = CreateSyncItem(parent_dir, filename, kItemFile);
  ProcessFile(entry);
}

void SyncUnion::ProcessSymlink(const string &parent_dir,
                               const string &link_name) {
  LogCvmfs(kLogUnionFs, kLogDebug, "SyncUnion::ProcessSymlink(%s, %s)",
           parent_dir.c_str(), link_name.c_str());
  SharedPtr<SyncItem> entry = 
      CreateSyncItem(parent_dir, link_name, kItemSymlink);
  ProcessFile(entry);
}

void SyncUnion::ProcessFile(SharedPtr<SyncItem> entry) {
  LogCvmfs(kLogUnionFs, kLogStdout, "SyncUnion::ProcessFile(%s)",
           entry->filename().c_str());
  if (entry->IsAlreadyProcessed()) 
  {
    LogCvmfs(kLogUnionFs, kLogStdout, "file [%s] is already processed",
               entry->filename().c_str());
    return;
  }
  if (entry->IsWhiteout()) 
  {
    LogCvmfs(kLogUnionFs, kLogStdout, "file [%s] is a whiteout",
               entry->filename().c_str());
    // Approach works for the files inside renamed directories
    // However also has a pitfall for the cases when the renamed directory contains a subdir with a nested file
    // And the file was firstly modified, then deleted
    // For instance I have /repo/dir/subdir/test.txt 
    // I perform mv /repo/dir /repo/dir_renamed
    // The scratch area then looks like: /scratch_area/dir (a whiteout)
    //                                   /scratch_area/dir_renamed (empty dir)
    // After doing echo "Test" >> /repo/dir_renamed/subdir/test.txt
    // The scratch area looks like: /scratch_area/dir (a whiteout)
    //                              /scratch_area/dir_renamed/subdir/test.txt
    // After doing rm /repo/dir_renamed/subdir/test.txt
    // The scratch area: /scratch_area/dir (a whiteout)
    //                   /scratch_area/dir_renamed/subdir/test.txt (a whiteout)
    // The parent of test.txt will not contain information about subdir previous path 
    // which poses the issue with mediator->Remove() invocation: it is not possible for it to determine 
    // the corresponding entry in /rdonly (no /rdonly/dir_renamed/subdir/test.txt entry)
    // We should take a look at /rdonly/dir/subdir/test.txt entry
    mediator_->Remove(entry);
    return;
  } 
  if (entry->IsNew()) 
  {
      LogCvmfs(kLogUnionFs, kLogVerboseMsg, "processing file [%s] as new (add)",
               entry->filename().c_str());
      // Breaks primary key constraints for the files that
      // were modified during the transaction and are stored inside renamed directories
      // (for the cases when a directory renamed and nested file modification happened during the same transaction)
      // for instance overlayfs directory: /repo/dir/subdir/file.txt
      // a user opens transaction and performs the following operations
      // mv /repo/dir/subdir /repo/dir/subdir_1
      // in the scratch area we have /scratch_area/dir/subdir (as a whiteout) and /scratch_area/dir/subdir_1 (as an empty directory)
      // doing consequent command 
      // echo "Hi" >> /repo/dir/subdir_1/file.txt  leads to file.txt copy up and scratch area looks like:
      // /scratch_area/dir/subdir_1/file.txt
      // The first traversal will update the catalog entry that corresponds to /repo/dir/subdir/file.txt (previous (pre-mv) path),
      // changing its MD5 hash from the one that corresponds to the previous path to the one that corresponds to /repo/dir/subdir_1/file.txt
      // But on the second traversal SyncItem for /repo/dir/subdir_1/file.txt will be marked as a new one 
      // (since rdonly directory doesn't have /rdonly/subdir_1/file.txt entry)
      // Hence subsequent insert statement will calculate the MD5 hash which already exists in a catalog db and fail with 
      // a primary key constraint violation error 
      // TODO: Perform catalog entries renaming only for empty directories (empty and presented inside the scratch area)
      // Keeping a set of modified files that are presented inside renamed dirs is also OK 
      // Modify a mediator so it recalculates only content hash 
      mediator_->Add(entry);
      return;
  } 
    
  LogCvmfs(kLogUnionFs, kLogVerboseMsg,
               "processing file [%s] as existing (touch)",
               entry->filename().c_str());
  mediator_->Touch(entry);
}

void SyncUnion::EnterDirectory(const string &parent_dir,
                               const string &dir_name) {
  SharedPtr<SyncItem> entry = CreateSyncItem(parent_dir, dir_name, kItemDir);
  mediator_->EnterDirectory(entry);
}

void SyncUnion::LeaveDirectory(const string &parent_dir,
                               const string &dir_name) {
  SharedPtr<SyncItem> entry = CreateSyncItem(parent_dir, dir_name, kItemDir);
  mediator_->LeaveDirectory(entry);
}

void SyncUnion::ProcessCharacterDevice(const std::string &parent_dir,
                                       const std::string &filename) {
  LogCvmfs(kLogUnionFs, kLogDebug,
           "SyncUnionOverlayfs::ProcessCharacterDevice(%s, %s)",
           parent_dir.c_str(), filename.c_str());
  SharedPtr<SyncItem> entry =
      CreateSyncItem(parent_dir, filename, kItemCharacterDevice);
  ProcessFile(entry);
}

void SyncUnion::ProcessBlockDevice(const std::string &parent_dir,
                                   const std::string &filename) {
  LogCvmfs(kLogUnionFs, kLogDebug,
           "SyncUnionOverlayfs::ProcessBlockDevice(%s, %s)", parent_dir.c_str(),
           filename.c_str());
  SharedPtr<SyncItem> entry =
      CreateSyncItem(parent_dir, filename, kItemBlockDevice);
  ProcessFile(entry);
}

void SyncUnion::ProcessFifo(const std::string &parent_dir,
                            const std::string &filename) {
  LogCvmfs(kLogUnionFs, kLogDebug, "SyncUnionOverlayfs::ProcessFifo(%s, %s)",
           parent_dir.c_str(), filename.c_str());
  SharedPtr<SyncItem> entry = CreateSyncItem(parent_dir, filename, kItemFifo);
  ProcessFile(entry);
}

void SyncUnion::ProcessSocket(const std::string &parent_dir,
                              const std::string &filename) {
  LogCvmfs(kLogUnionFs, kLogDebug, "SyncUnionOverlayfs::ProcessSocket(%s, %s)",
           parent_dir.c_str(), filename.c_str());
  SharedPtr<SyncItem> entry = CreateSyncItem(parent_dir, filename, kItemSocket);
  ProcessFile(entry);
}

bool SyncUnion::ProcessRenamedDirectory(const std::string &parent_dir, const std::string &filename) {
  LogCvmfs(kLogUnionFs, kLogDebug, "SyncUnionOverlayfs::ProcessRenamedDirectory(%s, %s)",
           parent_dir.c_str(), filename.c_str());
  SharedPtr<SyncItem> entry = CreateSyncItem(parent_dir, filename, kItemDir);
  if (!entry->IsRenamedDirectory()) {
    return true;
  }
  return ProcessRenamedDirectory(entry);
}

bool SyncUnion::ProcessRenamedDirectory(SharedPtr<SyncItem> entry) {
  LogCvmfs(kLogUnionFs, kLogStdout, "[PROCESS RENAMED DIR] Detected renamed directory: %s", entry->GetScratchPath().c_str());
  // mediator_->RenameDirectory(entry);
  
  const std::string previous_path = kPathSeparator + entry->GetPreviousPath();
  // const std::string current_path = kPathSeparator + entry->GetRelativePath();
  UniquePtr<XattrList> renamed_entry_xattrs(XattrList::CreateFromFile(entry->GetScratchPath()));
  if (!renamed_entry_xattrs.IsValid()) 
  {
    LogCvmfs(kLogUnionFs, kLogStdout, "[PROCESS RENAMED DIR] Unable to create xattr list for renamed entry: [%s]", entry->GetScratchPath().c_str());
    return false;
  }
  if (!platform_setxattr(entry->GetScratchPath(), "user.cvmfs.previous_path", previous_path))
  {
    LogCvmfs(kLogUnionFs, kLogStdout, "[FIRST TRAVERSAL] Marking an entry [%s] with old path [%s] failed. Error: %s", entry->GetScratchPath().c_str(), 
                                                                                                    previous_path.c_str(), 
                                                                                                    strerror(errno));
    return false;                                                              
  }
  // if (!renamed_entry_xattrs->Set("user.cvmfs.previous_path", previous_path)) 
  // {
  //   LogCvmfs(kLogUnionFs, kLogStdout, "[PROCESS RENAMED DIR] Unable to previous path xattr for renamed entry: [%s]", entry->GetScratchPath().c_str());
  //   return false;
  // }
  LogCvmfs(kLogUnionFs, kLogStdout, "[PROCESS RENAMED DIR] Marked an entry [%s] with old path [%s]", entry->GetScratchPath().c_str(), previous_path.c_str());
  
  renamed_directories_[entry->GetRelativePath()] = entry->GetPreviousPath();
  previous_directories_paths_.insert(entry->GetPreviousPath());
  // Mark entries nested in the renamed directories in the scratch area
  // Mark only directories (since we are not able to mark whiteouts)
  // Do this to avoid situation with error on removing a whiteout inside a nested directory   
  FileSystemTraversal<SyncUnion> renamed_subtree_traversal(this, scratch_path(), true);
  renamed_subtree_traversal.fn_new_dir_prefix = &SyncUnion::ProcessSubdirectoryInRenamedDir;
  renamed_subtree_traversal.Recurse(entry->GetScratchPath());
  return true;
  // TRY ANOTHER APPROACH
  // Store a map with bidirectional link between current scratch path and rdonly path 
  // FileSystemTraversal<SyncUnion> renamed_subtree_traversal(this, rdonly_path(), true);
  // renamed_subtree_traversal.fn_new_dir_prefix = &SyncUnion::ProcessSubdirectoryInReadonlyDir;
  // renamed_subtree_traversal.Recurse(rdonly_path() + previous_path); 
  // return true;
}


bool SyncUnion::ProcessSubdirectoryInReadonlyDir(const string& parent_dir, 
                                                 const string& filename) {
  // const string rdonly_entry_path = parent_dir + kPathSeparator + filename; 
  // if (rdonly_to_catalog_paths.find(rdonly_entry_path) != rdonly_to_catalog_paths.end()) {

  // }
  return false;
}

bool SyncUnion::ProcessSubdirectoryInRenamedDir(const string& parent_dir,
                                                const string& filename) {
  // SharedPtr<SyncItem> entry = CreateSyncItem(parent_dir, filename, kItemDir);
  const string current_entry_path = parent_dir + kPathSeparator + filename;
  UniquePtr<XattrList> current_entry_xattrs(XattrList::CreateFromFile(current_entry_path));
  if (!current_entry_xattrs.IsValid()) 
  {
    LogCvmfs(kLogUnionFs, kLogStdout, "[PROCESS RENAMED DIR SUBDIRECTORY] Unable to create xattr list for entry: [%s]", current_entry_path.c_str());
  }
  if (current_entry_xattrs->Has("trusted.overlay.redirect"))
  {
    LogCvmfs(kLogUnionFs, kLogStdout, "[PROCESS RENAMED DIR SUBDIRECTORY] skip renamed subdirectory: [%s]", current_entry_path.c_str())
    return false;
  }
  string parent_directory_previous_path = "";
  if (!current_entry_xattrs->Get("user.cvmfs.previous_path", &parent_directory_previous_path))
  {
    LogCvmfs(kLogUnionFs, kLogStdout, "[PROCESS RENAMED DIR SUBDIRECTORY] unable to get previous path of the parent entry: [%s]",
                                                                                                parent_directory_previous_path.c_str());
  }  
  // if (entry->IsRenamedDirectory()) {
  //   LogCvmfs(kLogUnionFs, kLogStdout, "Subdirectory: [%s] is renamed. Skip marking for it", entry->GetRelativePath().c_str());
  //   return false;
  // }
  // Since we don't have access to the previous (pre-rename) path of the parent entry
  // We recreate parent directory sync item (it might be availabe if we would have never C++ standards with lambdas context)
  // SharedPtr<SyncItem> parent_directory_sync_item = CreateSyncItem(GetParentPath(parent_dir), 
                                                                  // GetFileName(parent_dir), 
                                                                  // kItemDir);
  // const string previous_path = kPathSeparator + parent_directory_sync_item->GetPreviousPath() + kPathSeparator + filename;
  const string relative_previous_entry_path = parent_directory_previous_path + kPathSeparator + filename; 
  // LogCvmfs(kLogUnionFs, kLogStdout, "Marking an entry [%s] with previous path [%s]", current_entry_path.c_str(), previous_path.c_str());
  LogCvmfs(kLogUnionFs, kLogStdout, "[PROCESS RENAMED DIR SUBDIRECTORY] Marking an entry [%s] with previous path [%s]", 
                                     current_entry_path.c_str(), relative_previous_entry_path.c_str());                                                                  
  // if (!current_entry_xattrs->Set("user.cvmfs.previous_path", relative_previous_entry_path))
  // {
  //   LogCvmfs(kLogUnionFs, kLogStdout, "[PROCESS RENAMED DIR SUBDIRECTORY] Marking an entry with old path failed: %s. Error: %s", current_entry_path.c_str(), strerror(errno));   
  // }
  if (!platform_setxattr(current_entry_path, "user.cvmfs.previous_path", relative_previous_entry_path))
  {
    LogCvmfs(kLogUnionFs, kLogStdout, "[PROCESS RENAMED DIR SUBDIRECTORY] Marking an entry with old path failed: %s. Error: %s", current_entry_path.c_str(), strerror(errno));
  }
  // rdonly_to_catalog_paths[rdonly_path() + previous_path] = entry->GetRelativePath();
  return true;
}

bool SyncUnion::IsAlreadyProcessed(SharedPtr<SyncItem> entry) const
{
  return previous_directories_paths_.find(entry->GetRelativePath()) != previous_directories_paths_.end();
}

}  // namespace publish
