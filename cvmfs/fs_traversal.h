/**
 * This file is part of the CernVM File System.
 *
 * It provides a traversal framework to abstract the traversal of directories.
 */

#ifndef CVMFS_FS_TRAVERSAL_H_
#define CVMFS_FS_TRAVERSAL_H_

#include <cassert>

#include <string>
#include <set>

#include "platform.h"

namespace publish {

/**
 * @brief A simple recursion engine to abstract the recursion of directories.
 * It provides several callback hooks to instrument and control the recursion.
 * Hooks will be called on the provided delegate object of type T
 *
 * Callbacks are called for every directory entry found by the recursion engine.
 * The recursion can be influenced by return values of these callbacks.
 */
template <class T>
class FileSystemTraversal {
 public:
	typedef void (T::*VoidCallback)(const std::string &relative_path,
                                  const std::string &dir_name);
	typedef bool (T::*BoolCallback)(const std::string &relative_path,
                                  const std::string &dir_name);

	/** callback if a directory is entered by the recursion */
  VoidCallback enteringDirectory;

	/** callback if a directory is left by the recursion */
	VoidCallback leavingDirectory;

	/**
	 *  callback if a directory was found
	 *  depending on the response of the callback, the recursion will continue in the found directory
	 *  if this callback is not specified, it will recurse by default!
	 */
	BoolCallback foundDirectory;

	/**
	 *  callback for a found directory after it was already recursed
	 *  e.g. for deletion of directories (first delete content, then the directory itself)
	 */
	VoidCallback foundDirectoryAfterRecursion;

	/** callback if a file was found */
	VoidCallback foundRegularFile;

  /** callback if a symlink was found */
  VoidCallback foundSymlink;

	/**
	 *  create a new recursion engine
	 *  @param delegate the object which will receive the callbacks
	 *  @param relativeToDirectory the DirEntries will be created relative to this directory
	 *  @param ignoredFiles a list of files which the delegate DOES NOT care about (no callback calls or recursion for them)
	 *  @param recurse should the recursion engine recurse at all? (if not, it basically just traverses the given directory)
	 */
	FileSystemTraversal(T *delegate,
	                const std::string &relativeToDirectory = "",
	                const bool recurse = true,
	                const std::set<std::string> &ignoredFiles = std::set<std::string>())
  {
    delegate_ = delegate;
    relative_to_directory_ = relativeToDirectory;
    recurse_ = recurse;
    ignored_files_ = ignoredFiles;
    enteringDirectory = NULL;
    leavingDirectory = NULL;
    foundDirectory = NULL;
    foundDirectoryAfterRecursion = NULL;
    foundRegularFile = NULL;
    foundSymlink = NULL;
    Init();
  }

	/**
	 *  start the recursion
	 *  @param dirPath the directory to start the recursion at
	 */
	void Recurse(const std::string &dir_path) const {
  	assert(enteringDirectory != NULL ||
  	       leavingDirectory != NULL ||
  	       foundRegularFile != NULL ||
  	       foundDirectory != NULL ||
  	       foundSymlink != NULL);

    std::string parent_path;
    std::string dir_name;
    CutPathIntoParentAndFileName(dir_path, parent_path, dir_name);

    assert(relative_to_directory_.length() == 0 ||
           dir_path.substr(0, relative_to_directory_.length()) == relative_to_directory_);

  	DoRecursion(parent_path, dir_name);
  }

 private:
	// The delegate all hooks are called on
	T *delegate_;

	/** dirPath in callbacks will be relative to this directory */
	std::string relative_to_directory_;
	bool recurse_;

	/** if one of these files are found somewhere they are completely ignored */
	std::set<std::string> ignored_files_;



  void Init() {
    // we definitely don't care about these "virtual" directories
    ignored_files_.insert(".");
    ignored_files_.insert("..");
  }

	void DoRecursion(const std::string &parent_path, const std::string &dir_name) const
	{
  DIR *dip;
  platform_dirent64 *dit;
  const std::string path = parent_path + "/" + dir_name;

  // get into directory and notify the user
  if ((dip = opendir(path.c_str())) == NULL) { return; }
  Notify(enteringDirectory, parent_path, dir_name);

  // go through the open directory notifying the user at specific positions
  while ((dit = platform_readdir(dip)) != NULL) {
    // check if filename is included in the ignored files list
    if (ignored_files_.find(dit->d_name) != ignored_files_.end()) {
      continue;
    }

    // notify user about found directory entry
    switch (dit->d_type) {
      case DT_DIR:
        if (Notify(foundDirectory, path, dit->d_name) && recurse_) {
          DoRecursion(path, dit->d_name);
        }
        Notify(foundDirectoryAfterRecursion, path, dit->d_name);
        break;
      case DT_REG:
        Notify(foundRegularFile, path, dit->d_name);
        break;
      case DT_LNK:
        Notify(foundSymlink, path, dit->d_name);
        break;
    }
  }

  // close directory and notify user
  if (closedir(dip) != 0) { return; }
  Notify(leavingDirectory, parent_path, dir_name);
}

  inline bool Notify(const BoolCallback callback,
        	           const std::string &parent_path,
        	           const std::string &entry_name) const {
    return (NULL == callback) ? true :
    (delegate_->*callback)(GetRelativePath(parent_path),
                           entry_name);
  }

  inline void Notify(const VoidCallback callback,
        	           const std::string &parent_path,
        	           const std::string &entry_name) const {
    if (NULL != callback) {
      (delegate_->*callback)(GetRelativePath(parent_path),
                             entry_name);
    }
  }

	std::string GetRelativePath(const std::string &absolute_path) const  {
    const unsigned int rel_dir_len = relative_to_directory_.length();
    if (rel_dir_len >= absolute_path.length()) { return ""; }
	  else if (rel_dir_len > 1) { return absolute_path.substr(rel_dir_len + 1); }
	  else if (rel_dir_len == 0){ return absolute_path; }
	  else if (relative_to_directory_ == "/") { return absolute_path.substr(1); }
    else return "";
  }

  void CutPathIntoParentAndFileName(const std::string &path,
                                    std::string &parent_path,
                                    std::string &file_name) const {
    const std::string::size_type idx = path.find_last_of('/');
    if (idx != std::string::npos) {
      parent_path = path.substr(0, idx);
      file_name   = path.substr(idx+1);
    } else {
      parent_path = "";
      file_name   = path;
    }
  }
};  // FileSystemTraversal

}  // namespace publish

#endif  // CVMFS_FS_TRAVERSAL_H_
