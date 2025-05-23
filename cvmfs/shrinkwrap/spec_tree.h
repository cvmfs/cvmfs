/**
 * This file is part of the CernVM File System.
 */
#ifndef CVMFS_SHRINKWRAP_SPEC_TREE_H_
#define CVMFS_SHRINKWRAP_SPEC_TREE_H_

#include <map>
#include <string>

enum SPEC_READ_ST {
  SPEC_READ_TREE,
  SPEC_READ_FS = -42,  // <- The answer to no questions at all
};

class SpecTreeNode {
 public:
  explicit SpecTreeNode(char modeParam) : mode(modeParam) { }
  ~SpecTreeNode() {
    for (std::map<std::string, SpecTreeNode *>::iterator it = nodes_.begin();
         it != nodes_.end();
         it++) {
      delete it->second;
    }
  }
  SpecTreeNode *GetNode(const std::string &name);
  int GetListing(std::string base_path, char ***buf, size_t *len);

  void AddNode(const std::string &name, SpecTreeNode *node);

  /**
   * 0: include this file
   * *: include deep copy from here on
   * ^: include flat copy from here on
   *
   * _: passby directory
   * -: NOT passby directory (for internal purposes, can be overwritten by _)
   *
   * !: Do not include this file
   */
  char mode;

 private:
  std::map<std::string, SpecTreeNode *> nodes_;
};

class SpecTree {
 public:
  explicit SpecTree(char mode = 0) { root_ = new SpecTreeNode(mode); }
  SpecTree(const SpecTree &tree) { root_ = tree.root_; }
  ~SpecTree() { delete root_; }
  static SpecTree *Create(const std::string &path);

  bool IsMatching(std::string path);

  /**
   * Method which returns a list over the given directory or asks the caller
   * to read from the file system.
   *
   * Memory for buf is only allocated if the return value is 0
   *
   * @param[in] dir The directory over which should be iterated
   * @param[out] buf The list of the paths to the elements in the directory
   * @param[out] len Length of the output list
   * @returns
   *    0 if listing was successfully loaded into the buffer
   *    -1 if directory should not be listed (because excluded)
   *    SPEC_READ_FS if listing should be read from source filesystem
   */
  int ListDir(const char *dir, char ***buf, size_t *len);

 private:
  void Open(const std::string &path);
  void Parse(FILE *spec_file);

  SpecTreeNode *root_;
};


#endif  // CVMFS_SHRINKWRAP_SPEC_TREE_H_
