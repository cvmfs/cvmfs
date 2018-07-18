/**
 * This file is part of the CernVM File System.
 */
#ifndef CVMFS_EXPORT_PLUGIN_SPEC_TREE_H_
#define CVMFS_EXPORT_PLUGIN_SPEC_TREE_H_

#include <map>
#include <string>

class SpecTreeNode {
 public:
  explicit SpecTreeNode(char modeParam)
    : mode(modeParam) {}
  // TODO(steuber): Destructor -> delete tree nodes
  // TODO(steuber): list_dir
  SpecTreeNode *GetNode(const std::string &name);
  std::map<std::string, SpecTreeNode*>::const_iterator GetNodes();

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
  std::map<std::string, SpecTreeNode*> nodes_;
};

class SpecTree {
 public:
  SpecTree() {
    root_ = new SpecTreeNode(0);
  }
  SpecTree(const SpecTree &tree) {
    root_ = tree.root_;
  }
  ~SpecTree() {
    delete root_;
  }
  static SpecTree *Create(const std::string &path);

  bool IsMatching(std::string path);

  char **list_dir() {
    return NULL;
  }
 private:
  void Open(const std::string &path);
  void Parse(FILE *spec_file);

  SpecTreeNode *root_;
};



#endif  // CVMFS_EXPORT_PLUGIN_SPEC_TREE_H_
