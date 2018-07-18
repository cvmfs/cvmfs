/**
 * This file is part of the CernVM File System.
 */
#include "spec_tree.h"

#include <errno.h>

#include <map>
#include <stack>
#include <string>
#include <vector>

#include "logging.h"
#include "util/posix.h"
#include "util/string.h"


SpecTree *SpecTree::Create(const std::string &path) {
    SpecTree *res = new SpecTree();
    res->Open(path);
    return res;
  }

struct NodeCacheEntry {
  SpecTreeNode *node;
  std::string path;
};

void SpecTree::Open(const std::string &path) {
  if (!FileExists(path)) {
    LogCvmfs(kLogCatalog, kLogStderr, "Cannot find specfile at '%s'",
      path.c_str());
      return;
  }

  FILE *spec_file = fopen(path.c_str(), "r");
  if (spec_file == NULL) {
    LogCvmfs(kLogCatalog, kLogStderr,
      "Cannot open specfile for reading at '%s' (errno: %d)",
      path.c_str(), errno);
    return;
  }
  Parse(spec_file);
  fclose(spec_file);
}

bool SpecTree::IsMatching(std::string path) {
  SpecTreeNode *cur_node = root_;
  bool is_wildcard = (cur_node->mode == '*');
  bool is_flat_cp = (cur_node->mode == '^');
  if (cur_node->mode == '!') {
    return false;
  }
  if (path.at(path.length()-1) == '/') {
    path.erase(path.length()-1);
  }
  std::vector<std::string> path_parts = SplitString(path, '/', 256);
  for (std::vector<std::string>::const_iterator part_it = path_parts.begin()+1;
      part_it != path_parts.end();
      part_it++) {
    cur_node = cur_node->GetNode(*part_it);
    if (cur_node == NULL) {
      if (is_wildcard || (is_flat_cp && (part_it+1) == path_parts.end()) ) {
        return true;
      }
      return false;
    }
    is_flat_cp = false;
    if (cur_node->mode == '!') {
      return false;
    }
    if (cur_node->mode == '*') {
      is_wildcard = true;
    }
    if (cur_node->mode == '^') {
      is_flat_cp = true;
    }
  }
  return is_wildcard || is_flat_cp
    || cur_node->mode == '_' || cur_node->mode == 0;
}

void SpecTree::Parse(FILE *spec_file) {
  // Slash terminated paths en route to the last treated spec line
  std::stack<NodeCacheEntry *> nodeCache;
  std::stack<NodeCacheEntry *> nodeBackup;
  struct NodeCacheEntry *root = new struct NodeCacheEntry;
  root->node = root_;
  root->path = "/";
  nodeCache.push(root);
  std::string line;
  char inclusionMode;
  NodeCacheEntry *entr;
  while (GetLineFile(spec_file, &line)) {
    // Go through spec file lines
    inclusionMode = 0;
    if (line.at(0) == '^' || line.at(0) == '!') {
      inclusionMode = line.at(0);
      line.erase(0, 1);
    }
    if (line.at(line.length()-1) == '*') {
      if (inclusionMode == 0) {
        inclusionMode = '*';
      }
      line.erase(line.length()-1);
    } else if (inclusionMode == '^') {
        inclusionMode = 0;
    }
    while (!nodeCache.empty()) {
      entr = nodeCache.top();
      if (IsPrefixOf(line, entr->path)) {
        line.erase(0, entr->path.length());
        break;
      } else {
        nodeCache.pop();
        delete entr;
      }
    }
    char passthroughMode = '_';
    if (inclusionMode == '!') passthroughMode = '-';
    // TODO(steuber): max_chunks?
    std::vector<std::string> path_parts = SplitString(line, '/', 256);
    SpecTreeNode *curNode = nodeCache.top()->node;
    struct NodeCacheEntry *curEl;
    char past1Mode = nodeCache.top()->node->mode;
    char past2Mode = nodeCache.top()->node->mode;
    for (std::vector<std::string>::const_iterator part_it = path_parts.begin();
      part_it != path_parts.end();
      part_it++) {
      if (*part_it == "") {
        continue;
      }
      if ((curNode = nodeCache.top()->node->GetNode(*part_it)) == NULL) {
        curNode = new SpecTreeNode(passthroughMode);
        nodeCache.top()->node->AddNode(*part_it, curNode);
      }
      curEl = new struct NodeCacheEntry;
      curEl->node = curNode;
      curEl->path = nodeCache.top()->path + *part_it + "/";
      nodeCache.push(curEl);
      past1Mode = past2Mode;
      past2Mode = passthroughMode;
    }
    curNode->mode = inclusionMode;
    if (inclusionMode != '!' && past1Mode == '-') {
      nodeCache.pop();
      while (!nodeCache.empty() && nodeCache.top()->node->mode == '-') {
        nodeCache.top()->node->mode = '_';
        nodeBackup.push(nodeCache.top());
        nodeCache.pop();
      }
      while (!nodeBackup.empty()) {
        nodeCache.push(nodeBackup.top());
        nodeCache.pop();
      }
      nodeCache.push(curEl);
    }
  }
}

SpecTreeNode *SpecTreeNode::GetNode(const std::string &name) {
  if (nodes_.count(name) == 0) {
    return NULL;
  }
  return nodes_[name];
}

void SpecTreeNode::AddNode(const std::string &name, SpecTreeNode *node) {
  nodes_[name] = node;
}

std::map<std::string, SpecTreeNode*>::const_iterator SpecTreeNode::GetNodes() {
  return nodes_.begin();
}