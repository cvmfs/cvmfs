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
#include "smalloc.h"
#include "util.h"
#include "util/exception.h"
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
    PANIC(kLogStderr, "Cannot find specfile at '%s'", path.c_str());
  }

  FILE *spec_file = fopen(path.c_str(), "r");
  if (spec_file == NULL) {
    PANIC(kLogStderr,
          "Cannot open specfile for reading at '%s' (errno: %d)",
          path.c_str(), errno);
  }
  Parse(spec_file);
  fclose(spec_file);
}

bool SpecTree::IsMatching(std::string path) {
  if (path.length() == 0) {
    path = "/";
  }
  SpecTreeNode *cur_node = root_;
  bool is_wildcard = (cur_node->mode == '*');
  bool is_flat_cp = (cur_node->mode == '^');
  if (cur_node->mode == '!') {
    return false;
  }
  if (path.length() > 0 && path.at(path.length()-1) == '/') {
    path.erase(path.length()-1);
  }
  std::vector<std::string> path_parts = SplitString(path, '/', 256);
  for (std::vector<std::string>::const_iterator part_it = path_parts.begin()+1;
      part_it != path_parts.end();
      part_it++) {
    cur_node = cur_node->GetNode(*part_it);
    if (cur_node == NULL) {
      if (is_wildcard || (is_flat_cp && (part_it+1) == path_parts.end())) {
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
  /*
   * This stack is used to speed up the parsing of (partially) sorted spec files
   * The node_backup stack is used in cases where NOT passthrough entries need
   * to be rewritten to passthrough entries by temporarily storing the
   * node_cache entries in node_backup while looking for all nodes that need to
   * be changed
   */
  std::stack<NodeCacheEntry *> node_cache;
  std::stack<NodeCacheEntry *> node_backup;
  // Root entry describing the root of the tree
  struct NodeCacheEntry *root = new struct NodeCacheEntry;
  root->node = root_;
  root->path = "/";
  node_cache.push(root);
  std::string line;
  // The mode for inclusion (see spec_tree.h) used for the current spec line
  char inclusion_mode;
  // Temporary storage for entries
  NodeCacheEntry *entr;
  SpecTreeNode *cur_node;
  // Whether there is a path on the stack disallowing inclusion
  while (GetLineFile(spec_file, &line)) {  // Go through spec file lines
    std::string raw_line = line;
    line = Trim(line);
    if (line.empty() || line[0] == '#')
      continue;
    if ((line[0] != '/') && (line[0] != '!') && (line[0] != '^'))
      PANIC(kLogStderr, "Invalid specification: %s", raw_line.c_str());

    // FIND inclusion_mode (START)
    inclusion_mode = 0;
    if (line.at(0) == '^' || line.at(0) == '!') {
      inclusion_mode = line.at(0);
      line.erase(0, 1);
    }
    if (line.empty())
      PANIC(kLogStderr, "Invalid specification: %s", raw_line.c_str());
    if (line.at(line.length()-1) == '*') {
      if (inclusion_mode == 0) {
        inclusion_mode = '*';
      }
      line.erase(line.length()-1);
    } else if (inclusion_mode == '^') {
      inclusion_mode = 0;
    }
    if (line.empty() || (line[0] != '/'))
      PANIC(kLogStderr, "Invalid specification: %s", raw_line.c_str());
    // FIND inclusion_mode (END)
    while (!node_cache.empty()) {  // Find nearest parent node in node cache
      entr = node_cache.top();
      if (HasPrefix(line, entr->path, false /*ignore_case*/)) {
        // If entr->path is prefix => parent
        line.erase(0, entr->path.length());
        break;
      } else {  // If no longer parent of current element
                // => drop cache entry for now
        node_cache.pop();
        delete entr;
      }
    }
    char passthrough_mode = '_';
    if (inclusion_mode == '!') passthrough_mode = '-';
    // TODO(steuber): max_chunks?
    // Split remaining path into its parts
    std::vector<std::string> path_parts = SplitString(line, '/', 256);
    cur_node = node_cache.top()->node;
    // Store second last mode to check whether it is NOT passthrough!
    char past_1_mode = node_cache.top()->node->mode;
    char past_2_mode = node_cache.top()->node->mode;
    for (std::vector<std::string>::const_iterator part_it = path_parts.begin();
      part_it != path_parts.end();
      part_it++) {
      if (*part_it == "") {  // If path is fully parsed/empty part => continue
        continue;
      }
      if ((cur_node = node_cache.top()->node->GetNode(*part_it)) == NULL) {
        cur_node = new SpecTreeNode(passthrough_mode);
        node_cache.top()->node->AddNode(*part_it, cur_node);
      }
      entr = new struct NodeCacheEntry;
      entr->node = cur_node;
      entr->path = node_cache.top()->path + *part_it + "/";
      node_cache.push(entr);
      past_1_mode = past_2_mode;
      past_2_mode = passthrough_mode;
    }
    cur_node->mode = inclusion_mode;
    if (inclusion_mode != '!' && past_1_mode == '-') {
      node_cache.pop();
      while (!node_cache.empty() && node_cache.top()->node->mode == '-') {
        node_cache.top()->node->mode = '_';
        node_backup.push(node_cache.top());
        node_cache.pop();
      }
      while (!node_backup.empty()) {
        node_cache.push(node_backup.top());
        node_cache.pop();
      }
      node_cache.push(entr);
    }
  }
  while (!node_cache.empty()) {
    entr = node_cache.top();
    delete entr;
    node_cache.pop();
  }
}

int SpecTree::ListDir(const char *dir,
  char ***buf,
  size_t *len) {
  std::string path = dir;
  SpecTreeNode *cur_node = root_;
  bool is_wildcard = (cur_node->mode == '*');
  bool is_flat_cp = (cur_node->mode == '^');
  if (cur_node->mode == '!') {
    return -1;
  }
  if (path.length() > 0 && path.at(path.length()-1) == '/') {
    path.erase(path.length()-1);
  }
  std::vector<std::string> path_parts = SplitString(path, '/', 256);
  for (std::vector<std::string>::const_iterator part_it = path_parts.begin()+1;
      part_it != path_parts.end();
      part_it++) {
    cur_node = cur_node->GetNode(*part_it);
    if (cur_node == NULL) {
      if (is_wildcard) {
        break;
      }
      return -1;
    }
    is_flat_cp = false;
    if (cur_node->mode == '!') {
      return -1;
    }
    if (cur_node->mode == '*') {
      is_wildcard = true;
    }
    if (cur_node->mode == '^') {
      is_flat_cp = true;
    }
  }
  if (is_wildcard || is_flat_cp) {
    return SPEC_READ_FS;
  }
  return cur_node->GetListing(path, buf, len);
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

int SpecTreeNode::GetListing(std::string base_path,
  char ***buf, size_t *len) {
  *len = 0;
  size_t buflen = 5;
  *buf = reinterpret_cast<char **>(smalloc(sizeof(char *) * buflen));
  // NULL terminate the list;
  AppendStringToList(NULL, buf, len, &buflen);
  for (std::map<std::string, SpecTreeNode*>::const_iterator
    it = nodes_.begin();
    it != nodes_.end();
    it++) {
    // Do not follow excluded paths and paths only existing for exclusion
    if (it->second->mode != '!' && it->second->mode != '-') {
      AppendStringToList(it->first.c_str(), buf, len, &buflen);
    }
  }
  return 0;  // nodes_.begin();
}
