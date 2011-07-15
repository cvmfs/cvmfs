/**
 * \file catalog_tree.cc
 * \namespace catalog_tree
 *
 * Stores the directory structure of
 * loaded file catalogs as a tree.
 *
 * Enables optimizations.
 *
 * Developed by Jakob Blomer 2011 at CERN
 * jakob.blomer@cern.ch
 */


#include "config.h"
#include "catalog_tree.h"

#include <string>
#include <sstream>
#include <vector>
#include <map>
#include <cassert>

using namespace std;

namespace catalog_tree {
   
   struct catalog_tree_t {
      catalog_tree_t *parent;
      vector<catalog_tree_t *> children;
      
      std::string path_postfix;
      catalog_meta_t *data;
   };
   
   catalog_tree_t *root = NULL;
   
   map<int, catalog_tree_t *> index_id;
   
   
   static catalog_tree_t *find_hosting(const string &path) {
      catalog_tree_t *iterator = root;
      string path_postfix = path.substr(1);
      
      do {
         catalog_tree_t *next_child = NULL;
         unsigned lprefix = 0;
         for (unsigned i = 0; i < iterator->children.size(); ++i) {
            catalog_tree_t *child = iterator->children[i];
            if ((child->path_postfix.length() > lprefix) &&
                (path_postfix.find(child->path_postfix, 0) == 0))
            {
               lprefix = child->path_postfix.length();
               next_child = child;
            }
         }
         if (next_child == NULL)
            break;
         
         iterator = next_child;
         path_postfix = path_postfix.substr(next_child->path_postfix.length());
      } while (1);
      
      return iterator;
   }
   
   
   void insert(catalog_meta_t *data) {
      assert(data);
      const string path = data->path + "/";
      assert(path[0] == '/');
      
      catalog_tree_t *node = new catalog_tree_t();
      node->data = data;
      
      if (!root) {
         assert(path == "/");
         node->parent = NULL;
         node->path_postfix = path;
         root = node;
      } else {
         catalog_tree_t *hosting = find_hosting(path);
         node->parent = hosting;
         node->path_postfix = path.substr(hosting->data->path.length() + 1);
         hosting->children.push_back(node);
      }
      
      index_id[data->catalog_id] = node;
   }
   
   
   catalog_meta_t *get_hosting(const std::string &path) {
      if (!root) return NULL;
      
      assert((path.length() > 0) && (path[0] == '/'));
      const string needle = path + "/";
      
      catalog_tree_t *hosting = find_hosting(needle);
      return hosting->data;
   }
   
   
   catalog_meta_t *get_parent(const int catalog_id) {
      map<int, catalog_tree_t *>::const_iterator i = index_id.find(catalog_id);
      assert(i != index_id.end());
      
      if (i->second->parent)
         return i->second->parent->data;
      else
         return NULL;
   }
   
   catalog_meta_t *get_catalog(const int catalog_id) {
      map<int, catalog_tree_t *>::const_iterator i = index_id.find(catalog_id);
      
      assert(i != index_id.end());
      return i->second->data;
   }
   
   
   static void visit_recursive(catalog_tree_t *node, void (visitor)(catalog_meta_t *info)) {
      visitor(node->data);
      for (unsigned i = 0; i < node->children.size(); ++i) {
         visit_recursive(node->children[i], visitor);
      }
   }
   
   void visit_children(const int catalog_id, void (visitor)(catalog_meta_t *info)) {
      map<int, catalog_tree_t *>::const_iterator node = index_id.find(catalog_id);
      assert(node != index_id.end());
      
      for (unsigned i = 0; i < node->second->children.size(); ++i) {
         visit_recursive(node->second->children[i], visitor);
      }
   }
   
   
   static string show_tree_recursive(int level, catalog_tree_t *node) {
      string result;
      for (int i = 0; i < level; ++i)
         result += "   ";
      ostringstream str_id;
      str_id << node->data->catalog_id;
      if (node->data->dirty)
         result += " ! ";
      result += node->path_postfix + " (" + str_id.str() + ")\n";
      for (unsigned i = 0; i < node->children.size(); ++i) {
         result += show_tree_recursive(level+1, node->children[i]);
      }
      return result;
   }
   
   string show_tree() {
      assert(root);
      
      return show_tree_recursive(0, root);
   }
   
}
