#ifndef CATALOG_TREE_H
#define CATALOG_TREE_H 1

#include "config.h"

#include <string>
#include <ctime>
#include "hash.h"

namespace catalog_tree {

   struct catalog_meta_t {
      std::string path;
      int catalog_id;
      hash::t_sha1 snapshot;
      time_t expires;
      time_t last_checked;
      time_t last_changed;
      bool dirty;
      
      catalog_meta_t(const std::string &p, const int id, const hash::t_sha1 &s) :
         path(p), catalog_id(id), snapshot(s), expires(0), last_checked(0), last_changed(0), dirty(false) {}
   };
   
   void insert(catalog_meta_t *data);
   catalog_meta_t *get_hosting(const std::string &path);
   catalog_meta_t *get_parent(const int catalog_id);
   catalog_meta_t *get_catalog(const int catalog_id);
   
   void visit_children(const int catalog_id, void (visitor)(catalog_meta_t *info));
   
   std::string show_tree();
   
}

#endif
