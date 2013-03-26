/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_MIGRATE_H_
#define CVMFS_SWISSKNIFE_MIGRATE_H_

#include "swissknife.h"

#include "hash.h"

namespace catalog {
  class Catalog;
}

namespace swissknife {

class CommandMigrate : public Command {
 public:
  CommandMigrate();
  ~CommandMigrate() { };
  std::string GetName() { return "migrate"; };
  std::string GetDescription() {
    return "CernVM-FS catalog repository migration \n"
      "This command migrates the whole catalog structure of a given repository";
  };
  ParameterList GetParams();

  int Main(const ArgumentList &args);

 protected:
  void CatalogCallback(const catalog::Catalog* catalog,
                       const hash::Any&        catalog_hash,
                       const unsigned          tree_level);

 private:
  bool print_tree_;
  bool print_hash_;
};

}

#endif  // CVMFS_SWISSKNIFE_MIGRATE_H_
