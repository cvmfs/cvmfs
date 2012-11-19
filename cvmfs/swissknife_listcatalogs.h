/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_LISTCATALOGS_H_
#define CVMFS_SWISSKNIFE_LISTCATALOGS_H_

#include "swissknife.h"

namespace swissknife {

class CommandListCatalogs : public Command {
 public:
  ~CommandListCatalogs() { };
  std::string GetName() { return "lscat"; };
  std::string GetDescription() {
    return "CernVM File System Catalog Listing\n"
      "This command lists the nested catalog tree that builds up a "
      "cvmfs repository structure.";
  };
  ParameterList GetParams() {
    ParameterList result;
    result.push_back(Parameter('t', "print tree structure of catalogs",
                               true, true));
    result.push_back(Parameter('h', "print hash for each catalog",
                               true, true));
    return result;
  }
  int Main(const ArgumentList &args);
};

}

#endif  // CVMFS_SWISSKNIFE_LISTCATALOGS_H_
