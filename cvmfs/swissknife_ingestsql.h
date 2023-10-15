/**
 *  * This file is part of the CernVM File System.
 *   */

#ifndef CVMFS_SWISSKNIFE_INGESTSQL_H_
#define CVMFS_SWISSKNIFE_INGESTSQL_H_

#include <string>

#include "catalog_mgr_rw.h"
#include "swissknife.h"
#include "swissknife_sync.h"

namespace swissknife {
class IngestSQL : public Command {
 public:
  ~IngestSQL() {}
  virtual string GetName() const { return "ingestsql"; }
  virtual string GetDescription() const {
    return "Graft the contents of a SQLite DB to the repository";
  }
  virtual ParameterList GetParams() const {
    ParameterList r;

    r.push_back(Parameter::Mandatory('D', "input sqlite DB"));
    r.push_back(Parameter::Mandatory('N', "fully qualified repository name"));
    r.push_back(Parameter::Optional('g', "gateway URL"));
    r.push_back(Parameter::Optional('w', "stratum 0 base url"));
    r.push_back(Parameter::Optional(
        't', "temporary directory (will try TMPDIR if not set)"));
    r.push_back(Parameter::Optional('@', "proxy URL"));
    r.push_back(Parameter::Optional('l', "lease path"));
    r.push_back(Parameter::Optional('q', "number of concurrent write jobs"));
    r.push_back(Parameter::Optional('k', "public key"));
    r.push_back(Parameter::Optional('s', "gateway secret"));
    r.push_back(Parameter::Switch(
        'a', "Allow additions (default true, false if -d specified)"));
    r.push_back(Parameter::Switch('d', "Allow deletions"));
    r.push_back(Parameter::Switch('x', "Force deletion of any lease"));
    r.push_back(Parameter::Switch('c', "Enable corefile generation (requires ulimit -c >0)"));
    r.push_back(Parameter::Optional('n', "create empty database file"));
    r.push_back(Parameter::Optional('C', "config prefix, default /etc/cvmfs/gateway-client/"));

    return r;
  }
  int Main(const ArgumentList &args);

 private:
  void process_sqlite(vector<string> sqlite_db_files,
                      catalog::WritableCatalogManager &catalog_manager,
                      string lease, bool allow_additions, bool allow_deletions);
  int add_files(sqlite3 *db, catalog::WritableCatalogManager &catalog_manager);
  int add_directories(sqlite3 *db,
                      catalog::WritableCatalogManager &catalog_manager);
  int add_symlinks(sqlite3 *db,
                   catalog::WritableCatalogManager &catalog_manager);
  int do_deletions(sqlite3 *db,
                   catalog::WritableCatalogManager &catalog_manager);
};
}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_INGESTSQL_H_
