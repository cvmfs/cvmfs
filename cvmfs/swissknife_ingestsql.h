/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_INGESTSQL_H_
#define CVMFS_SWISSKNIFE_INGESTSQL_H_

#include <string>
#include <utility>
#include <map>
#include <vector>

#include "catalog_mgr_rw.h"
#include "swissknife.h"

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
    r.push_back(Parameter::Optional('k', "public key"));
    r.push_back(Parameter::Optional('l', "lease path"));
    r.push_back(Parameter::Optional('p', "prefix to add to lease and all graft files"));
    r.push_back(Parameter::Optional('q', "number of concurrent write jobs"));
    r.push_back(Parameter::Optional('s', "gateway secret"));
    r.push_back(Parameter::Optional('3', "s3 config"));
    r.push_back(Parameter::Switch(
        'a', "Allow additions (default true, false if -d specified)"));
    r.push_back(Parameter::Switch('d', "Allow deletions"));
    r.push_back(Parameter::Switch('x', "Force deletion of any lease"));
    r.push_back(Parameter::Switch('c', "Enable corefile generation (requires ulimit -c >0)"));
    r.push_back(Parameter::Optional('n', "create empty database file"));
    r.push_back(Parameter::Optional('C', "config prefix, default /etc/cvmfs/gateway-client/"));
    r.push_back(Parameter::Optional('B', "mount point to block on pending visibility of update"));
    r.push_back(Parameter::Optional('T', "reset TTL in sec"));
    r.push_back(Parameter::Switch('z', "Create missing nested catalogs"));
    r.push_back(Parameter::Optional('r', "lease retry interval"));
    r.push_back(Parameter::Switch('Z', "check and set completed_graft property"));
    r.push_back(Parameter::Optional('P', "priority for graft (integer)"));
    r.push_back(Parameter::Switch('v', "Enable verbose logging"));

    return r;
  }
  int Main(const ArgumentList &args);

  struct Directory {
    std::string name;
    time_t mtime;
    mode_t mode;
    uid_t owner;
    gid_t grp;
    int nested;
    XattrList xattr;
    Directory() {}
    Directory(const std::string &name, time_t mtime, mode_t mode, uid_t owner, gid_t grp, int nested) :
      name(name), mtime(mtime), mode(mode), owner(owner), grp(grp), nested(nested)
    {}
  };
  
  struct Symlink {
    std::string name;
    std::string target;
    time_t mtime;
    uid_t owner;
    gid_t grp;
    int skip_if_file_or_dir;
    Symlink(std::string &&name, std::string &&target, time_t mtime, uid_t owner, gid_t grp, int skip_if_file_or_dir) :
      name(std::move(name)), target(std::move(target)), mtime(mtime), owner(owner), grp(grp), skip_if_file_or_dir(skip_if_file_or_dir)
    {}
  };
  
  struct File {
    std::string name;
    time_t mtime;
    size_t size;
    uid_t owner;
    gid_t grp;
    mode_t mode;
    int internal;
    FileChunkList chunks;
    int compressed;
  
    File(std::string &&name, time_t mtime, size_t size, uid_t owner, gid_t grp, mode_t mode, int internal, int compressed) :
      name(std::move(name)), mtime(mtime), size(size), owner(owner), grp(grp), mode(mode), internal(internal), compressed(compressed)
    {}
  };

 typedef std::map<std::string, Directory> DirMap;
 typedef std::map<std::string, std::vector<File>> FileMap;
 typedef std::map<std::string, std::vector<Symlink>> SymlinkMap;

 private:
  void process_sqlite(const std::vector<sqlite3*>& dbs,
                      catalog::WritableCatalogManager &catalog_manager,
                      bool allow_additions, bool allow_deletions, const std::string &lease_path, const std::string &additional_prefix);
  int add_files(catalog::WritableCatalogManager &catalog_manager, const std::vector<File> &files);
  int add_symlinks(
                   catalog::WritableCatalogManager &catalog_manager, const std::vector<Symlink> &symlinks);
  int do_additions(const DirMap &all_dirs, const FileMap &all_files, const SymlinkMap &all_symlinks, const std::string &lease_path,
                      catalog::WritableCatalogManager &catalog_manager);
  int do_deletions(sqlite3 *db,
                   catalog::WritableCatalogManager &catalog_manager, const std::string &lease_path, const std::string &additional_prefix);
  void load_dirs(sqlite3 *db, const std::string &lease_path, const std::string &additional_prefix, std::map<std::string, Directory> &all_dirs);
  void load_files(sqlite3 *db, const std::string &lease_path, const std::string &additional_prefix, std::map<std::string, std::vector<File>> &all_files);
  void load_symlinks(sqlite3 *db, const std::string &lease_path, const std::string &additional_prefix, std::map<std::string, std::vector<Symlink>> &all_symlinks);
};
}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_INGEST_H_
