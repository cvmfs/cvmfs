/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_INGESTSQL_GEN_H_
#define CVMFS_SWISSKNIFE_INGESTSQL_GEN_H_

#include <stdint.h>
#include <sys/types.h>

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "swissknife.h"

namespace swissknife {

/**
 * Scans a local file or directory and emits the matching ingestsql input,
 * either as SQL / a listing on stdout or as a ready-to-ingest SQLite file.
 * Files are described as external data (24 MiB chunks, raw SHA-1).
 */
class IngestSQLGen : public Command {
 public:
  ~IngestSQLGen() { }
  virtual std::string GetName() const { return "ingestsql_gen"; }
  virtual std::string GetDescription() const {
    return "Generate ingestsql input from a local file or directory";
  }
  virtual ParameterList GetParams() const {
    ParameterList r;
    r.push_back(Parameter::Optional('i', "local file or directory"));
    r.push_back(Parameter::Optional(
        'p', "repository path of the input (default: /<input basename>)"));
    r.push_back(Parameter::Optional(
        'o', "write a SQLite database (must not exist) instead of stdout"));
    r.push_back(
        Parameter::Optional('f', "stdout format: sql (default), table"));
    r.push_back(Parameter::Optional(
        'N', "comma-separated globs of repository paths to make nested "
             "catalogs (directories with .cvmfscatalog always are)"));
    r.push_back(Parameter::Optional('u', "override ownership, <uid>:<gid>"));
    r.push_back(Parameter::Optional(
        'j', "number of hashing threads (default: number of CPUs)"));
    r.push_back(Parameter::Switch('S', "print the annotated schema and exit"));
    r.push_back(Parameter::Switch(
        'H', "do not hash files; the output cannot be ingested"));
    return r;
  }
  int Main(const ArgumentList &args);

  enum EntryType {
    kDir,
    kFile,
    kLink
  };

  struct Entry {
    EntryType type;
    std::string name;  ///< repository path
    std::string local_path;
    mode_t mode;  ///< permission bits only
    int64_t mtime_ns;
    uid_t uid;
    gid_t gid;
    uint64_t size;
    std::string acl;
    bool nested;
    std::string target;
    std::vector<std::string> hashes;
    Entry()
        : type(kFile)
        , mode(0)
        , mtime_ns(0)
        , uid(0)
        , gid(0)
        , size(0)
        , nested(false) { }
  };

 private:
  bool Scan(const std::string &local_path, const std::string &name);
  bool ScanDir(const std::string &local_path, const std::string &name);
  bool HashFiles(unsigned num_threads);
  bool IsNested(const std::string &local_path, const std::string &name) const;
  void Warn(const std::string &local_path, const std::string &msg);

  void PrintSql(const std::string &input) const;
  void PrintTable() const;
  bool WriteDatabase(const std::string &path) const;

  std::vector<Entry> entries_;
  std::vector<std::string> nested_globs_;
  std::map<std::pair<dev_t, ino_t>, std::string> hardlinks_;
  bool no_hash_;
  bool override_owner_;
  uid_t uid_;
  gid_t gid_;
  unsigned num_skipped_;
  unsigned num_warnings_;
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_INGESTSQL_GEN_H_
