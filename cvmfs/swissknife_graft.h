/**
 * This file is part of the CernVM File System.
 *
 * It assists in creating graft files for publishing
 * in CVMFS.
 */

#ifndef CVMFS_SWISSKNIFE_GRAFT_H_
#define CVMFS_SWISSKNIFE_GRAFT_H_

#include <string>

#include "swissknife.h"

namespace swissknife {

class CommandGraft : public Command {
 public:
  ~CommandGraft() { }
  std::string GetName() { return "graft"; }
  std::string GetDescription() {
    return "Creates a graft file for publishing in CVMFS.";
  }
  ParameterList GetParams() {
    ParameterList r;
    r.push_back(Parameter::Mandatory('i', "Input file to process ('-' for reading"
                                       " from stdin)"));
    r.push_back(Parameter::Optional('o', "Output location for graft file"));
    r.push_back(Parameter::Switch('v', "Verbose output"));
    return r;
  }

  int Main(const ArgumentList &args);
  int Publish(const std::string &input_file, const std::string &output_file, bool output_file_is_dir, bool input_file_is_stdin);
  int Recurse(const std::string &input_file, const std::string &output_file);
  int ChecksumFd(int fd, shash::Any &id, off_t &processed_size);

 private:
  void FileCallback(const std::string &relative_path,
                    const std::string &file_name);
  bool DirCallback(const std::string &relative_path,
                   const std::string &dir_name);

  std::string m_output_file;
  std::string m_input_file;
  bool m_verbose;
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_GRAFT_H_
