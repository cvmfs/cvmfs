/**
 * This file is part of the CernVM File System.
 *
 * It assists in creating graft files for publishing in CVMFS.
 */

#ifndef CVMFS_SWISSKNIFE_GRAFT_H_
#define CVMFS_SWISSKNIFE_GRAFT_H_

#include <string>
#include <vector>

#include "compression.h"
#include "hash.h"

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
    r.push_back(Parameter::Mandatory('i', "Input file to process "
                                       "('-' for reading from stdin)"));
    r.push_back(Parameter::Optional('o', "Output location for graft file"));
    r.push_back(Parameter::Switch('v', "Verbose output"));
    r.push_back(Parameter::Optional('Z', "Compression algorithm "
                                    "(default: none)"));
    r.push_back(Parameter::Optional('c', "Chunk size (in MB; default: 32)"));
    r.push_back(Parameter::Optional('a', "hash algorithm (default: SHA-1)"));
    return r;
  }

  int Main(const ArgumentList &args);

 private:
  int Publish(const std::string &input_file, const std::string &output_file,
              bool output_file_is_dir, bool input_file_is_stdin);
  int Recurse(const std::string &input_file, const std::string &output_file);

  void FileCallback(const std::string &relative_path,
                    const std::string &file_name);
  bool DirCallback(const std::string &relative_path,
                   const std::string &dir_name);

  bool ChecksumFdWithChunks(int fd,
                            zlib::Compressor *compressor,
                            uint64_t *file_size,
                            shash::Any *file_hash,
                            std::vector<uint64_t> *chunk_offsets,
                            std::vector<shash::Any> *chunk_checksums);

  std::string output_file_;
  std::string input_file_;
  bool verbose_;
  zlib::Algorithms compression_alg_;
  shash::Algorithms hash_alg_;
  uint64_t chunk_size_;
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_GRAFT_H_
