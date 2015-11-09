/**
 * This file is part of the CernVM File System.
 *
 * Process a set of input files and create appropriate graft files.
 */

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include <cstdio>
#include <iostream>
#include <vector>
#include <sstream>

#include "compression.h"
#include "fs_traversal.h"
#include "hash.h"

#include "swissknife_graft.h"

#define CHUNK_SIZE 16*1024

int swissknife::CommandGraft::ChecksumFd(int fd, shash::Any &id, off_t &processed_size) {  // NOLINT
  shash::ContextPtr hash_context(id.algorithm);
  hash_context.buffer = alloca(hash_context.size);
  shash::Init(hash_context);

  z_stream strm;
  zlib::CompressInit(&strm);
  zlib::StreamStates retval;

  std::vector<unsigned char> buf; buf.reserve(CHUNK_SIZE);
  off_t cksum_size = 0;
  bool eof;

  do {
    ssize_t nbytes = read(fd, &buf[0], CHUNK_SIZE);
    if (nbytes < 0) {
      if (errno == EINTR) {continue;}
      zlib::CompressFini(&strm);
      return nbytes;
    }
    cksum_size += nbytes;
    eof = nbytes == 0;
    retval = zlib::CompressZStream2Null(&buf[0], nbytes, eof, &strm,
                                        &hash_context);
    if (retval == zlib::kStreamDataError) {
      zlib::CompressFini(&strm);
      errno = EINVAL;
      return -1;
    }
  } while (!eof);

  zlib::CompressFini(&strm);
  if (retval != zlib::kStreamEnd) {
    errno = EINVAL;
    return -1;
  }
  shash::Final(hash_context, &id);
  processed_size = cksum_size;
  return 0;
}


static void splitpath(const std::string &path,
                      std::string &out_dirname,
                      std::string &out_fname) {  // NOLINT
  size_t dir_sep = path.rfind('/');
  if (dir_sep != std::string::npos) {
    out_dirname = path.substr(0, dir_sep+1);
    out_fname = path.substr(dir_sep+1);
  } else {
    out_dirname = ".";
    out_fname = path;
  }
}


bool swissknife::CommandGraft::DirCallback(const std::string &relative_path,
                                           const std::string &dir_name) {
  if (!m_output_file.size()) {return true;}
  std::string full_path = m_output_file + "/" + 
                          (relative_path.size() ? relative_path : ".") + "/" + dir_name;
  if (-1 == mkdir(full_path.c_str(), 0755)) {
    if (errno == EEXIST) {
      struct stat sbuf;
      if ((stat(full_path.c_str(), &sbuf) == 0) && (S_ISDIR(sbuf.st_mode))) {
        return true;
      }
    }
    return false;
  }
  return true;
}


void swissknife::CommandGraft::FileCallback(const std::string &relative_path,
                                            const std::string &file_name) {
  std::string full_input_path = m_input_file + "/" +
                                (relative_path.size() ? relative_path : ".") + "/" + file_name;
  std::string full_output_path;
  if (m_output_file.size()) {
    full_output_path = m_output_file + "/" +
                       (relative_path.size() ? relative_path : ".") + "/" + file_name;
  }
  Publish(full_input_path, full_output_path, false, false);
}


int swissknife::CommandGraft::Recurse(const std::string &input_file, const std::string &output_file) {
  m_output_file = output_file;
  m_input_file = input_file;

  FileSystemTraversal<CommandGraft> traverser(this, input_file, true);
  traverser.fn_new_file       = &CommandGraft::FileCallback;
  traverser.fn_new_dir_prefix = &CommandGraft::DirCallback;
  traverser.Recurse(input_file);
  return 1;
}


int swissknife::CommandGraft::Main(const swissknife::ArgumentList &args) {
  const std::string &input_file = *args.find('i')->second;
  const std::string output_file = (args.find('o') == args.end()) ? "" : *(args.find('o')->second);
  m_verbose = args.find('v') != args.end();

  struct stat sbuf;
  bool output_file_is_dir = output_file.size() &&
                            (0 == stat(output_file.c_str(), &sbuf)) &&
                            S_ISDIR(sbuf.st_mode);
  if (output_file_is_dir && (input_file == "-")) {
    std::cerr << "Output file (" << output_file << "): Is a directory" << std::endl;
    return 1;
  }

  if (input_file != "-") {
    bool input_file_is_dir = (0 == stat(input_file.c_str(), &sbuf)) &&
                            S_ISDIR(sbuf.st_mode);
    if (input_file_is_dir) {
      if (!output_file_is_dir && output_file.size()) {
        std::cerr << "Input (" << input_file << ") is a directory but output (" << output_file << ") is not" << std::endl;
        return 1;
      }
      if (m_verbose) {
        std::cerr << "Recursing into directory " << input_file << std::endl;
      }
      return Recurse(input_file, output_file);
    } else {
      return Publish(input_file, output_file, output_file_is_dir, false);
    }
  }
  return Publish(input_file, output_file, output_file_is_dir, true);
}


int swissknife::CommandGraft::Publish(const std::string &input_file, const std::string &output_file, bool output_file_is_dir, bool input_file_is_stdin) {
  if (output_file.size() && m_verbose) {
    std::cout << "Grafting " << input_file << " to " << output_file << std::endl;
  } else if (!output_file.size()) {
    std::cout << "Grafting " << input_file << std::endl;
  }
  int fd;
  if (input_file_is_stdin) {
    fd = 0;
  } else {
    fd = open(input_file.c_str(), O_RDONLY, 0755);
    if (fd < 0) {
      std::stringstream ss; ss << "Unable to open input file (" << input_file << ")";
      perror(ss.str().c_str());
      return 1;
    }
  }

  shash::Any hash(shash::kSha1);
  off_t processed_size;
  int retval = ChecksumFd(fd, hash, processed_size);
  if (!input_file_is_stdin) {close(fd);}
  if (retval) {
    std::stringstream ss; ss << "Unable to checksum input file (" << input_file << ")";
    perror(ss.str().c_str());
    return 1;
  }

  // Build the .cvmfsgraft-$filename
  if (output_file.size()) {
    std::string dirname, fname;
    std::string graft_fname;
    if (output_file_is_dir) {
      splitpath(input_file, dirname, fname);
      graft_fname =  output_file + "/.cvmfsgraft-" + fname;
    } else {
      splitpath(output_file, dirname, fname);
      graft_fname = dirname + "/.cvmfsgraft-" + fname;
    }
    fd = open(graft_fname.c_str(), O_CREAT|O_TRUNC|O_WRONLY, 0755);
    if (fd < 0) {
      if ((errno == EISDIR) && !input_file_is_stdin) {
        splitpath(input_file, dirname, fname);
        graft_fname = output_file + "/.cvmfsgraft-" + fname;
        fd = open(graft_fname.c_str(), O_CREAT|O_TRUNC|O_WRONLY, 0755);
      }
      if (fd < 0) {
        std::stringstream ss; ss << "Unable to open graft file (" << graft_fname << ")";
        perror(ss.str().c_str());
        return 1;
      }
    }
  } else {
    fd = 1;
  }
  std::stringstream ss;
  ss << "size=" << processed_size << std::endl;
  ss << "checksum=" << hash.ToString(true) << std::endl;
  std::string graft_contents = ss.str();
  size_t nbytes = graft_contents.size();
  const char *buf = graft_contents.c_str();
  while (nbytes) {
    int retval = write(fd, buf, nbytes);
    if (retval < 0) {
      if (errno == EINTR) {continue;}
      perror("Failed writing to graft file");
      return 1;
    }
    buf += retval;
    nbytes -= retval;
  }
  if (output_file.size()) {close(fd);}
  else {return 0;}

  // Create and truncate the output file.
  std::string output_fname;
  if (output_file_is_dir) {
    std::string dirname, fname;
    splitpath(input_file, dirname, fname);
    output_fname = output_file + "/" + fname;
  } else {
    output_fname = output_file;
  }
  fd = open(output_fname.c_str(), O_CREAT|O_TRUNC|O_WRONLY, 0755);
  if (fd < 0) {
    std::stringstream ss; ss << "Unable to open output file (" << output_file << ")";
    perror(ss.str().c_str());
    return 1;
  }
  close(fd);

  return 0;
}
