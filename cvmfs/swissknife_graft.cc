/**
 * This file is part of the CernVM File System.
 *
 * Process a set of input files and create appropriate graft files.
 */

#include "swissknife_graft.h"

#include <fcntl.h>
#include <unistd.h>

#include <cstdio>
#include <vector>

#include "compression.h"
#include "fs_traversal.h"
#include "hash.h"
#include "platform.h"


bool swissknife::CommandGraft::DirCallback(const std::string &relative_path,
                                           const std::string &dir_name) {
  if (!m_output_file_.size()) {return true;}
  std::string full_output_path = m_output_file_ + "/" +
                          (relative_path.size() ? relative_path : ".") +
                          "/" + dir_name;
  std::string full_input_path = m_input_file_ + "/" +
                                (relative_path.size() ? relative_path : ".") +
                                "/" + dir_name;
  platform_stat64 sbuf;
  if (-1 == platform_stat(full_input_path.c_str(), &sbuf)) {
    return false;
  }
  return MkdirDeep(full_output_path, sbuf.st_mode);
}


void swissknife::CommandGraft::FileCallback(const std::string &relative_path,
                                            const std::string &file_name) {
  std::string full_input_path = m_input_file_ + "/" +
                                (relative_path.size() ? relative_path : ".") +
                                "/" + file_name;
  std::string full_output_path;
  if (m_output_file_.size()) {
    full_output_path = m_output_file_ + "/" +
                       (relative_path.size() ? relative_path : ".") +
                       "/" + file_name;
  }
  Publish(full_input_path, full_output_path, false, false);
}


int swissknife::CommandGraft::Recurse(const std::string &input_file,
                                      const std::string &output_file) {
  m_output_file_ = output_file;
  m_input_file_ = input_file;

  FileSystemTraversal<CommandGraft> traverser(this, input_file, true);
  traverser.fn_new_file       = &CommandGraft::FileCallback;
  traverser.fn_new_dir_prefix = &CommandGraft::DirCallback;
  traverser.Recurse(input_file);
  return 0;
}


int swissknife::CommandGraft::Main(const swissknife::ArgumentList &args) {
  const std::string &input_file = *args.find('i')->second;
  const std::string output_file =
      (args.find('o') == args.end()) ? "" : *(args.find('o')->second);
  m_verbose_ = args.find('v') != args.end();

  platform_stat64 sbuf;
  bool output_file_is_dir = output_file.size() &&
                            (0 == platform_stat(output_file.c_str(), &sbuf)) &&
                            S_ISDIR(sbuf.st_mode);
  if (output_file_is_dir && (input_file == "-")) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Output file (%s): Is a directory\n",
             output_file.c_str());
    return 1;
  }

  if (input_file != "-") {
    bool input_file_is_dir = (0 == platform_stat(input_file.c_str(), &sbuf)) &&
                            S_ISDIR(sbuf.st_mode);
    if (input_file_is_dir) {
      if (!output_file_is_dir && output_file.size()) {
        LogCvmfs(kLogCvmfs, kLogStderr, "Input (%s) is a directory but output"
                " (%s) is not\n",
                input_file.c_str(), output_file.c_str());
        return 1;
      }
      if (m_verbose_) {
        LogCvmfs(kLogCvmfs, kLogStderr, "Recursing into directory %s\n",
                 input_file.c_str());
      }
      return Recurse(input_file, output_file);
    } else {
      return Publish(input_file, output_file, output_file_is_dir, false);
    }
  }
  return Publish(input_file, output_file, output_file_is_dir, true);
}


int swissknife::CommandGraft::Publish(const std::string &input_file,
                                      const std::string &output_file,
                                      bool output_file_is_dir,
                                      bool input_file_is_stdin) {
  if (output_file.size() && m_verbose_) {
    LogCvmfs(kLogCvmfs, kLogStdout, "Grafting %s to %s", input_file.c_str(),
             output_file.c_str());
  } else if (!output_file.size()) {
    LogCvmfs(kLogCvmfs, kLogStdout, "Grafting %s", input_file.c_str());
  }
  int fd;
  if (input_file_is_stdin) {
    fd = 0;
  } else {
    fd = open(input_file.c_str(), O_RDONLY);
    if (fd < 0) {
      std::string errmsg = "Unable to open input file (" + input_file + ")";
      perror(errmsg.c_str());
      return 1;
    }
  }

  // Get input file mode; output file will be set identically.
  platform_stat64 sbuf;
  if (-1 == platform_fstat(fd, &sbuf)) {
    std::string errmsg = "Unable to stat input file (" + input_file + ")";
    perror(errmsg.c_str());
  }
  mode_t input_file_mode = sbuf.st_mode;

  shash::Any hash(shash::kSha1);
  uint64_t processed_size;
  bool retval = zlib::CompressFd2Null(fd, &hash, &processed_size);
  if (!input_file_is_stdin) {close(fd);}
  if (!retval) {
    std::string errmsg = "Unable to checksum input file (" + input_file + ")";
    perror(errmsg.c_str());
    return 1;
  }

  // Build the .cvmfsgraft-$filename
  if (output_file.size()) {
    std::string dirname, fname;
    std::string graft_fname;
    if (output_file_is_dir) {
      SplitPath(input_file, dirname, fname);
      graft_fname =  output_file + "/.cvmfsgraft-" + fname;
    } else {
      SplitPath(output_file, dirname, fname);
      graft_fname = dirname + "/.cvmfsgraft-" + fname;
    }
    fd = open(graft_fname.c_str(), O_CREAT|O_TRUNC|O_WRONLY, 0644);
    if (fd < 0) {
      std::string errmsg = "Unable to open graft file (" + graft_fname + ")";
      perror(errmsg.c_str());
      return 1;
    }
  } else {
    fd = 1;
  }
  std::string graft_contents = "size=" + StringifyInt(processed_size) + "\n" +
                               "checksum=" + hash.ToString(true) + "\n";
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
  if (output_file.size()) {
    close(fd);
  } else {
    return 0;
  }

  // Create and truncate the output file.
  std::string output_fname;
  if (output_file_is_dir) {
    std::string dirname, fname;
    SplitPath(input_file, dirname, fname);
    output_fname = output_file + "/" + fname;
  } else {
    output_fname = output_file;
  }
  fd = open(output_fname.c_str(), O_CREAT|O_TRUNC|O_WRONLY, input_file_mode);
  if (fd < 0) {
    std::string errmsg = "Unable to open output file (" + output_file + ")";
    perror(errmsg.c_str());
    return 1;
  }
  close(fd);

  return 0;
}
