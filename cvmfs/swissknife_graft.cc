/**
 * This file is part of the CernVM File System.
 *
 * Process a set of input files and create appropriate graft files.
 */

#include "swissknife_graft.h"
#include "cvmfs_config.h"

#include <fcntl.h>
#include <unistd.h>

#include <cstdio>
#include <vector>

#include "fs_traversal.h"
#include "hash.h"
#include "platform.h"
#include "util/posix.h"

bool swissknife::CommandGraft::ChecksumFdWithChunks(
    int fd, zlib::Compressor *compressor, uint64_t *file_size,
    shash::Any *file_hash, std::vector<uint64_t> *chunk_offsets,
    std::vector<shash::Any> *chunk_checksums) {
  if (!compressor || !file_size || !file_hash) {
    return false;
  }
  *file_size = 0;
  shash::Any chunk_hash(hash_alg_);
  ssize_t bytes_read;
  unsigned char in_buf[zlib::kZChunk];
  unsigned char *cur_in_buf = in_buf;
  size_t in_buf_size = zlib::kZChunk;
  unsigned char out_buf[zlib::kZChunk];
  size_t avail_in = 0;

  // Initialize the file and per-chunk checksums
  shash::ContextPtr file_hash_context(hash_alg_);
  file_hash_context.buffer = alloca(file_hash_context.size);
  shash::Init(file_hash_context);

  shash::ContextPtr chunk_hash_context(hash_alg_);
  chunk_hash_context.buffer = alloca(chunk_hash_context.size);
  shash::Init(chunk_hash_context);

  bool do_chunk = chunk_size_ > 0;
  if (do_chunk) {
    if (!chunk_offsets || !chunk_checksums) {
      return false;
    }
    chunk_offsets->push_back(0);
  }

  bool flush = 0;
  do {
    bytes_read = read(fd, cur_in_buf + avail_in, in_buf_size);
    if (-1 == bytes_read) {
      if (errno == EINTR) {
        continue;
      }
      LogCvmfs(kLogCvmfs, kLogStderr, "Failure when reading file: %s",
               strerror(errno));
      return false;
    }
    *file_size += bytes_read;
    avail_in += bytes_read;

    flush = (static_cast<size_t>(bytes_read) < in_buf_size);

    // If possible, make progress on deflate.
    unsigned char *cur_out_buf = out_buf;
    size_t avail_out = zlib::kZChunk;
    compressor->Deflate(flush, &cur_in_buf, &avail_in, &cur_out_buf,
                        &avail_out);
    if (do_chunk) {
      shash::Update(out_buf, avail_out, chunk_hash_context);
      if (generate_bulk_hash_)
        shash::Update(out_buf, avail_out, file_hash_context);
    } else {
      shash::Update(out_buf, avail_out, file_hash_context);
    }

    if (!avail_in) {
      // All bytes are consumed; set the buffer back to the beginning.
      cur_in_buf = in_buf;
      in_buf_size = zlib::kZChunk;
    } else {
      in_buf_size = zlib::kZChunk - (cur_in_buf - in_buf) - avail_in;
    }

    // Start a new hash if current one is above threshold
    if (do_chunk && (*file_size - chunk_offsets->back() >= chunk_size_)) {
      shash::Final(chunk_hash_context, &chunk_hash);
      chunk_offsets->push_back(*file_size);
      chunk_checksums->push_back(chunk_hash);
      shash::Init(chunk_hash_context);
    }
  } while (!flush);

  shash::Final(file_hash_context, file_hash);
  if (do_chunk) {
    shash::Final(chunk_hash_context, &chunk_hash);
    chunk_checksums->push_back(chunk_hash);
  }

  // Zero-size chunks are not allowed; except if there is only one chunk
  if (do_chunk && (chunk_offsets->back() == *file_size) &&
      (chunk_offsets->size() > 1))
  {
    chunk_offsets->pop_back();
    chunk_checksums->pop_back();
  }

  if (do_chunk && !generate_bulk_hash_)
    file_hash->SetNull();

  // Do not chunk a file if it is under threshold.
  if (do_chunk && (chunk_offsets->size() == 1)) {
    *file_hash = (*chunk_checksums)[0];
    chunk_offsets->clear();
    chunk_checksums->clear();
  }
  return true;
}

bool swissknife::CommandGraft::DirCallback(const std::string &relative_path,
                                           const std::string &dir_name) {
  if (!output_file_.size()) {
    return true;
  }
  std::string full_output_path = output_file_ + "/" +
                                 (relative_path.size() ? relative_path : ".") +
                                 "/" + dir_name;
  std::string full_input_path = input_file_ + "/" +
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
  std::string full_input_path = input_file_ + "/" +
                                (relative_path.size() ? relative_path : ".") +
                                "/" + file_name;
  std::string full_output_path;
  if (output_file_.size()) {
    full_output_path = output_file_ + "/" +
                       (relative_path.size() ? relative_path : ".") + "/" +
                       file_name;
  }
  Publish(full_input_path, full_output_path, false, false);
}

int swissknife::CommandGraft::Main(const swissknife::ArgumentList &args) {
  const std::string &input_file = *args.find('i')->second;
  const std::string output_file =
      (args.find('o') == args.end()) ? "" : *(args.find('o')->second);
  verbose_ = args.find('v') != args.end();
  generate_bulk_hash_ = args.find('b') != args.end();
  hash_alg_ = (args.find('a') == args.end())
                  ? shash::kSha1
                  : shash::ParseHashAlgorithm(*args.find('a')->second);
  compression_alg_ =
      (args.find('Z') == args.end())
          ? zlib::kNoCompression
          : zlib::ParseCompressionAlgorithm(*args.find('Z')->second);

  if (args.find('c') == args.end()) {
    chunk_size_ = kDefaultChunkSize;
  } else {
    std::string chunk_size = *args.find('c')->second;
    if (!String2Uint64Parse(chunk_size, &chunk_size_)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Unable to parse chunk size: %s",
               chunk_size.c_str());
      return 1;
    }
  }
  chunk_size_ *= 1024 * 1024;  // Convert to MB.

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
        LogCvmfs(kLogCvmfs, kLogStderr,
                 "Input (%s) is a directory but output"
                 " (%s) is not\n",
                 input_file.c_str(), output_file.c_str());
        return 1;
      }
      if (verbose_) {
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
  if (output_file.size() && verbose_) {
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
  mode_t input_file_mode = input_file_is_stdin ? 0644 : sbuf.st_mode;

  shash::Any file_hash(hash_alg_);
  uint64_t processed_size;
  std::vector<uint64_t> chunk_offsets;
  std::vector<shash::Any> chunk_checksums;
  zlib::Compressor *compressor = zlib::Compressor::Construct(compression_alg_);

  bool retval =
      ChecksumFdWithChunks(fd, compressor, &processed_size, &file_hash,
                           &chunk_offsets, &chunk_checksums);

  if (!input_file_is_stdin) {
    close(fd);
  }
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
      SplitPath(input_file, &dirname, &fname);
      graft_fname = output_file + "/.cvmfsgraft-" + fname;
    } else {
      SplitPath(output_file, &dirname, &fname);
      graft_fname = dirname + "/.cvmfsgraft-" + fname;
    }
    fd = open(graft_fname.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (fd < 0) {
      std::string errmsg = "Unable to open graft file (" + graft_fname + ")";
      perror(errmsg.c_str());
      return 1;
    }
  } else {
    fd = 1;
  }
  const bool with_suffix = true;
  std::string graft_contents = "size=" + StringifyInt(processed_size) + "\n" +
                               "checksum=" + file_hash.ToString(with_suffix) + "\n" +
                               "compression=" + zlib::AlgorithmName(compression_alg_) + "\n";
  if (!chunk_offsets.empty()) {
    std::vector<std::string> chunk_off_str;
    chunk_off_str.reserve(chunk_offsets.size());
    std::vector<std::string> chunk_ck_str;
    chunk_ck_str.reserve(chunk_offsets.size());
    for (unsigned idx = 0; idx < chunk_offsets.size(); idx++) {
      chunk_off_str.push_back(StringifyInt(chunk_offsets[idx]));
      chunk_ck_str.push_back(chunk_checksums[idx].ToStringWithSuffix());
    }
    graft_contents += "chunk_offsets=" + JoinStrings(chunk_off_str, ",") + "\n";
    graft_contents +=
        "chunk_checksums=" + JoinStrings(chunk_ck_str, ",") + "\n";
  }
  size_t nbytes = graft_contents.size();
  const char *buf = graft_contents.c_str();
  retval = SafeWrite(fd, buf, nbytes);
  if (!retval) {
    perror("Failed writing to graft file");
    close(fd);
    return 1;
  }
  if (output_file.size()) {
    close(fd);
  } else {
    return 0;
  }

  // Create and truncate the output file.
  std::string output_fname;
  if (output_file_is_dir) {
    output_fname = output_file + "/" + GetFileName(input_file);
  } else {
    output_fname = output_file;
  }
  fd =
      open(output_fname.c_str(), O_CREAT | O_TRUNC | O_WRONLY, input_file_mode);
  if (fd < 0) {
    std::string errmsg = "Unable to open output file (" + output_file + ")";
    perror(errmsg.c_str());
    return 1;
  }
  close(fd);

  return 0;
}

int swissknife::CommandGraft::Recurse(const std::string &input_file,
                                      const std::string &output_file) {
  output_file_ = output_file;
  input_file_ = input_file;

  FileSystemTraversal<CommandGraft> traverser(this, input_file, true);
  traverser.fn_new_file = &CommandGraft::FileCallback;
  traverser.fn_new_dir_prefix = &CommandGraft::DirCallback;
  traverser.Recurse(input_file);
  return 0;
}
