/**
 * This file is part of the CernVM File System.
 *
 * The Forlift classes provide an interface to push files to a storage 
 * component.
 */

#include "upload.h"

#include <fcntl.h>
#include <errno.h>

#include <cstdio>
#include <cassert>

#include "compression.h"
#include "util.h"

using namespace std;  // NOLINT

namespace upload {
  
/**
 * A simple spooler in case upstream storage is local.
 * Compresses and hashes files and stores them on the upstream path.
 * Meant to be forked.
 */
int MainLocalSpooler(const string &fifo_paths, 
                     const string &fifo_digests,
                     const string &upstream_basedir) 
{
  FILE *fpaths = fopen(fifo_paths.c_str(), "r");
  if (!fpaths)
    return 1;
  printf("Spooler connected paths\n");
  int fd_digests = open(fifo_digests.c_str(), O_WRONLY);
  if (fd_digests < 0)
    return 1;
  printf("Spooler connected digests\n");
  
  string line;
  string local_path;
  string remote_path;
  string path_postfix;
  bool carbon_copy;
  int retval;
  while ((retval = getc_unlocked(fpaths)) != EOF) {
    char next_char = retval;

    if (next_char == '\0') {
      if (local_path.empty())
        local_path = line;
      else if (remote_path.empty())
        remote_path = line;
      else if (path_postfix.empty())
        path_postfix = line;
      line.clear();
    } else if (next_char == '\n') {
      // End of Transaction?
      if (local_path.empty()) {
        printf("sending transaction ack back\n");
        string return_line = "0";
        return_line.push_back('\0');
        return_line.push_back('\0');
        return_line.push_back('\n');
        WritePipe(fd_digests, return_line.data(), return_line.length());
        break;
      }
      
      carbon_copy = (line == "0");
      remote_path = upstream_basedir + "/" + remote_path;
      printf("Spooler received line: local %s remote %s postfix %s carbon copy %d\n", local_path.c_str(), remote_path.c_str(), path_postfix.c_str(), carbon_copy);
      
      unsigned result;
      hash::Any compressed_hash(hash::kSha1);
      if (carbon_copy) {
        // Just copy
        result = CopyPath2Path(local_path, remote_path) ? 0 : 100;
      } else {
        // Compress and hash, remote_path is the hosting directory
        string tmp_path;
        FILE *fcas = CreateTempFile(remote_path + "/cvmfs", 0777,
                                    "w", &tmp_path);
        if (fcas == NULL) {
          result = errno;
        } else {
          result = zlib::CompressPath2File(local_path, fcas, &compressed_hash) ?
                   0 : 101;
          fclose(fcas);
          if (result == 0) {
            const string cas_path = remote_path + compressed_hash.MakePath(1, 2)  
                                    + path_postfix;
            int retval = rename(tmp_path.c_str(), cas_path.c_str());
            if (retval != 0) {
              unlink(tmp_path.c_str());
              result = errno;
            }
          }
        }
      }
      printf("sending back: %d %s %s\n", result, local_path.c_str(), compressed_hash.ToString().c_str());
      string return_line = StringifyInt(result);
      return_line.push_back('\0');
      return_line.append(local_path);
      return_line.push_back('\0');
      if (!carbon_copy)
        return_line.append(compressed_hash.ToString());
      return_line.push_back('\n');
      WritePipe(fd_digests, return_line.data(), return_line.length());
      
      line.clear();
      local_path.clear();
      remote_path.clear();
      path_postfix.clear();
    } else {
      line.push_back(next_char);
    }
  }
  
  printf("Spooler exists\n");
  return 0;
}
  
Spooler::Spooler(const string &fifo_paths, const string &fifo_digests) {
  atomic_init64(&num_pending_);
  atomic_init64(&num_errors_);
  fifo_paths_ = fifo_paths;
  fifo_digests_ = fifo_digests;
  spooler_callback_ = NULL;
  connected_ = false;
  fd_digests_ = fd_paths_ = -1;
  fdigests_ = NULL;
}
  
Spooler::~Spooler() {
  if (connected_) {
    close(fd_paths_);
    fclose(fdigests_);
    pthread_join(thread_receive_, NULL);
  }
}
  
void *Spooler::MainReceive(void *caller) {
  Spooler *spooler = reinterpret_cast<Spooler *>(caller);
  
  printf("receiver started\n");
  string line;
  string local_path;
  int result = -1;
  int retval;
  while ((retval = getc(spooler->fdigests_)) != EOF) {
    char next_char = retval;
    
    if (next_char == '\0') {
      if (result == -1)
        result = String2Uint64(line);
      else
        local_path = line;
      line.clear();
    } else if (next_char == '\n') {
      printf("received: %s %d %s\n", local_path.c_str(), result, line.c_str());
      if (result != 0)
        atomic_inc64(&spooler->num_errors_);
      if (spooler->spooler_callback())
        spooler->spooler_callback()->Callback(local_path, result, line);
      atomic_dec64(&(spooler->num_pending_));
      
      // End of transaction
      if (local_path.empty())
        break;
      
      result = -1;
      local_path.clear();
      line.clear();
    } else {
      line.push_back(next_char);
    }
  }
  printf("receiver stopped\n");
  
  return NULL;
}
  
bool Spooler::Connect() {
  fd_paths_ = open(fifo_paths_.c_str(), O_WRONLY);
  if (fd_paths_ < 0)
    return false;
  printf("write connected\n");
  
  fd_digests_ = open(fifo_digests_.c_str(), O_RDONLY);
  if (fd_digests_ < 0) {
    close(fd_paths_);
    fd_paths_ = -1;
    return false;
  }
  fdigests_ = fdopen(fd_digests_, "r");
  assert(fdigests_);
  printf("read connected\n");

  // Start reveiver thread
  int retval = pthread_create(&thread_receive_, NULL, MainReceive, this);
  assert(retval == 0);
  
  connected_ = true;
  return true;
}

  
void Spooler::SpoolProcess(const string &local_path, const string &remote_dir,
                           const string &file_postfix)
{
  string line = local_path;
  line.push_back('\0');
  line.append(remote_dir);
  line.push_back('\0');
  line.append(file_postfix);
  line.push_back('\0');
  line.push_back('1');
  line.push_back('\n');
  WritePipe(fd_paths_, line.data(), line.size());
  atomic_inc64(&num_pending_);
}
  

void Spooler::SpoolCopy(const string &local_path, const string &remote_path) {
  string line = local_path;
  line.push_back('\0');
  line.append(remote_path);
  line.push_back('\0');
  line.push_back('\0');
  line.push_back('0');
  line.push_back('\n');
  WritePipe(fd_paths_, line.data(), line.size());
  atomic_inc64(&num_pending_);
}
  
void Spooler::EndOfTransaction() {
  string line = "";
  line.push_back('\0');
  line.push_back('\0');
  line.push_back('\0');
  line.push_back('0');
  line.push_back('\n');
  WritePipe(fd_paths_, line.data(), line.size());
  atomic_inc64(&num_pending_);
}

}  // namespace upload
