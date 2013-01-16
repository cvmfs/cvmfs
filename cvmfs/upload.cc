/**
 * This file is part of the CernVM File System.
 *
 * The Spooler class provides an interface to push files to a storage
 * component.  Copy/Move commands and paths are send via a pipe and the result
 * (e.g. the SHA-1 digest) is received from the spooler via another pipe.
 */

#include "upload.h"

#include <fcntl.h>
#include <errno.h>
#include <unistd.h>

#include <cstdio>
#include <cassert>
#include <cstdlib>

#include <vector>
#include <string>

#include "compression.h"
#include "util.h"
#include "logging.h"

using namespace std;  // NOLINT

namespace upload {

/**
 * Commands to the spooler
 */
enum Commands {
  kCmdProcess = 1,
  kCmdCopy,
  kCmdEndOfTransaction,
  kCmdMoveFlag = 128,
};


static bool GetString(FILE *f, std::string *str) {
  str->clear();
  do {
    int retval = getc_unlocked(f);
    if (retval == EOF)
      return false;
    char c = retval;
    if (c == '\0')
      return true;
    str->push_back(c);
  } while (true);
}


bool LocalStat::Stat(const string &path) {
  return FileExists(base_path_ + "/" + path);
}


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
  LogCvmfs(kLogSpooler, kLogVerboseMsg,
           "Default spooler connected to paths pipe");
  int fd_digests = open(fifo_digests.c_str(), O_WRONLY);
  if (fd_digests < 0) {
    fclose(fpaths);
    return 1;
  }
  LogCvmfs(kLogSpooler, kLogVerboseMsg,
           "Default spooler connected to digests pipe");

  int retval;
  while ((retval = getc_unlocked(fpaths)) != EOF) {
    bool move_file = false;
    if (retval & kCmdMoveFlag) {
      retval -= kCmdMoveFlag;
      move_file = true;
    }
    unsigned char command = retval;

    string local_path;
    string remote_path;
    string remote_dir;
    string file_suffix;
    string return_line = "";
    switch (command) {
      case kCmdEndOfTransaction:
        LogCvmfs(kLogSpooler, kLogVerboseMsg,
                 "Default spooler sends transaction ack back");
        return_line = "0";
        return_line.push_back('\0');
        return_line.push_back('\0');
        return_line.push_back('\n');
        WritePipe(fd_digests, return_line.data(), return_line.length());
        goto tear_down;
      case kCmdCopy:
        GetString(fpaths, &local_path);
        GetString(fpaths, &remote_path);
        remote_path = upstream_basedir + "/" + remote_path;
        LogCvmfs(kLogSpooler, kLogVerboseMsg,
                 "Default spooler received 'copy': source %s, dest %s move %d",
                 local_path.c_str(), remote_path.c_str(), move_file);
        if (move_file) {
          int retval = rename(local_path.c_str(), remote_path.c_str());
          return_line = (retval == 0) ? "0" : StringifyInt(errno);
        } else {
          int retval = CopyPath2Path(local_path, remote_path);
          return_line = retval ? "0" : "100";
        }
        return_line.push_back('\0');
        return_line.append(local_path);
        return_line.push_back('\0');
        return_line.push_back('\n');
        break;
      case kCmdProcess: {
        GetString(fpaths, &local_path);
        GetString(fpaths, &remote_dir);
        GetString(fpaths, &file_suffix);
        LogCvmfs(kLogSpooler, kLogVerboseMsg,
                 "Default spooler received 'process': source %s, dest %s, "
                 "postfix %s, move %d", local_path.c_str(),
                 remote_dir.c_str(), file_suffix.c_str(), move_file);

        hash::Any compressed_hash(hash::kSha1);
        remote_path = upstream_basedir + "/" + remote_dir;
        string tmp_path;
        FILE *fcas = CreateTempFile(remote_path + "/cvmfs", 0777, "w",
                                    &tmp_path);
        if (fcas == NULL) {
          return_line = "103";
        } else {
          int retval = zlib::CompressPath2File(local_path, fcas,
                                               &compressed_hash);
          return_line = retval ? "0" : "103";
          fclose(fcas);
          if (retval) {
            const string cas_path = remote_path + compressed_hash.MakePath(1, 2)
                                    + file_suffix;
            retval = rename(tmp_path.c_str(), cas_path.c_str());
            if (retval != 0) {
              unlink(tmp_path.c_str());
              return_line = "104";
            }
          }
        }
        if (move_file) {
          if (unlink(local_path.c_str()) != 0)
            return_line = "105";
        }
        return_line.push_back('\0');
        return_line.append(local_path);
        return_line.push_back('\0');
        return_line.append(compressed_hash.ToString());
        return_line.push_back('\n');
        break;
      }
      default:
        LogCvmfs(kLogSpooler, kLogVerboseMsg, "unknown command %d",
                 command);
        return_line = "1";
        return_line.push_back('\0');
        return_line.push_back('\0');
        return_line.push_back('\n');
        break;
    }
    LogCvmfs(kLogSpooler, kLogVerboseMsg,
             "Default spooler sends back result %s",
             return_line.c_str());
    WritePipe(fd_digests, return_line.data(), return_line.length());
  }

 tear_down:
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "Default spooler terminates");
  fclose(fpaths);
  close(fd_digests);
  return 0;
}


Spooler::Spooler(const string &fifo_paths, const string &fifo_digests) {
  atomic_init64(&num_pending_);
  atomic_init64(&num_errors_);
  fifo_paths_ = fifo_paths;
  fifo_digests_ = fifo_digests;
  spooler_callback_ = NULL;
  connected_ = false;
  move_mode_ = false;
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

  LogCvmfs(kLogSpooler, kLogVerboseMsg, "receiver thread started");
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
      LogCvmfs(kLogSpooler, kLogVerboseMsg, "received: %s %d %s",
               local_path.c_str(), result, line.c_str());
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
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "receiver thread stopped");

  return NULL;
}


bool Spooler::Connect() {
  fd_paths_ = open(fifo_paths_.c_str(), O_WRONLY);
  if (fd_paths_ < 0)
    return false;
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "connected to paths pipe (write)");

  fd_digests_ = open(fifo_digests_.c_str(), O_RDONLY);
  if (fd_digests_ < 0) {
    close(fd_paths_);
    fd_paths_ = -1;
    return false;
  }
  fdigests_ = fdopen(fd_digests_, "r");
  assert(fdigests_);
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "connected to digests pipe (read)");

  // Start reveiver thread
  int retval = pthread_create(&thread_receive_, NULL, MainReceive, this);
  assert(retval == 0);

  connected_ = true;
  return true;
}


void Spooler::SpoolProcess(const string &local_path, const string &remote_dir,
                           const string &file_postfix)
{
  string line = "";
  unsigned char command = kCmdProcess;
  if (move_mode_) command |= kCmdMoveFlag;
  line.push_back(command);
  line.append(local_path);
  line.push_back('\0');
  line.append(remote_dir);
  line.push_back('\0');
  line.append(file_postfix);
  line.push_back('\0');
  WritePipe(fd_paths_, line.data(), line.size());
  atomic_inc64(&num_pending_);
}


void Spooler::SpoolCopy(const string &local_path, const string &remote_path) {
  string line = "";
  unsigned char command = kCmdCopy;
  if (move_mode_) command |= kCmdMoveFlag;
  line.push_back(command);
  line.append(local_path);
  line.push_back('\0');
  line.append(remote_path);
  line.push_back('\0');
  WritePipe(fd_paths_, line.data(), line.size());
  atomic_inc64(&num_pending_);
}


void Spooler::EndOfTransaction() {
  char command = kCmdEndOfTransaction;
  WritePipe(fd_paths_, &command, 1);
  atomic_inc64(&num_pending_);
}


void Spooler::WaitFor() {
  while (!IsIdle())
    SafeSleepMs(100);
}


Spooler *MakeSpoolerEnsemble(const std::string &spooler_definition) {
  string upstream_driver;
  string upstream_path;
  string paths_out;
  string digests_in;

  vector<string> components = SplitString(spooler_definition, ',');
  if (components.size() != 3) {
    PrintError("Invalid spooler definition");
    return NULL;
  }
  vector<string> upstream = SplitString(components[0], ':');
  if ((upstream.size() != 2) || (upstream[0] != "local")) {
    PrintError("Invalid spooler driver");
    return NULL;
  }
  upstream_driver = upstream[0];
  upstream_path = upstream[1];
  paths_out = components[1];
  digests_in = components[2];

  int pid = fork();
  assert(pid >= 0);
  if (pid == 0) {
    int retval = 1;
    if (upstream_driver == "local")
      retval = upload::MainLocalSpooler(paths_out, digests_in, upstream_path);
    exit(retval);
  }

  Spooler *spooler = new Spooler(paths_out, digests_in);
  bool retval = spooler->Connect();
  if (!retval) {
    PrintError("Failed to connect to spooler");
    return NULL;
  }

  return spooler;
}


BackendStat *GetBackendStat(const string &spooler_definition) {
  vector<string> components = SplitString(spooler_definition, ',');
  vector<string> upstream = SplitString(components[0], ':');
  if ((upstream.size() != 2) || (upstream[0] != "local")) {
    PrintError("Invalid upstream");
    return NULL;
  }
  return new LocalStat(upstream[1]);
}

}  // namespace upload
