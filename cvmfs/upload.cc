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

#include "upload_local.h"
#include "upload_riak.h"


using namespace std;  // NOLINT

namespace upload {

bool LocalStat::Stat(const string &path) {
  return FileExists(base_path_ + "/" + path);
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

  // Start receiver thread
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
    usleep(100000);  // 100 milliseconds
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

    AbstractSpoolerBackend *backend;

    if (upstream_driver == "local") {
      LogCvmfs(kLogSpooler, kLogVerboseMsg, "creating local spooler backend");
      LocalSpoolerBackend *local_backend = new LocalSpoolerBackend();
      local_backend->set_upstream_path(upstream_path);
      backend = local_backend;
    }

    if (upstream_driver == "riak") {
      LogCvmfs(kLogSpooler, kLogVerboseMsg, "creating riak spooler backend");
      RiakSpoolerBackend *riak_backend = new RiakSpoolerBackend();
      backend = riak_backend;
    }

    if (! backend->Connect(paths_out, digests_in)) {
      PrintError("failed to connect to spooler backend");
      delete backend;
      exit(2);
    } else {
      LogCvmfs(kLogSpooler, kLogVerboseMsg, "connected local spooler backend");
    }

    if (! backend->Initialize()) {
      PrintError("failed to initialize spooler backend");
      delete backend;
      exit(3);
    } else {
      LogCvmfs(kLogSpooler, kLogVerboseMsg, "initialized local spooler backend");
    }

    retval = backend->Run();
    LogCvmfs(kLogSpooler, kLogVerboseMsg, "spooler backend is up and running...");
    delete backend;

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
