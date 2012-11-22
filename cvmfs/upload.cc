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


Spooler::SpoolerDefinition::SpoolerDefinition(
                                    const std::string& definition_string) :
  valid_(false)
{
  // split the spooler definition into spooler driver and pipe definitions
  vector<string> components = SplitString(definition_string, ',');
  if (components.size() != 3) {
    LogCvmfs(kLogSpooler, kLogStderr, "Invalid spooler definition");
    return;
  }

  // split the spooler driver definition into name and config part
  vector<string> upstream = SplitString(components[0], ':');
  if (upstream.size() != 2) {
    LogCvmfs(kLogSpooler, kLogStderr, "Invalid spooler driver");
    return;
  }

  // recognize and configure the spooler driver
  if (upstream[0] == "local") {
    driver_type   = Local;
    upstream_path = upstream[1];
  } else if (upstream[0] == "riak") {
    driver_type      = Riak;
    config_file_path = upstream[1];
  } else {
    LogCvmfs(kLogSpooler, kLogStderr, "unknown spooler driver: %s",
      upstream[0].c_str());
    return;
  }

  // save named pipe paths and validate this SpoolerDefinition
  paths_out_pipe  = components[1];
  digests_in_pipe = components[2];
  valid_ = true;
}


Spooler* Spooler::Construct(const std::string &definition_string) {
  // read the spooler definition
  SpoolerDefinition spooler_definition(definition_string);
  if (!spooler_definition.IsValid())
    return NULL;

  // spawn a SpoolerBackend according to the provided defintion
  SpawnSpoolerBackend(spooler_definition);

  // create a Spooler frontend and connect it to the backend
  Spooler *spooler = new Spooler();
  if (! spooler->Connect(spooler_definition.paths_out_pipe,
                         spooler_definition.digests_in_pipe)) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to connect to spooler");
    return NULL;
  }

  return spooler;
}


void Spooler::SpawnSpoolerBackend(
                      const Spooler::SpoolerDefinition &definition) {
  assert (definition.IsValid());

  // spawn spooler backend process
  int pid = fork();
  if (pid < 0) {
    LogCvmfs(kLogSpooler, kLogStderr, "failed to spawn spooler backend");
    assert(pid >= 0); // nothing to do here anymore... good bye
  }

  if (pid > 0)
    return;

  // ---------------------------------------------------------------------------
  // From here on we are in the SpoolerBackend process

  AbstractSpoolerBackend *backend = NULL;
  int retval = 1;

  // create a SpoolerBackend object of the requested type
  switch (definition.driver_type) {
    case SpoolerDefinition::Local:
      backend = new LocalSpoolerBackend(definition.upstream_path);
      break;

    case SpoolerDefinition::Riak:
      backend = new RiakSpoolerBackend(definition.config_file_path);
      break;

    default:
      LogCvmfs(kLogSpooler, kLogStderr, "invalid spooler definition");
      assert (false && definition.IsValid());
  }
  assert (backend != NULL);

  // connect the named pipes in the SpoolerBackend
  if (! backend->Connect(definition.paths_out_pipe,
                         definition.digests_in_pipe)) {
    LogCvmfs(kLogSpooler, kLogStderr, "failed to connect spooler backend");
    retval = 2;
    goto out;
  }
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "connected spooler backend");

  // do the final initialization of the SpoolerBackend
  if (! backend->Initialize()) {
    LogCvmfs(kLogSpooler, kLogStderr, "failed to initialize spooler backend");
    retval = 3;
    goto out;
  }
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "initialized spooler backend");

  // run the SpoolerBackend service
  // returns on a termination signal though the named pipes
  retval = backend->Run();

  // all done, good bye...
out:
  delete backend;
  exit(retval);
}


Spooler::Spooler() :
  spooler_callback_(NULL),
  connected_(false),
  move_mode_(false),
  fd_paths_(-1),
  fd_digests_(-1),
  fdigests_(NULL)
{
  atomic_init64(&num_pending_);
  atomic_init64(&num_errors_);
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


bool Spooler::Connect(const std::string &fifo_paths,
                      const std::string &fifo_digests) {
  fd_paths_ = open(fifo_paths.c_str(), O_WRONLY);
  if (fd_paths_ < 0)
    return false;
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "connected to paths pipe (write)");

  fd_digests_ = open(fifo_digests.c_str(), O_RDONLY);
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
                           const string &file_postfix) {
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
