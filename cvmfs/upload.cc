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
  vector<string> upstream = SplitString(components[0], ':', 2);
  if (upstream.size() != 2) {
    LogCvmfs(kLogSpooler, kLogStderr, "Invalid spooler driver");
    return;
  }

  // recognize and configure the spooler driver
  if (upstream[0] == "local") {
    driver_type   = Local;
  } else if (upstream[0] == "riak") {
    driver_type   = Riak;
  } else {
    LogCvmfs(kLogSpooler, kLogStderr, "unknown spooler driver: %s",
      upstream[0].c_str());
    return;
  }
  spooler_description = upstream[1];

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

  // create a Spooler frontend
  return new Spooler(spooler_definition);
}


Spooler::Spooler(const SpoolerDefinition &spooler_definition) :
  spooler_callback_(NULL),
  move_mode_(false)
{
  // create a spooler backend object according to the spooler_definition
  // THIS CODE IS SUBJECT TO CHANGE
  int driver = (spooler_definition.driver_type == SpoolerDefinition::Riak) ? 1 : 0;
  backend_ = SpoolerBackend::Construct(driver, spooler_definition.spooler_description);

  atomic_init64(&num_pending_);
  atomic_init64(&num_errors_);
}


Spooler::~Spooler() {}


void Spooler::SpoolProcess(const string &local_path, const string &remote_dir,
                           const string &file_suffix) {
  backend_->Process(local_path, remote_dir, file_suffix, move_mode_);
}


void Spooler::SpoolCopy(const string &local_path, const string &remote_path) {
  backend_->Copy(local_path, remote_path, move_mode_);
}


void Spooler::EndOfTransaction() {
  backend_->EndOfTransaction();
}


void Spooler::WaitFor() {
  backend_->Wait();
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
