/**
 * This file is part of the CernVM File System.
 */

#include "upload.h"

#include <vector>

#include "upload_local.h"

#include "util_concurrency.h"

using namespace upload;

AbstractSpooler::SpoolerDefinition::SpoolerDefinition(
                                       const std::string& definition_string) :
  valid_(false)
{
  // split the spooler definition into spooler driver and pipe definitions
  std::vector<std::string> components = SplitString(definition_string, ',');
  if (components.size() != 3) {
    LogCvmfs(kLogSpooler, kLogStderr, "Invalid spooler definition");
    return;
  }

  // split the spooler driver definition into name and config part
  std::vector<std::string> upstream = SplitString(components[0], ':', 2);
  if (upstream.size() != 2) {
    LogCvmfs(kLogSpooler, kLogStderr, "Invalid spooler driver");
    return;
  }

  // recognize and configure the spooler driver
  if (upstream[0] == "local") {
    driver_type = Local;
  // } else if (upstream[0] == "riak") {
  //   driver_type = Riak;
  } else {
    LogCvmfs(kLogSpooler, kLogStderr, "unknown spooler driver: %s",
      upstream[0].c_str());
    return;
  }

  // save data
  spooler_description = upstream[1];
  temp_directory = "/ramdisk/tmp"; // TODO: make this configurable

  valid_ = true;
}

AbstractSpooler* AbstractSpooler::Construct(
                                      const std::string &spooler_description) {
  // parse the spooler description string
  SpoolerDefinition spooler_definition(spooler_description);
  assert (spooler_definition.IsValid());

  // create a concrete Spooler object dependent on the parsed definition
  AbstractSpooler *spooler = NULL;
  switch (spooler_definition.driver_type) {
    case SpoolerDefinition::Local:
      spooler = new LocalSpooler(spooler_definition);
      break;

    // case SpoolerDefinition::Riak:
    //   spooler = new RiakSpooler(spooler_definition);
    //   break;

    default:
      LogCvmfs(kLogSpooler, kLogStderr, "invalid spooler definition");
  }

  // check if we managed to produce a sound spooler
  if (!spooler) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to create spooler object");
    return NULL;
  }

  // initialize the spooler and return it to the user
  if (!spooler->Initialize()) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to intialize spooler object");
    delete spooler;
    return NULL;
  }

  // all done... deliver what the user was asking for
  return spooler;
}


AbstractSpooler::AbstractSpooler(const SpoolerDefinition &spooler_definition) :
  spooler_definition_(spooler_definition),
  move_(false)
{}


AbstractSpooler::~AbstractSpooler() {}


// -----------------------------------------------------------------------------

bool LocalStat::Stat(const std::string &path) {
  return FileExists(base_path_ + "/" + path);
}

namespace upload {

  BackendStat *GetBackendStat(const std::string &spooler_definition) {
    std::vector<std::string> components = SplitString(spooler_definition, ',');
    std::vector<std::string> upstream = SplitString(components[0], ':');
    if ((upstream.size() != 2) || (upstream[0] != "local")) {
      PrintError("Invalid upstream");
      return NULL;
    }
    return new LocalStat(upstream[1]);
  }
  
}
