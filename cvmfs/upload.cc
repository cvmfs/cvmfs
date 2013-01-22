/**
 * This file is part of the CernVM File System.
 */

#include "upload.h"

 #include <sys/mman.h>
 #include <fcntl.h>

#include <vector>

#include "upload_local.h"
//#include "upload_riak.h"

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
  } else if (upstream[0] == "riak") {
    driver_type = Riak;
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


bool AbstractSpooler::Initialize() {

  return true;
}

void AbstractSpooler::TearDown() {

}


AbstractSpooler::ProcessingWorker::ProcessingWorker(const worker_context *context) :
  destination_path_(context->destination_path),
  mapped_file_(NULL),
  mapped_size_(0) {}


void AbstractSpooler::ProcessingWorker::operator()(const expected_data &data) {
  // get data references to the provided job structure for convenience
  const std::string &local_path     = data.local_path;
  const bool         allow_chunking = data.allow_chunking;

  returned_data result;

  // map the file to process into memory
  if (! MapFile(local_path)) {
    result.return_code = 1;
    return;
  }

  // RAII... do the unmap in ANY case...
  BoundScopedCallback<AbstractSpooler::ProcessingWorker>
    unmap_file_(&AbstractSpooler::ProcessingWorker::UnmapFile, this);

  // chunk the file if needed
  if (allow_chunking && ! GenerateFileChunks(result)) {
    result.return_code = 2;
    goto fail;
  }

  // check if we only produced one chunk...
  if (result.file_chunks.size() == 1) {
    // ... and simply use that as bulk file as well
    result.bulk_file = result.file_chunks.front();

    // or generate an additional bulk version of the file
  } else if (! GenerateBulkFile(result)) {
    result.return_code = 3;
    goto fail;
  }

  // all done
  result.return_code = 0;
  master()->JobSuccessful(result);
  return;

fail:
  master()->JobFailed(result);
}


bool AbstractSpooler::ProcessingWorker::GenerateFileChunks(returned_data &data) {
  return true;
}


bool AbstractSpooler::ProcessingWorker::GenerateBulkFile(returned_data &data) {
  return true;
}



  // std::string tmp_path;
  // FILE *fcas = CreateTempFile(remote_path + "/cvmfs", 0777, "w",
  //                             &tmp_path);

  // int retcode = 0;
  // if (fcas == NULL) {
  //   retcode = 103;
  // } else {
  //   int retval = zlib::CompressPath2File(local_path,
  //                                        fcas,
  //                                       &compressed_hash);
  //   retcode = retval ? 0 : 103;
  //   fclose(fcas);
  //   if (retval) {
  //     const std::string cas_path = remote_path + compressed_hash.MakePath(1, 2)
  //                             + file_suffix;
  //     retval = rename(tmp_path.c_str(), cas_path.c_str());
  //     if (retval != 0) {
  //       unlink(tmp_path.c_str());
  //       retcode = 104;
  //     }
  //   }
  // }
  // if (move) {
  //   if (unlink(local_path.c_str()) != 0)
  //     retcode = 105;
  // }

  // // inform the scheduler of the compression results, which in turn will invoke
  // // listener's callbacks
  // returned_data return_values(retcode,
  //                             local_path,
  //                             compressed_hash);

  // if (retcode == 0) {
  //   master()->JobSuccessful(return_values);
  // } else {
  //   master()->JobFailed(return_values);
  // }



bool AbstractSpooler::ProcessingWorker::MapFile(const std::string &file_path) {
  assert (mapped_size_ == 0 && mapped_file_ == NULL);

  // open the file
  int fd;
  if ((fd = open(file_path.c_str(), O_RDONLY, 0)) == -1) {
    LogCvmfs(kLogSpooler, kLogStderr, "failed to open %s", file_path.c_str());
    return false;
  }

  // get file size
  platform_stat64 filesize;
  if (platform_fstat(fd, &filesize) != 0) {
    LogCvmfs(kLogSpooler, kLogStderr, "failed to fstat %s", file_path.c_str());
    return false;
  }

  // map the given file into memory
  void *mapping = mmap(NULL, filesize.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
  if (mapping == MAP_FAILED) {
    return false;
  }

  // save results
  mapped_file_ = mapping;
  mapped_size_ = filesize.st_size;
  return true;
}


void AbstractSpooler::ProcessingWorker::UnmapFile() {
  assert (mapped_size_ > 0 && mapped_file_ != NULL);

  // unmap the previously mapped file
  if (munmap(mapped_file_, mapped_size_) != 0) {
    return;
  }

  // reset data
  mapped_file_ = NULL;
  mapped_size_ = 0;
}


void AbstractSpooler::Process(const std::string &local_path,
                              const std::string &remote_dir,
                              const bool         allow_chunking) {
}

void AbstractSpooler::EndOfTransaction() {

}


void AbstractSpooler::WaitForUpload() const {

}


void AbstractSpooler::WaitForTermination() const {

}


unsigned int AbstractSpooler::GetNumberOfErrors() const {

}

void AbstractSpooler::JobDone(const SpoolerResult &data) {
  NotifyListeners(data);
}


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
