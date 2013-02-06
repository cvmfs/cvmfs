/**
 * This file is part of the CernVM File System.
 */

#include "upload.h"

#include <vector>

#include "upload_local.h"
//#include "upload_riak.h"

#include "upload_file_chunker.h"

using namespace upload;

SpoolerDefinition::SpoolerDefinition(
                      const std::string& definition_string,
                      const bool         use_file_chunking,
                      const size_t       min_file_chunk_size,
                      const size_t       avg_file_chunk_size,
                      const size_t       max_file_chunk_size) :
  use_file_chunking(use_file_chunking),
  min_file_chunk_size(min_file_chunk_size),
  avg_file_chunk_size(avg_file_chunk_size),
  max_file_chunk_size(max_file_chunk_size),
  valid_(false)
{
  // check if given file chunking values are sane
  if (use_file_chunking && (min_file_chunk_size >= avg_file_chunk_size ||
                            avg_file_chunk_size >= max_file_chunk_size)) {
    LogCvmfs(kLogSpooler, kLogStderr, "file chunk size values are not sane");
    return;
  }

  // split the spooler driver definition into name and config part
  std::vector<std::string> upstream = SplitString(definition_string, ':', 3);
  if (upstream.size() != 3) {
    LogCvmfs(kLogSpooler, kLogStderr, "Invalid spooler driver");
    return;
  }

  // recognize and configure the spooler driver
  if (upstream[0]        == "local") {
    driver_type = Local;
  } else if (upstream[0] == "riak") {
    driver_type = Riak;
  } else {
    LogCvmfs(kLogSpooler, kLogStderr, "unknown spooler driver: %s",
      upstream[0].c_str());
    return;
  }

  // save data
  temporary_path        = upstream[1];
  spooler_configuration = upstream[2];
  valid_ = true;
}


void AbstractSpooler::RegisterPlugins() {
  RegisterPlugin<LocalSpooler>();
  //RegisterPlugin<RiakSpooler>();
}


AbstractSpooler::AbstractSpooler(const SpoolerDefinition &spooler_definition) :
  spooler_definition_(spooler_definition)
{}


AbstractSpooler::~AbstractSpooler() {}


bool AbstractSpooler::Initialize() {
  // configure the file processor context
  concurrent_processing_context_ =
    new FileProcessor::worker_context(spooler_definition_.temporary_path,
                                      spooler_definition_.use_file_chunking);

  // create and configure a file processor worker environment
  const unsigned int number_of_cpus = GetNumberOfCpuCores();
  concurrent_processing_ =
     new ConcurrentWorkers<FileProcessor>(number_of_cpus,
                                          number_of_cpus * 500, // TODO: magic number (?)
                                          concurrent_processing_context_);
  assert(concurrent_processing_);
  concurrent_processing_->RegisterListener(&AbstractSpooler::ProcessingCallback, this);

  // initialize the file processor environment
  if (! concurrent_processing_->Initialize()) {
    LogCvmfs(kLogSpooler, kLogWarning, "Failed to initialize concurrent "
                                       "processing in AbstractSpooler.");
    return false;
  }

  // configure the file chunking size restrictions
  if (spooler_definition_.use_file_chunking) {
    ChunkGenerator::SetFileChunkRestrictions(
          spooler_definition_.min_file_chunk_size,
          spooler_definition_.avg_file_chunk_size,
          spooler_definition_.max_file_chunk_size);
  }

  // all done...
  return true;
}


void AbstractSpooler::TearDown() {
  concurrent_processing_->WaitForTermination();
}


void AbstractSpooler::Process(const std::string &local_path,
                              const bool         allow_chunking) {
  // fill the file processor parameter structure and schedule the job
  const FileProcessor::Parameters params(local_path, allow_chunking);
  concurrent_processing_->Schedule(params);
}


void AbstractSpooler::ProcessingCallback(const FileProcessor::Results &data) {
  Upload(data);
}


void AbstractSpooler::EndOfTransaction() {
  WaitForUpload();
}


void AbstractSpooler::WaitForUpload() const {
  concurrent_processing_->WaitForEmptyQueue();
}


void AbstractSpooler::WaitForTermination() const {
  concurrent_processing_->WaitForTermination();
}


unsigned int AbstractSpooler::GetNumberOfErrors() const {
  return concurrent_processing_->GetNumberOfFailedJobs();
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
