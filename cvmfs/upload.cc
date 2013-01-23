/**
 * This file is part of the CernVM File System.
 */

#include "upload.h"

#include <vector>

#include "upload_local.h"
//#include "upload_riak.h"

#include "util_concurrency.h"
#include "file_chunk_generator.h"
#include "compression.h"

using namespace upload;

const std::string AbstractSpooler::kChunkSuffix = "P";

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
  concurrent_processing_context_ =
    new AbstractSpooler::ProcessingWorker::worker_context(
                                      spooler_definition_.spooler_description);

  const unsigned int number_of_cpus = GetNumberOfCpuCores();
  concurrent_processing_ =
     new ConcurrentWorkers<AbstractSpooler::ProcessingWorker>(number_of_cpus,
                                                              number_of_cpus * 500, // TODO: magic number (?)
                                                              concurrent_processing_context_);
  concurrent_processing_->RegisterListener(&AbstractSpooler::ProcessingCallback, this);

  assert(concurrent_processing_);

  // initialize the concurrent workers
  if (! concurrent_processing_->Initialize()) {
    LogCvmfs(kLogSpooler, kLogWarning, "Failed to initialize concurrent "
                                       "processing in AbstractSpooler.");
    return false;
  }

  return true;
}

void AbstractSpooler::TearDown() {

}


AbstractSpooler::ProcessingWorker::ProcessingWorker(const worker_context *context) :
  temporary_path_(context->temporary_path) {}


void AbstractSpooler::ProcessingWorker::operator()(const expected_data &data) {
  // get data references to the provided job structure for convenience
  const std::string &local_path     = data.local_path;
  const bool         allow_chunking = data.allow_chunking;

  returned_data result(local_path);

  // map the file to process into memory
  MemoryMappedFile mmf(local_path);
  if (!mmf.Map()) {
    result.return_code = 1;
    goto fail;
  }

  // chunk the file if needed
  if (allow_chunking && ! GenerateFileChunks(mmf, result)) {
    result.return_code = 2;
    goto fail;
  }

  // check if we only produced one chunk...
  if (result.file_chunks.size() == 1) {
    // ... and simply use that as bulk file as well
    result.bulk_file = result.file_chunks.front();

    // otherwise generate an additional bulk version of the file
  } else if (! GenerateBulkFile(mmf, result)) {
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


bool AbstractSpooler::ProcessingWorker::GenerateFileChunks(
                                            const MemoryMappedFile &mmf,
                                            returned_data          &data) const {
  assert (mmf.IsMapped());

  LogCvmfs(kLogSpooler, kLogVerboseMsg, "generating file chunks for %s",
             mmf.file_path().c_str());

  ChunkGenerator chunk_generator(mmf);

  while (chunk_generator.HasMoreData()) {
    // find the next file chunk boundary
    Chunk chunk_boundary = chunk_generator.Next();
    TemporaryFileChunk file_chunk;
    file_chunk.offset = chunk_boundary.offset();
    file_chunk.size   = chunk_boundary.size();

    // do what you need to do with the data
    if (!ProcessFileChunk(mmf, file_chunk)) {
      return false;
    }

    // all done... save the chunk information
    data.file_chunks.push_back(file_chunk);
  }

  return true;
}


bool AbstractSpooler::ProcessingWorker::GenerateBulkFile(
                                            const MemoryMappedFile &mmf,
                                            returned_data          &data) const {
  assert (mmf.IsMapped());

  LogCvmfs(kLogSpooler, kLogVerboseMsg, "generating bulk file for %s",
             mmf.file_path().c_str());

  // create a chunk that contains the whole file
  data.bulk_file.offset = 0;
  data.bulk_file.size   = mmf.size();

  // process the whole file in bulk
  return ProcessFileChunk(mmf, data.bulk_file);
}


bool AbstractSpooler::ProcessingWorker::ProcessFileChunk(
                                            const MemoryMappedFile &mmf,
                                            TemporaryFileChunk &chunk) const {
  // create a temporary to store the compression result of this chunk
  FILE *fcas = CreateTempFile(temporary_path_ + "/chunk", 0777, "w",
                              &chunk.temporary_path);
  if (fcas == NULL) {
    LogCvmfs(kLogSpooler, kLogStderr, "failed to create temporary file for "
                                      "chunk: %d %d of file %s",
             chunk.offset,
             chunk.size,
             mmf.file_path().c_str());
    return false;
  }

  // compress the chunk and compute the content hash simultaneously
  if (! zlib::CompressMem2File(mmf.buffer() + chunk.offset,
                              chunk.size,
                              fcas,
                              &chunk.content_hash)) {
    LogCvmfs(kLogSpooler, kLogStderr, "failed to compress chunk: %d %d of "
                                      "file %s",
             chunk.offset,
             chunk.size,
             mmf.file_path().c_str());
    return false;
  }

  LogCvmfs(kLogSpooler, kLogVerboseMsg, "compressed chunk: %d %d | hash: %s",
           chunk.offset,
           chunk.size,
           chunk.content_hash.ToString().c_str());

  return true;
}


void AbstractSpooler::Process(const std::string &local_path,
                              const std::string &remote_dir,
                              const bool         allow_chunking) {
  processing_parameters params(local_path,
                               allow_chunking);
  concurrent_processing_->Schedule(params);
}

void AbstractSpooler::ProcessingCallback(const ProcessingWorker::returned_data &data) {
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
