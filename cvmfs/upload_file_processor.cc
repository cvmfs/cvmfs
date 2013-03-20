/**
 * This file is part of the CernVM File System.
 */

#include "upload_file_processor.h"

#include "upload_file_chunker.h"
#include "upload_facility.h"

#include "compression.h"

using namespace upload;


FileProcessor::FileProcessor(const SpoolerDefinition  &spooler_definition,
                             AbstractUploader         *uploader) :
  uploader_(uploader)
{
  worker_context_ = new FileProcessorWorker::worker_context(
                                      spooler_definition.temporary_path,
                                      spooler_definition.use_file_chunking,
                                      uploader);

  const unsigned int number_of_cpus = GetNumberOfCpuCores();
  workers_ = new ConcurrentWorkers<FileProcessor::FileProcessorWorker>(
                                      number_of_cpus,
                                      number_of_cpus * 5000,
                                      worker_context_.weak_ref());
  workers_->RegisterListener(&FileProcessor::ProcessingCallback, this);
}


bool FileProcessor::Initialize() {
  return workers_->Initialize();
}


void FileProcessor::ProcessingCallback(const FileProcessorWorker::Results  &data) {
  NotifyListeners(SpoolerResult(data.return_code,
                                data.local_path,
                                data.bulk_file.content_hash(),
                                data.file_chunks));
}


void FileProcessor::WaitForProcessing() const {
  uploader_->DisablePrecaching();
  workers_->WaitForEmptyQueue();
  uploader_->EnablePrecaching();
}


void FileProcessor::WaitForTermination() const {
  uploader_->DisablePrecaching();
  workers_->WaitForTermination();
  uploader_->EnablePrecaching();
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//



FileProcessor::FileProcessorWorker::FileProcessorWorker(
                                                const worker_context *context) :
  temporary_path_(context->temporary_path),
  use_file_chunking_(context->use_file_chunking),
  uploader_(context->uploader) {}


void FileProcessor::FileProcessorWorker::operator()(
                            const FileProcessorWorker::Parameters &parameters) {
  // asynchrously remove already finished processing jobs
  RemoveCompletedPendingFiles();

  // map the file to be processed into memory
  MemoryMappedFile mmf(parameters.local_path);
  if (!mmf.Map()) {
    master()->JobFailed(Results(parameters.local_path, 1));
    return;
  }

  // process the file (chunking, compression, checksumming, uploading)...
  PendingFile* file = CreatePendingFile(parameters.local_path);
  if (! ProcessFile(parameters, mmf, file)) {
    master()->JobFailed(Results(parameters.local_path, 2));
    return;
  }

  // all done... inform the pending file (mitigate race condition)
  FinalizeProcessing(file);

  // success will be reported once all uploads are finsihed
  return;
}


bool FileProcessor::FileProcessorWorker::ProcessFile(
                                const Parameters       &parameters,
                                const MemoryMappedFile &mmf,
                                      PendingFile      *file) const {
  assert (mmf.IsMapped());

  // do file chunking if it is enabled
  if (use_file_chunking_ && parameters.allow_chunking) {
    LogCvmfs(kLogSpooler, kLogVerboseMsg, "generating file chunks for %s",
               mmf.file_path().c_str());

    UniquePtr<ChunkGenerator> chunk_generator(ChunkGenerator::Construct(mmf));
    assert (chunk_generator);

    int generated_chunks = 0;
    while (chunk_generator->HasMoreData()) {
      // find the next file chunk boundary
      Chunk chunk_boundary = chunk_generator->Next();
      TemporaryFileChunk file_chunk(chunk_boundary.offset(),
                                    chunk_boundary.size());

      // do what you need to do with the data
      if (! ProcessFileChunk(mmf, file_chunk)) {
        return false;
      }

      ++generated_chunks;
      if (generated_chunks == 1 && ! chunk_generator->HasMoreData()) {
        // this will be the only generated chunk (i.e. the file is small)
        // we directly use it as bulk version (optimization)
        LockGuard<PendingFile> lock(file);
        file->AddBulk(file_chunk);
      } else {
        // we are dealing with a chunked file (i.e. the file is big)
        // upload the piece we just generated...
        {
          LockGuard<PendingFile> lock(file);
          file->AddChunk(file_chunk);
        }
        UploadChunk(file_chunk, file, FileChunk::kCasSuffix);
      }
    }
  }

  // if still necessary we generate a bulk version of the file (legacy)
  if (! file->HasBulkChunk()) {
    LogCvmfs(kLogSpooler, kLogVerboseMsg, "generating bulk file for %s",
             mmf.file_path().c_str());

    // create a chunk that contains the whole file
    TemporaryFileChunk bulk_file(0, mmf.size());
    if (! ProcessFileChunk(mmf, bulk_file)) {
      return false;
    }

    // add the bulk chunk to the pending file
    {
      LockGuard<PendingFile> lock(file);
      file->AddBulk(bulk_file);
    }
  }

  // in any case we need to upload the bulk version of the file
  UploadChunk(file->bulk_chunk(), file);
  return true;
}


void FileProcessor::FileProcessorWorker::UploadChunk(
                                const TemporaryFileChunk &file_chunk,
                                      PendingFile        *file,
                                const std::string        &cas_suffix) const {
  // schedule the chunk for upload
  // the pending file itself will get the callback when a chunk is uploaded
  uploader_->Upload(file_chunk.temporary_path(),
                    file_chunk.content_hash(),
                    cas_suffix,
                    AbstractUploader::MakeCallback(&PendingFile::UploadCallback,
                                                   file));
}


bool FileProcessor::FileProcessorWorker::ProcessFileChunk(
                                          const MemoryMappedFile       &mmf,
                                          TemporaryFileChunk     &chunk) const {
  // create a temporary to store the compression result of this chunk
  std::string temporary_path;
  FILE *fcas = CreateTempFile(temporary_path_ + "/chunk", 0666, "w",
                              &temporary_path);
  chunk.set_temporary_path(temporary_path);
  if (fcas == NULL) {
    LogCvmfs(kLogSpooler, kLogStderr, "failed to create temporary file for "
                                      "chunk: %d %d of file %s",
             chunk.offset(),
             chunk.size(),
             mmf.file_path().c_str());
    return false;
  }

  // compress the chunk and compute the content hash simultaneously
  hash::Any content_hash(hash::kSha1);
  if (! zlib::CompressMem2File(mmf.buffer() + chunk.offset(),
                               chunk.size(),
                               fcas,
                               &content_hash)) {
    LogCvmfs(kLogSpooler, kLogStderr, "failed to compress chunk: %d %d of "
                                      "file %s",
             chunk.offset(),
             chunk.size(),
             mmf.file_path().c_str());
    fclose(fcas);
    return false;
  }
  chunk.set_content_hash(content_hash);

  // close temporary file
  fclose(fcas);

  // all done... thanks
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "compressed chunk: %d %d | hash: %s",
           chunk.offset(),
           chunk.size(),
           chunk.content_hash().ToString().c_str());

  return true;
}


void FileProcessor::FileProcessorWorker::ProcessingCompleted(
                                                    PendingFile *pending_file) {
  const std::string &local_path = pending_file->local_path();
  {
    LockGuard<PendingFilesMap> lock(pending_files_);
    const int removed_items = pending_files_.erase(local_path);
    assert (removed_items == 1);
  }

  if (!pending_file->IsCompletedSuccessfully()) {
    master()->JobFailed(Results(local_path, 4));
    return;
  }

  Results final_result(local_path, 0);
  final_result.file_chunks = pending_file->GetFinalizedFileChunks();
  final_result.bulk_file   = pending_file->GetFinalizedBulkFile();
  {
    LockGuard<PendingFilesList> lock(completed_files_);
    completed_files_.push_back(pending_file);
  }

  master()->JobSuccessful(final_result);
}

FileProcessor::PendingFile*
FileProcessor::FileProcessorWorker::CreatePendingFile(
                                                const std::string &local_path) {
  PendingFile *file = new PendingFile(local_path, this);
  {
    LockGuard<PendingFilesMap> lock(pending_files_);
    pending_files_[local_path] = file;
  }

  return file;
}

void FileProcessor::FileProcessorWorker::FinalizeProcessing(
                                                    PendingFile *pending_file) {
  LockGuard<PendingFile> lock(pending_file);
  pending_file->FinalizeProcessing();
}

void FileProcessor::FileProcessorWorker::RemoveCompletedPendingFiles() {
  LockGuard<PendingFilesList> lock(completed_files_);
  if (completed_files_.empty()) {
    return;
  }

  PendingFilesList::iterator       i    = completed_files_.begin();
  PendingFilesList::const_iterator iend = completed_files_.end();
  PendingFile *file_to_delete;
  for (; i != iend; ++i) {
    file_to_delete = *i;

    // Prevent a race condition with ProcessingCompleted()
    // makes sure that this PendingFile is not deleted too early by locking it
    // before deleting it. The locked mutex will be destroyed by the delete
    file_to_delete->Lock();
    delete *i;
  }
  completed_files_.clear();
}


void FileProcessor::FileProcessorWorker::TearDown() {
  RemoveCompletedPendingFiles();
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


void FileProcessor::PendingFile::AddChunk(const TemporaryFileChunk &file_chunk) {
  assert (file_chunks_.find(file_chunk.temporary_path()) == file_chunks_.end());
  file_chunks_[file_chunk.temporary_path()] = file_chunk;
}


void FileProcessor::PendingFile::AddBulk(const TemporaryFileChunk &file_chunk) {
  assert (! has_bulk_chunk_);
  bulk_chunk_     = file_chunk;
  has_bulk_chunk_ = true;
}


void FileProcessor::PendingFile::FinalizeProcessing() {
  assert (has_bulk_chunk_);
  processing_complete_ = true;
  CheckForCompletionAndNotify();
}


void FileProcessor::PendingFile::UploadCallback(const UploaderResults &data) {
  LockGuard<PendingFile> lock(this);

  TemporaryFileChunk *chunk = NULL;
  if (bulk_chunk_.temporary_path() == data.local_path) {
    chunk = &bulk_chunk_;
  } else {
    TemporaryFileChunkMap::iterator chunk_itr =
                                          file_chunks_.find(data.local_path);
    assert (chunk_itr != file_chunks_.end());
    chunk = &chunk_itr->second;
  }

  assert (chunk != NULL);

  ++chunks_uploaded_;
  if (data.return_code == 0) {
    chunk->set_upload_state(TemporaryFileChunk::kUploadSuccessful);
  } else {
    chunk->set_upload_state(TemporaryFileChunk::kUploadFailed);
    ++errors_;
  }

  CheckForCompletionAndNotify();
}


void FileProcessor::PendingFile::CheckForCompletionAndNotify() {
  if (!uploading_complete_ &&
      processing_complete_ &&
      chunks_uploaded_ == file_chunks_.size() + 1) {
    uploading_complete_ = true;
    delegate_->ProcessingCompleted(this);
  }
}


FileChunks FileProcessor::PendingFile::GetFinalizedFileChunks() const {
  FileChunks final_chunks;
  TemporaryFileChunkMap::const_iterator i    = file_chunks_.begin();
  TemporaryFileChunkMap::const_iterator iend = file_chunks_.end();
  for (; i != iend; ++i) {
    final_chunks.push_back(static_cast<FileChunk>(i->second));
  }
  return final_chunks;
}


FileChunk FileProcessor::PendingFile::GetFinalizedBulkFile() const {
  assert (has_bulk_chunk_);
  return static_cast<FileChunk>(bulk_chunk_);
}
