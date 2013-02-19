#include "upload_file_processor.h"

#include "upload_file_chunker.h"
#include "upload_facility.h"

#include "compression.h"

using namespace upload;

FileProcessor::FileProcessor(const worker_context *context) :
  temporary_path_(context->temporary_path),
  use_file_chunking_(context->use_file_chunking),
  uploader_(context->uploader) {}


void FileProcessor::operator()(const FileProcessor::Parameters &data) {
  // get data references to the provided job structure for convenience
  const std::string &local_path     = data.local_path;
  const bool         allow_chunking = data.allow_chunking;

  // map the file to process into memory
  MemoryMappedFile mmf(local_path);
  if (!mmf.Map()) {
    master()->JobFailed(Results(local_path, 1));
    return;
  }

  // schedule a pending job
  PendingFile *file = new PendingFile(local_path,
          PendingFile::MakeCallback(&FileProcessor::ProcessingCompleted, this));
  {
    LockGuard<PendingFiles> lock(pending_files_);
    pending_files_[local_path] = file;
  }

  // chunk the file if requested and if enabled
  int generated_chunks = 0;
  if (allow_chunking && use_file_chunking_) {
    generated_chunks = GenerateFileChunks(mmf, file);
    if (generated_chunks == -1) {
      master()->JobFailed(Results(local_path, 2));
      return;
    }
  }

  // check if we only produced one chunk...
  if (generated_chunks == 1) {
    // ... then simply use that as bulk file and discard the chunk
    LockGuard<PendingFile> lock(file);
    file->PromoteSingleChunkToBulk();

    // otherwise generate an additional bulk version of the file
  } else if (! GenerateBulkFile(mmf, file)) {
    master()->JobFailed(Results(local_path, 3));
    return;
  }

  // all done... inform the pending file (mitigate race condition)
  {
    LockGuard<PendingFile> lock(file);
    file->FinalizeProcessing();
  }

  // success will be reported once all uploads are finsihed
  return;
}


int FileProcessor::GenerateFileChunks(const MemoryMappedFile  &mmf,
                                            PendingFile       *file) const {
  assert (mmf.IsMapped());

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
      return -1;
    }

    // add the chunk to the pending file in progress
    {
      LockGuard<PendingFile> lock(file);
      file->AddChunk(file_chunk);
    }

    // hand the chunk over to the upload facility for upload
    UploadChunk(file_chunk, file);
    ++generated_chunks;
  }

  return generated_chunks;
}


bool FileProcessor::GenerateBulkFile(const MemoryMappedFile &mmf,
                                           PendingFile      *file) const {
  assert (mmf.IsMapped());

  LogCvmfs(kLogSpooler, kLogVerboseMsg, "generating bulk file for %s",
             mmf.file_path().c_str());

  // create a chunk that contains the whole file
   TemporaryFileChunk bulk_file(0, mmf.size());

  // process the whole file in bulk
  if (!ProcessFileChunk(mmf, bulk_file)) {
    return false;
  }

  // add the chunk to the pending file in progress
  {
    LockGuard<PendingFile> lock(file);
    file->AddBulk(bulk_file);
  }

  // hand the chunk over to the upload facility for upload
  UploadChunk(bulk_file, file);
  return true;
}


void FileProcessor::UploadChunk(const TemporaryFileChunk &file_chunk,
                                      PendingFile        *file) const {
  // schedule the chunk for upload
  // the pending file itself will get the callback when a chunk is uploaded
  uploader_->Upload(file_chunk.temporary_path(),
                    file_chunk.content_hash(),
                    "",
                    AbstractUploader::MakeCallback(&PendingFile::UploadCallback,
                                                   file));
}


bool FileProcessor::ProcessFileChunk(const MemoryMappedFile       &mmf,
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

void FileProcessor::ProcessingCompleted(const std::string &local_path) {
  PendingFile* file = NULL;
  {
    LockGuard<PendingFiles> lock(pending_files_);
    PendingFiles::iterator file_itr = pending_files_.find(local_path);
    assert (file_itr != pending_files_.end());
    file = file_itr->second;
    pending_files_.erase(local_path);
  }

  if (!file->IsCompletedSuccessfully()) {
    master()->JobFailed(Results(local_path, 4));
    return;
  }

  Results final_result(local_path, 0);
  final_result.file_chunks = file->GetFinalizedFileChunks();
  final_result.bulk_file   = file->GetFinalizedBulkFile();

  master()->JobSuccessful(final_result);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


PendingFile::~PendingFile() {
  delete finished_callback_;
}


void PendingFile::AddChunk(const TemporaryFileChunk  &file_chunk) {
  assert (file_chunks_.find(file_chunk.temporary_path()) == file_chunks_.end());
  file_chunks_[file_chunk.temporary_path()] = file_chunk;
}


void PendingFile::AddBulk(const TemporaryFileChunk  &file_chunk) {
  bulk_chunk_ = file_chunk;
}


void PendingFile::FinalizeProcessing() {
  processing_complete_ = true;
  CheckForCompletionAndNotify();
}


void PendingFile::UploadCallback(const UploaderResults &data) {
  LockGuard<PendingFile> lock(this);

  TemporaryFileChunk *chunk = NULL;
  if (bulk_chunk_.temporary_path() == data.local_path) {
    chunk = &bulk_chunk_;
  } else {
    TemporaryFileChunkMap::iterator chunk_itr = file_chunks_.find(data.local_path);
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


void PendingFile::CheckForCompletionAndNotify() {
  if (!uploading_complete_ &&
      processing_complete_ &&
      chunks_uploaded_ == file_chunks_.size() + 1) {
    uploading_complete_ = true;
    (*finished_callback_)(local_path_);
  }
}


FileChunks PendingFile::GetFinalizedFileChunks() const {
  FileChunks final_chunks;
  TemporaryFileChunkMap::const_iterator i    = file_chunks_.begin();
  TemporaryFileChunkMap::const_iterator iend = file_chunks_.end();
  for (; i != iend; ++i) {
    final_chunks.push_back(static_cast<FileChunk>(i->second));
  }
  return final_chunks;
}


FileChunk PendingFile::GetFinalizedBulkFile() const {
  return static_cast<FileChunk>(bulk_chunk_);
}


void PendingFile::PromoteSingleChunkToBulk() {
  assert (file_chunks_.size() == 1);
  bulk_chunk_     = file_chunks_.begin()->second;
  file_chunks_.clear();
}
