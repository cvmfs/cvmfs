/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FILE_PROCESSING_CHUNK_H_
#define CVMFS_FILE_PROCESSING_CHUNK_H_

#include <sys/types.h>
#include <tbb/atomic.h>

#include <cassert>
#include <string>
#include <vector>

#include "../compression.h"
#include "../duplex_zlib.h"
#include "../hash.h"
#include "../util.h"
#include "char_buffer.h"

namespace upload {

struct UploadStreamHandle;
class IoDispatcher;
class File;

/**
 * The Chunk class describes a file chunk in processing. It holds state infor-
 * mation for compression and hashing as well as other chunk specific meta data.
 * Note that it deliberately _does not_ contain the actual chunk data which is
 * managed dynamically by a stream of Buffers. Still the Chunk class is in
 * charge of scheduling (or deferred scheduling) the write of processed Buffers.
 * Note: When creating a chunk it is seen as a 'partial' data chunk and can be
 *       promoted to a 'bulk' chunk (representing an entire file).
 */
class Chunk {
 public:
  Chunk(File* file,
        const off_t offset,
        shash::Algorithms hash_algorithm,
        zlib::Algorithms compression_alg)
        : file_(file)
        , file_offset_(offset)
        , chunk_size_(0)
        , is_bulk_chunk_(false)
        , is_fully_defined_(false)
        , deferred_write_(false)
        , zlib_initialized_(false)
        , compression_algorithm_(compression_alg)
        , content_hash_context_(hash_algorithm)
        , content_hash_(hash_algorithm, shash::kSuffixPartial)
        , content_hash_initialized_(false)
        , upload_stream_handle_(NULL)
        , current_deflate_buffer_(NULL)
        , bytes_written_(0)
  {
    Initialize();
  }

  bool IsInitialized()         const { return zlib_initialized_ &&
                                              content_hash_initialized_;     }
  bool IsFullyProcessed()      const { return done_;                         }
  bool IsBulkChunk()           const { return is_bulk_chunk_;                }
  bool IsFullyDefined()        const { return is_fully_defined_;             }
  bool HasUploadStreamHandle() const { return upload_stream_handle_ != NULL; }

  void Finalize();
  void ScheduleCommit();
  Chunk* CopyAsBulkChunk(const size_t file_size);
  void SetAsBulkChunk();

  /**
   * Provides the ChunkProcessingTask with memory for the data compression. The
   * user (i.e. ChunkProcessingTask::Crunch()) is responsible to set the
   * used_bytes_ field of the provided CharBuffer after data has been added.
   * This method returns always the same buffer until it is full. In that case
   * the filled CharBuffer is automatically scheduled for writing and a fresh
   * CharBuffer is provided.
   *
   * @param bytes  an estimate of how much memory needs to be provided.
   * @return       a CharBuffer to write compression results
   *               (Note: The CharBuffer might already contain data!)
   */
  CharBuffer* GetDeflateBuffer(const size_t bytes);

  /**
   * An enabled deferred write mode instructs the Chunk object to store Buffers
   * instead of directly sending them to the IoDispatcher for write out.
   * This is used for Files that might become chunked to produce a bulk chunk
   * together with the actual file chunks.
   * Deferred writes of Buffers are issued as soon as the final decision for
   * file chunking is made. (See protected member FlushDeferredWrites())
   */
  void EnableDeferredWrite() {
    assert(!HasUploadStreamHandle());
    deferred_write_ = true;
  }

  File* file() const { return file_; }
  off_t offset() const { return file_offset_; }
  size_t size() const { return chunk_size_; }
  void set_size(const size_t size) {
    chunk_size_       = size;
    is_fully_defined_ = true;
  }

  shash::ContextPtr& content_hash_context() { return content_hash_context_; }
  const shash::Any&  content_hash() const { return content_hash_; }
  zlib::Compressor*   compressor() { return compressor_.weak_ref(); }

  UploadStreamHandle* upload_stream_handle() const {
    return upload_stream_handle_;
  }
  void set_upload_stream_handle(UploadStreamHandle* ush) {
    assert(!HasUploadStreamHandle());
    upload_stream_handle_ = ush;
  }

  size_t bytes_written() const { return bytes_written_; }
  size_t compressed_size() const { return compressed_size_; }
  void add_bytes_written(const size_t new_bytes) {
    bytes_written_ += new_bytes;
  }

 protected:
  void Initialize();
  void FlushDeferredWrites(const bool delete_buffers = true);
  void ScheduleWrite(CharBuffer *buffer);

 private:
  Chunk(const Chunk &other);
  Chunk& operator=(const Chunk &other);  // don't copy assign

 private:
  File *file_;  ///< This is a chunk of File
  off_t file_offset_;  ///< Offset in the associated File
  /**
   * Size of the chunk
   * Note: might not be defined from from the beginning
   */
  size_t chunk_size_;

  tbb::atomic<bool>        done_;
  bool                     is_bulk_chunk_;
  bool                     is_fully_defined_;

  bool                     deferred_write_;
  /**
   * Buffers stored for a deferred write (see EnableDeferredWrite())
   */
  std::vector<CharBuffer*> deferred_buffers_;

  bool                     zlib_initialized_;
  zlib::Algorithms         compression_algorithm_;

  shash::ContextPtr        content_hash_context_;
  shash::Any               content_hash_;
  bool                     content_hash_initialized_;

  /**
   * Opaque handle for streamed upload
   */
  UploadStreamHandle      *upload_stream_handle_;
  /**
   * Current deflate destination buffer
   */
  CharBuffer              *current_deflate_buffer_;
  /**
   * Bytes already uploaded (compressed).
   */
  size_t                   bytes_written_;
  tbb::atomic<size_t>      compressed_size_;  ///< size of the compressed data

  /**
   * Compressor
   */
  UniquePtr<zlib::Compressor>  compressor_;
};

typedef std::vector<Chunk*> ChunkVector;

}  // namespace upload

#endif  // CVMFS_FILE_PROCESSING_CHUNK_H_
