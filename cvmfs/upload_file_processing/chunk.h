/**
 * This file is part of the CernVM File System.
 */

#ifndef UPLOAD_FILE_PROCESSING_CHUNK_H
#define UPLOAD_FILE_PROCESSING_CHUNK_H

#include <sys/types.h>
#include <string>
#include <vector>
#include <cassert>

#include <iostream> // TODO: remove

#include <zlib.h>
#include <openssl/sha.h>

#include <tbb/atomic.h>

#include "char_buffer.h"
#include "../hash.h"

namespace upload {

struct UploadStreamHandle;

class IoDispatcher;
class File;

class Chunk {
 public:
  Chunk(File* file, const off_t offset) :
    file_(file), file_offset_(offset), chunk_size_(0),
    is_bulk_chunk_(false), is_fully_defined_(false), deferred_write_(false),
    zlib_initialized_(false), sha1_initialized_(false),
    upload_stream_handle_(NULL), bytes_written_(0)
  {
    Initialize();
  }

  bool IsInitialized()         const { return zlib_initialized_ &&
                                              sha1_initialized_;             }
  bool IsFullyProcessed()      const { return done_;                         }
  bool IsBulkChunk()           const { return is_bulk_chunk_;                }
  bool IsFullyDefined()        const { return is_fully_defined_;             }
  bool HasUploadStreamHandle() const { return upload_stream_handle_ != NULL; }

  void Finalize();
  void ScheduleCommit();
  Chunk* CopyAsBulkChunk(const size_t file_size);
  void SetAsBulkChunk() { is_bulk_chunk_ = true; }

  void ScheduleWrite(CharBuffer *buffer);

  void EnableDeferredWrite() {
    assert (! HasUploadStreamHandle());
    deferred_write_ = true;
  }

  File*          file()                   const { return file_;               }
  off_t          offset()                 const { return file_offset_;        }
  size_t         size()                   const { return chunk_size_;         }
  void       set_size(const size_t size) {
    chunk_size_       = size;
    is_fully_defined_ = true;
  }

  SHA_CTX&       sha1_context()                 { return sha1_context_;       }
  hash::Any      sha1() const                   { return hash::Any(
                                                          hash::kSha1,
                                                          sha1_digest_,
                                                          SHA_DIGEST_LENGTH); }

  z_stream&      zlib_context()                 { return zlib_context_;       }

  UploadStreamHandle* upload_stream_handle() const { return upload_stream_handle_; }
  void set_upload_stream_handle(UploadStreamHandle* ush) {
    assert (! HasUploadStreamHandle());
    upload_stream_handle_ = ush;
  }

  const std::string& temporary_path()     const { return tmp_file_path_; }
  void           set_temporary_path(const std::string path) {
    tmp_file_path_ = path;
  }

  size_t         bytes_written()          const { return bytes_written_;      }
  size_t         compressed_size()        const { return compressed_size_;    }
  void       add_bytes_written(const size_t new_bytes) {
    bytes_written_ += new_bytes;
  }
  void       add_compressed_size(const size_t new_bytes) {
    compressed_size_ += new_bytes;
  }

 protected:
  void Initialize();
  void FlushDeferredWrites(const bool delete_buffers = true);

 private:
  Chunk(const Chunk &other);
  Chunk& operator=(const Chunk &other);  // don't copy assign

 private:
  File                    *file_;
  off_t                    file_offset_;
  size_t                   chunk_size_;
  tbb::atomic<bool>        done_;
  bool                     is_bulk_chunk_;
  bool                     is_fully_defined_;

  bool                     deferred_write_;
  std::vector<CharBuffer*> deferred_buffers_;

  z_stream                 zlib_context_;
  bool                     zlib_initialized_;

  SHA_CTX                  sha1_context_;
  unsigned char            sha1_digest_[SHA_DIGEST_LENGTH];
  bool                     sha1_initialized_;

  UploadStreamHandle      *upload_stream_handle_;
  std::string              tmp_file_path_;
  size_t                   bytes_written_;
  tbb::atomic<size_t>      compressed_size_;
};

typedef std::vector<Chunk*> ChunkVector;

}

#endif /* UPLOAD_FILE_PROCESSING_CHUNK_H */
