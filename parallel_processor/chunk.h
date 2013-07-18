#ifndef CHUNK_H
#define CHUNK_H

#include <sys/types.h>
#include <string>
#include <vector>
#include <cassert>

#include <iostream> // TODO: remove

#include <zlib.h>
#include <openssl/sha.h>

#include <tbb/atomic.h>

#include "util.h"
#include "buffer.h"

class IoDispatcher;

class Chunk {
 public:
  Chunk(const off_t offset) :
    file_offset_(offset), chunk_size_(0), is_bulk_chunk_(false),
    deferred_write_(false),
    zlib_initialized_(false),
    sha1_digest_(""), sha1_initialized_(false),
    file_descriptor_(0), bytes_written_(0)
  {
    Initialize();
  }

  bool IsInitialized()     const { return zlib_initialized_ && sha1_initialized_; }
  bool HasFileDescriptor() const { return file_descriptor_ > 0;                   }
  bool IsFullyProcessed()  const { return done_;                                  }
  bool IsBulkChunk()       const { return is_bulk_chunk_;                         }

  void Done();
  Chunk* CopyAsBulkChunk(const size_t file_size);

  void ScheduleWrite(CharBuffer *buffer);

  void EnableDeferredWrite() {
    assert (! HasFileDescriptor());
    deferred_write_ = true;
  }

  off_t          offset()                 const { return file_offset_;        }
  size_t         size()                   const { return chunk_size_;         }
  void       set_size(const size_t size)        { chunk_size_ = size;         }
  std::string    sha1_string()            const {
    assert (IsFullyProcessed());
    return ShaToString(sha1_digest_);
  }

  SHA_CTX&       sha1_context()                 { return sha1_context_;       }
  unsigned char* sha1_digest()                  { return sha1_digest_;        }

  z_stream&      zlib_context()                 { return zlib_context_;       }

  int            file_descriptor()        const { return file_descriptor_;    }
  void       set_file_descriptor(const int fd) {
    assert (! HasFileDescriptor());
    file_descriptor_ = fd;
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
  void Initialize() {
    done_            = false;
    compressed_size_ = 0;

    const int sha1_retval = SHA1_Init(&sha1_context_);
    assert (sha1_retval == 1);

    zlib_context_.zalloc   = Z_NULL;
    zlib_context_.zfree    = Z_NULL;
    zlib_context_.opaque   = Z_NULL;
    zlib_context_.next_in  = Z_NULL;
    zlib_context_.avail_in = 0;
    const int zlib_retval = deflateInit(&zlib_context_, Z_DEFAULT_COMPRESSION);
    assert (zlib_retval == 0);

    zlib_initialized_ = true;
    sha1_initialized_ = true;
  }

  void FlushDeferredWrites(const bool delete_buffers = true);

 private:
  Chunk(const Chunk &other);
  Chunk& operator=(const Chunk &other) { assert (false); }  // don't copy assign

 private:
  off_t                    file_offset_;
  size_t                   chunk_size_;
  tbb::atomic<bool>        done_;
  bool                     is_bulk_chunk_;

  bool                     deferred_write_;
  std::vector<CharBuffer*> deferred_buffers_;

  z_stream                 zlib_context_;
  bool                     zlib_initialized_;

  SHA_CTX                  sha1_context_;
  unsigned char            sha1_digest_[SHA_DIGEST_LENGTH];
  bool                     sha1_initialized_;

  int                      file_descriptor_;
  std::string              tmp_file_path_;
  size_t                   bytes_written_;
  tbb::atomic<size_t>      compressed_size_;
};

typedef std::vector<Chunk*> ChunkVector;

#endif /* CHUNK_H */
