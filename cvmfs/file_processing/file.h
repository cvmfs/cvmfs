/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FILE_PROCESSING_FILE_H_
#define CVMFS_FILE_PROCESSING_FILE_H_

#include <tbb/atomic.h>

#include <string>
#include <vector>

#include "../compression.h"
#include "../hash.h"
#include "../platform.h"
#include "char_buffer.h"
#include "chunk_detector.h"
#include "file_scrubbing_task.h"

namespace upload {

class IoDispatcher;
class ChunkDetector;
class Chunk;
typedef std::vector<Chunk*> ChunkVector;

/**
 * File to be processed by the processing pipe line. A File object is the only
 * input object for the processing. It is in charge of the Chunk handling and
 * holds file related meta data.
 */
class File : public AbstractFile {
 public:
  File(const std::string    &path,
       IoDispatcher         *io_dispatcher,
       ChunkDetector        *chunk_detector,
       shash::Algorithms     hash_algorithm,
       zlib::Algorithms      compression_alg,
       const shash::Suffix   hash_suffix = shash::kSuffixNone);
  ~File();

  bool MightBecomeChunked() const { return might_become_chunked_; }

  /**
   * This creates a next chunk which will be the successor of the current chunk
   *
   * @param   offset  the offset at which the new chunk should be created
   *                  Note: this implictly defines the size of the current chunk
   * @return  the predecessor (!) of the just created chunk
   */
  Chunk* CreateNextChunk(const off_t offset);

  /**
   * Notifies that a given Chunk has been fully processed and committed.
   * @param chunk   The finalized chunk
   */
  void ChunkCommitted(Chunk *chunk);

  /**
   * After all Chunks have been produced, this makes sure that the last generated
   * Chunk will contain all remaining data of File.
   */
  void FullyDefineLastChunk();

  off_t FindNextCutMark(CharBuffer *buffer) {
    assert(chunk_detector_ != NULL);
    assert(might_become_chunked_);
    return chunk_detector_->FindNextCutMark(buffer);
  }

  bool HasBulkChunk()              const { return bulk_chunk_ != NULL;     }
  bool HasChunkDetector()          const { return chunk_detector_ != NULL; }

        Chunk*        bulk_chunk()        { return bulk_chunk_;  }
  const Chunk*        bulk_chunk()  const { return bulk_chunk_;  }
  const ChunkVector&  chunks()      const { return chunks_;      }
        shash::Suffix hash_suffix() const { return hash_suffix_; }

  Chunk* current_chunk() {
    return (chunks_.size() > 0) ? chunks_.back() : NULL;
  }
  const Chunk* current_chunk() const {
    return (chunks_.size() > 0) ? chunks_.back() : NULL;
  }

  IoDispatcher* io_dispatcher() { return io_dispatcher_; }

 protected:
  void AddChunk(Chunk *chunk, const bool register_chunk = true);
  void CreateInitialChunk();
  void ForkOffBulkChunk();
  void PromoteSingleChunkAsBulkChunk();
  void Finalize();

 private:
  const bool might_become_chunked_;  ///< Result of the chunkedness forecast
  /**
   * Secure hash algorithm creating content-addressable storage
   */
  const shash::Algorithms hash_algorithm_;
  /**
   * Suffix to be appended to the bulk chunk content hash
   */
  const shash::Suffix hash_suffix_;
  /**
   * Compression algorithm for the chunks
   */
  const zlib::Algorithms compression_alg_;

  ChunkVector chunks_;  ///< List of generated Chunks
  Chunk *bulk_chunk_;  ///< Associated bulk Chunk
  /**
   * Counter of Chunks in flight (synchronization)
   */
  tbb::atomic<unsigned int> chunks_to_commit_;

  IoDispatcher *io_dispatcher_;
  /**
   * The ChunkDetector to be used for this file.
   */
  ChunkDetector *chunk_detector_;
};

}  // namespace upload

#endif  // CVMFS_FILE_PROCESSING_FILE_H_
