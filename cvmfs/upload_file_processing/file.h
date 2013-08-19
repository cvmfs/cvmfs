/**
 * This file is part of the CernVM File System.
 */

#ifndef UPLOAD_FILE_PROCESSING_FILE_H
#define UPLOAD_FILE_PROCESSING_FILE_H

#include <string>
#include <vector>

#include <tbb/atomic.h>

#include "../platform.h"

#include "char_buffer.h"
#include "chunk_detector.h"

namespace upload {

class IoDispatcher;
class ChunkDetector;
class Chunk;
typedef std::vector<Chunk*> ChunkVector;

class File {
 public:
  File(const std::string  &path,
       IoDispatcher       *io_dispatcher,
       ChunkDetector      *chunk_detector,
       const bool          allow_chunking = true,
       const std::string  &hash_suffix    = "");
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
  void ChunkCommitted(Chunk *chunk);
  void FullyDefineLastChunk();

  off_t FindNextCutMark(CharBuffer *buffer) {
    assert (chunk_detector_ != NULL);
    assert (might_become_chunked_);
    return chunk_detector_->FindNextCutMark(buffer);
  }

  bool HasBulkChunk()          const { return bulk_chunk_ != NULL; }

  size_t size()                const { return size_;               }
  const std::string& path()    const { return path_;               }

        Chunk* bulk_chunk()          { return bulk_chunk_;         }
  const Chunk* bulk_chunk()    const { return bulk_chunk_;         }
  const ChunkVector& chunks()  const { return chunks_;             }

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
  const std::string           path_;
  const size_t                size_;
  const bool                  might_become_chunked_;
  const std::string           hash_suffix_;

  ChunkVector                 chunks_;
  Chunk                      *bulk_chunk_;
  tbb::atomic<unsigned int>   chunks_to_commit_;

  IoDispatcher               *io_dispatcher_;
  ChunkDetector              *chunk_detector_;
};

} // namespace upload

#endif /* UPLOAD_FILE_PROCESSING_FILE_H */
