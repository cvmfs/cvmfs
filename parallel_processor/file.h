#ifndef FILE_H
#define FILE_H

#include <string>
#include <vector>

#include <tbb/atomic.h>

#include "../cvmfs/platform.h"

#include "buffer.h"
#include "chunk_detector.h"

class IoDispatcher;
class ChunkDetector;
class Chunk;
typedef std::vector<Chunk*> ChunkVector;



class File {
 public:
  File(const std::string      &path,
       const platform_stat64  &info,
       IoDispatcher           *io_dispatcher) :
    path_(path), size_(info.st_size),
    might_become_chunked_(ChunkDetector::MightBecomeChunked(size_)),
    bulk_chunk_(NULL),
    io_dispatcher_(io_dispatcher),
    chunk_detector_(NULL)
  {
    chunks_to_commit_ = 0;
    CreateInitialChunk();
  }

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

  bool HasChunkDetector() const { return chunk_detector_ != NULL; }
  void AddChunkDetector(ChunkDetector *detector) {
    assert (chunk_detector_ == NULL);
    assert (might_become_chunked_);
    chunk_detector_ = detector;
  }

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

  ChunkVector                 chunks_;
  Chunk                      *bulk_chunk_;
  tbb::atomic<unsigned int>   chunks_to_commit_;

  IoDispatcher               *io_dispatcher_;
  ChunkDetector              *chunk_detector_;
};

#endif /* FILE_H */
