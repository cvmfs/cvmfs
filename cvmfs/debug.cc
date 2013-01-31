#include <iostream>
#include <algorithm>

#include "util.h"

class LiveFileChunk : public FileChunk {
 public:
  explicit LiveFileChunk(const FileChunk &chunk) :
    FileChunk(chunk),
    open_(false),
    file_descriptor_(0) {}

  bool Fetch();
  inline int file_descriptor() const { return file_descriptor_; }

  inline bool IsOpen() const { return open_; }

  inline bool operator<(const off_t offset) const {
    return this->offset + this->size <= offset;
  }

 private:
  bool open_;
  int  file_descriptor_;
};
typedef std::vector<LiveFileChunk> LiveFileChunks;

void find_chunk(const LiveFileChunks &chunks, const off_t offset) {
  LiveFileChunks::const_iterator chunk_itr = chunks.begin();
  chunk_itr = std::lower_bound(chunk_itr, chunks.end(), offset);

  if (chunk_itr == chunks.end()) {
    std::cout << "needle: " << offset << " not found" << std::endl;
    return;
  }

  std::cout << "needle: " << offset << " found: (" << chunk_itr->offset << " | " << chunk_itr->size << ")" << std::endl;
}

int main() {
  LiveFileChunks chunks;
  chunks.push_back(LiveFileChunk(FileChunk(hash::Any(),    0, 1000)));
  chunks.push_back(LiveFileChunk(FileChunk(hash::Any(), 1000, 1000)));
  chunks.push_back(LiveFileChunk(FileChunk(hash::Any(), 2000, 1000)));
  chunks.push_back(LiveFileChunk(FileChunk(hash::Any(), 3000, 1000)));

  find_chunk(chunks, 1000);
  find_chunk(chunks, 999);
  find_chunk(chunks, 0);
  find_chunk(chunks, 1999);
  find_chunk(chunks, 3999);
  find_chunk(chunks, 4000);

  return 0;
}

