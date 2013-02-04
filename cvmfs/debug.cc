#include <iostream>
#include <algorithm>

#include "util.h"
#include "upload_file_chunker.h"

using namespace upload;

int main() {
  MemoryMappedFile file("Makefile");
  file.Map();
  assert (file.IsMapped());

  UniquePtr<ChunkGenerator> chunk_generator(ChunkGenerator::Construct(file));
  assert (chunk_generator);

  uint64_t sum = 0;
  uint64_t num = 0;

  while (chunk_generator->HasMoreData()) {
    // find the next file chunk boundary
    Chunk chunk = chunk_generator->Next();
    sum += chunk.size();
    num++;

    std::cout << chunk.offset() << " | " << chunk.size() << std::endl;
  }

  std::cout << "average size: " << (sum / num) << std::endl;

  return 0;
}

