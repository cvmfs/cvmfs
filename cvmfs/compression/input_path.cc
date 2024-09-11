/**
 * This file is part of the CernVM File System.
 */

#include "input_path.h"

#include <iostream>

namespace zlib {

InputPath::InputPath(const std::string& path, const size_t max_chunk_size) :
                                      InputAbstract(true, max_chunk_size, NULL),
                                      path_(path) {
  file_ = fopen(path.c_str(), "rb");
  input_file_ = new InputFile(file_, max_chunk_size, true);
}

}  // namespace zlib
