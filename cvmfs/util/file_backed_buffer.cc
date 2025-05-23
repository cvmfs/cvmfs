/**
 * This file is part of the CernVM File System.
 */

#include "file_backed_buffer.h"

#include <cassert>
#include <cstdio>
#include <cstring>

#include "util/exception.h"
#include "util/logging.h"
#include "util/posix.h"
#include "util/smalloc.h"

FileBackedBuffer *FileBackedBuffer::Create(uint64_t in_memory_threshold,
                                           const std::string &tmp_dir) {
  return new FileBackedBuffer(in_memory_threshold, tmp_dir);
}

FileBackedBuffer::FileBackedBuffer(uint64_t in_memory_threshold,
                                   const std::string &tmp_dir)
    : in_memory_threshold_(in_memory_threshold)
    , tmp_dir_(tmp_dir)
    , state_(kWriteState)
    , mode_(kMemoryMode)
    , size_(0)
    , buf_(NULL)
    , pos_(0)
    , fp_(NULL)
    , file_path_("")
    , mmapped_(NULL) { }

FileBackedBuffer::~FileBackedBuffer() {
  free(buf_);
  if (mode_ != kFileMode)
    return;

  if (state_ == kWriteState) {
    int retval = fclose(fp_);
    if (retval != 0)
      PANIC(kLogStderr, "could not close temporary file %s: error %d",
            file_path_.c_str(), retval);
  } else {
    mmapped_->Unmap();
    delete mmapped_;
  }
  int retval = unlink(file_path_.c_str());
  if (retval != 0)
    PANIC(kLogStderr, "could not delete temporary file %s: error %d",
          file_path_.c_str(), retval);
}

void FileBackedBuffer::Append(const void *source, uint64_t len) {
  assert(source != NULL);

  // Cannot write after Commit()
  assert(state_ == kWriteState);

  if (len == 0)
    return;

  // check the size and eventually save to file
  if (mode_ == kMemoryMode && pos_ + len > in_memory_threshold_) {
    // changes mode_ to kFileMode
    SaveToFile();
  }

  if (mode_ == kMemoryMode) {
    if (buf_ == NULL) {
      assert(size_ == 0);
      assert(pos_ == 0);
      buf_ = reinterpret_cast<unsigned char *>(smalloc(len));
      size_ = len;
    } else if (size_ < pos_ + len) {
      uint64_t newsize = (size_ * 2 < pos_ + len) ? (pos_ + len) : (size_ * 2);
      buf_ = reinterpret_cast<unsigned char *>(srealloc(buf_, newsize));
      size_ = newsize;
    }

    memcpy(buf_ + pos_, source, len);
    pos_ += len;
  } else {
    assert(fp_ != NULL);
    uint64_t bytes_written = fwrite(source, 1, len, fp_);
    if (bytes_written != len) {
      PANIC(kLogStderr,
            "could not append to temporary file %s: length %lu, "
            "actually written %lu, error %d",
            file_path_.c_str(), len, bytes_written, ferror(fp_));
    }
    pos_ += len;
    size_ += len;
  }
}

void FileBackedBuffer::Commit() {
  assert(state_ == kWriteState);

  if (mode_ == kFileMode) {
    int retval = fclose(fp_);
    if (retval != 0)
      PANIC(kLogStderr, "could not close file after writing finished: %s",
            file_path_.c_str());
    fp_ = NULL;

    mmapped_ = new MemoryMappedFile(file_path_);
    if (!mmapped_->Map())
      PANIC(kLogStderr, "could not memory-map file %s", file_path_.c_str());
  } else {
    // Trim memory chunk size to actual data size
    buf_ = reinterpret_cast<unsigned char *>(srealloc(buf_, pos_));
    size_ = pos_;
  }

  pos_ = 0;
  state_ = kReadState;
}

int64_t FileBackedBuffer::Data(void **ptr, int64_t len, uint64_t pos) {
  assert(state_ == kReadState);

  int64_t actual_len = (pos + len <= size_) ? len
                                            : static_cast<int64_t>(size_)
                                                  - static_cast<int64_t>(pos);
  assert(actual_len >= 0);

  if (mode_ == kMemoryMode) {
    *ptr = buf_ + pos;
  } else {
    *ptr = mmapped_->buffer() + pos;
  }
  return actual_len;
}

int64_t FileBackedBuffer::Read(void *ptr, int64_t len) {
  int64_t bytes_read = ReadP(ptr, len, pos_);
  pos_ += bytes_read;
  return bytes_read;
}

int64_t FileBackedBuffer::ReadP(void *ptr, int64_t len, uint64_t pos) {
  void *source;
  int64_t bytes_read = Data(&source, len, pos);
  memcpy(ptr, source, bytes_read);
  return bytes_read;
}

void FileBackedBuffer::Rewind() {
  assert(state_ == kReadState);
  pos_ = 0;
}

uint64_t FileBackedBuffer::GetSize() const {
  if (state_ == kWriteState && mode_ == kMemoryMode)
    return pos_;
  return size_;
}


void FileBackedBuffer::SaveToFile() {
  assert(state_ == kWriteState);
  assert(mode_ == kMemoryMode);

  assert(fp_ == NULL);
  fp_ = CreateTempFile(tmp_dir_, 0644, "w", &file_path_);
  if (fp_ == NULL)
    PANIC(kLogStderr, "could not create temporary file");

  uint64_t bytes_written = fwrite(buf_, 1, pos_, fp_);
  if (bytes_written != pos_) {
    PANIC(kLogStderr,
          "could not write to temporary file %s: length %lu, "
          "actually written %lu, error %d",
          file_path_.c_str(), pos_, bytes_written, ferror(fp_));
  }

  free(buf_);
  buf_ = NULL;
  size_ = pos_;
  mode_ = kFileMode;
}
