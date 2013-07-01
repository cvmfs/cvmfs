
#include <iostream>
#include <cmath>
#include <cerrno>
#include <cassert>
#include <vector>

#include <tbb/parallel_for.h>
#include <tbb/blocked_range.h>
#include <tbb/tick_count.h>
#include <tbb/scalable_allocator.h>

#include <zlib.h>

#include <openssl/sha.h>

#include "cvmfs/fs_traversal.h"

using namespace tbb;

static const std::string output_path = "/Volumes/ramdisk";

template<typename T, class A = std::allocator<T> >
class Buffer {
 public:
  Buffer() : size_(0), buffer_(nullptr) {}

  Buffer(const size_t size) : size_(0) {
    Allocate(size);
  }

  Buffer(Buffer &&other) {
    *this = std::move(other);
  }

  Buffer& operator=(Buffer &&other) {
    size_         = other.size();
    buffer_       = (size_ > 0) ? other.buffer_
                                : nullptr;
    other.size_   = 0;
    other.buffer_ = nullptr;
    return *this;
  }

  ~Buffer() {
    Deallocate();
  }

  void Allocate(const size_t size) {
    assert (!Initialized());
    size_ = size;
    buffer_ = A().allocate(size_);
  }

  bool Initialized() const { return size_ > 0; }

  typename A::pointer ptr() {
    assert (Initialized());
    return buffer_;
  }

  const typename A::pointer ptr() const {
    assert (Initialized());
    return buffer_;
  }

  operator T*() {
    return ptr();
  }

  const size_t size() const { return size_; }

 private:
  Buffer(const Buffer &other) { assert (false); } // no copy!
  Buffer& operator=(const Buffer& other) { assert (false); }

  void Deallocate() {
    if (size_ == 0) {
      return;
    }
    A().deallocate(buffer_, size_);
  }

 private:
  size_t               size_;
  typename A::pointer  buffer_;
};
typedef Buffer<unsigned char, scalable_allocator<unsigned char> > CharBuffer;


class File {
 public:
  explicit File(const std::string &path) : path_(path) {}
  File(File &&other) noexcept :
    uncompressed_buffer_(std::move(other.uncompressed_buffer())),
    compressed_buffer_(std::move(other.compressed_buffer()))
  {
    path_                = std::move(other.path());
    sha1_context_        = other.sha1_context();
    sha1_                = std::move(other.sha1());
  }

  const std::string& path() const { return path_; }

  CharBuffer&  uncompressed_buffer() { return uncompressed_buffer_; }
  CharBuffer&  compressed_buffer()   { return compressed_buffer_;   }
  SHA_CTX&     sha1_context()        { return sha1_context_;        }
  std::string& sha1()                { return sha1_;                }

 public:
  bool Read() {
    // open file
    FILE *f = fopen(path_.c_str(), "r");
    if (f == NULL) {
      std::cout << "cannot open file: " << path_
                << " errno: " << errno << std::endl;
      return false;
    }

    // get file size
    fseek(f, 0, SEEK_END);
    const size_t size = ftell(f);
    rewind(f);

    // allocate buffer
    uncompressed_buffer_.Allocate(size);

    // read the file chunk wise
    do {
      const size_t read_bytes = fread(uncompressed_buffer_, 1, size, f);
    } while(!feof(f) && !ferror(f));

    // close the file
    fclose(f);

    return true;
  }

  bool Write() {
    assert (sha1_.size() > 0);
    assert (compressed_buffer_.Initialized());

    const std::string path = output_path + "/" + sha1_;

    // open file
    FILE *f = fopen(path.c_str(), "w+");
    if (f == NULL) {
      std::cout << "cannot open file: " << path
                << " errno: " << errno << std::endl;
      return false;
    }

    // write buffer
    const size_t written = fwrite(compressed_buffer_, 1,
                                  compressed_buffer_.size(), f);
    if (written != compressed_buffer_.size()) {
      std::cout << "failed to write " << compressed_buffer_.size() << " bytes "
                << "to file: " << path << std::endl;
      return false;
    }

    // close the file
    fclose(f);

    return true;
  }

  size_t Compress() {
    assert (uncompressed_buffer_.Initialized());

    z_stream stream;
    stream.zalloc   = Z_NULL;
    stream.zfree    = Z_NULL;
    stream.opaque   = Z_NULL;
    stream.next_in  = Z_NULL;
    stream.avail_in = 0;
    const int retval = deflateInit(&stream, Z_DEFAULT_COMPRESSION);
    if (retval != 0) {
      return 0;
    }

    const size_t output_size = deflateBound(&stream, uncompressed_buffer_.size());
    compressed_buffer_.Allocate(output_size);

    stream.avail_in  = uncompressed_buffer_.size();
    stream.next_in   = uncompressed_buffer_.ptr();
    stream.avail_out = output_size;
    stream.next_out  = compressed_buffer_.ptr();

    const int result = deflate(&stream, Z_FINISH);
    if (result != Z_STREAM_END) {
      return 0;
    }

    return stream.total_out;
  }

  void Hash() {
    assert (uncompressed_buffer_.Initialized());

    SHA1_Init(&sha1_context_);
    SHA1_Update(&sha1_context_,
                uncompressed_buffer_,
                uncompressed_buffer_.size());
    unsigned char sha[20];
    char sha_string[41];
    SHA1_Final(sha, &sha1_context_);

    for (unsigned i = 0; i < 20; ++i) {
      char dgt1 = (unsigned)sha[i] / 16;
      char dgt2 = (unsigned)sha[i] % 16;
      dgt1 += (dgt1 <= 9) ? '0' : 'a' - 10;
      dgt2 += (dgt2 <= 9) ? '0' : 'a' - 10;
      sha_string[i*2] = dgt1;
      sha_string[i*2+1] = dgt2;
    }
    sha_string[40] = '\0';

    sha1_ = static_cast<char*>(sha_string);
  }

  void Destroy() {
    uncompressed_buffer_ = CharBuffer();
    compressed_buffer_   = CharBuffer();
  }


 private:
  std::string  path_;
  CharBuffer   uncompressed_buffer_;
  CharBuffer   compressed_buffer_;
  SHA_CTX      sha1_context_;
  std::string  sha1_;
};
typedef std::vector<File> FileVector;


class TraversalDelegate {
 public:
  TraversalDelegate() {}
  ~TraversalDelegate() {}

  void FileCb(const std::string &relative_path,
              const std::string &file_name) {
    const std::string path = relative_path + "/" + file_name;
    File file(path);
    files_.push_back(std::move(file));
  }

  FileVector& files() { return files_; }

 private:
  FileVector files_;
};


int main() {
  TraversalDelegate delegate;
  tick_count start, end;
  tick_count all_start, all_end;



  all_start = tick_count::now();



  start = tick_count::now();
  FileSystemTraversal<TraversalDelegate> t(&delegate, "", true);
  t.fn_new_file = &TraversalDelegate::FileCb;
  t.Recurse("../benchmark_repo");
  end = tick_count::now();
  std::cout << "recursion took:   " << (end - start).seconds() << " seconds" << std::endl;



  FileVector &files = delegate.files();


  start = tick_count::now();
  for (auto &file : files) {
    file.Read();
  }
  end = tick_count::now();
  std::cout << "reading took:     " << (end - start).seconds() << " seconds" << std::endl;



  start = tick_count::now();
  parallel_for(blocked_range<size_t>(0, files.size()),
               [&](blocked_range<size_t> block) {
                 for (size_t i = block.begin(); i != block.end(); ++i) {
                    File &file = files[i];

                    // file.Read();
                    file.Compress();
                    file.Hash();
                 }
               });
  end = tick_count::now();
  std::cout << "crunching took:   " << (end - start).seconds() << " seconds" << std::endl;



  start = tick_count::now();
  for (auto &file : files) {
    file.Write();
  }
  end = tick_count::now();
  std::cout << "writing took:     " << (end - start).seconds() << " seconds" << std::endl;


  all_end = tick_count::now();
  std::cout << "overall time:     " << (all_end - all_start).seconds() << " seconds" << std::endl;

  return 0;
}
