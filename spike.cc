
#include <iostream>
#include <cmath>
#include <cerrno>
#include <cassert>
#include <vector>
#include <sstream>

#include <tbb/parallel_for.h>
#include <tbb/parallel_invoke.h>
#include <tbb/blocked_range.h>
#include <tbb/tick_count.h>
#include <tbb/scalable_allocator.h>
#include <tbb/concurrent_queue.h>
#include <tbb/compat/thread>
#include <tbb/task.h>
#include <tbb/task_scheduler_init.h>
#include <tbb/task_scheduler_observer.h>
#include <tbb/atomic.h>

#include <zlib.h>

#include <openssl/sha.h>

#include "cvmfs/fs_traversal.h"

using namespace tbb;

static const std::string input_path  = "../benchmark_repo";
static const std::string output_path = "/Volumes/ramdisk/output";

void Print(const std::string &msg) {
  static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
  pthread_mutex_lock(&mutex);
  std::cout << msg << std::endl;
  pthread_mutex_unlock(&mutex);
}

template<typename T, class A = std::allocator<T> >
class Buffer {
 public:
  Buffer() : size_(0), used_bytes_(0), buffer_(nullptr) {}

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

  void SetUsedBytes(const size_t bytes) { used_bytes_ = bytes; }

  const size_t size()       const { return size_;       }
  const size_t used_bytes() const { return used_bytes_; }

 private:
  Buffer(const Buffer &other) { assert (false); } // no copy!
  Buffer& operator=(const Buffer& other) { assert (false); }

  void Deallocate() {
    if (size_ == 0) {
      return;
    }
    A().deallocate(buffer_, size_);
    std::stringstream ss;
    ss << "freeing: " << size_ << " bytes";
    Print(ss.str());
  }

 private:
  size_t               used_bytes_;
  size_t               size_;
  typename A::pointer  buffer_;
};
typedef Buffer<unsigned char, scalable_allocator<unsigned char> > CharBuffer;



class File;
class IoDispatcher;

class CruncherTask : public task {
  public:
  CruncherTask(File *file, IoDispatcher *io_dispatcher) :
    file_(file), io_dispatcher_(io_dispatcher) {}

  task* execute();

 private:
  File          *file_;
  IoDispatcher  *io_dispatcher_;
};


class File {
 public:
  File() : path_(""),
           uncompressed_buffer_(NULL),
           compressed_buffer_(NULL) {}
  explicit File(const std::string &path) :
    path_(path) {}

  const std::string& path() const { return path_; }

  CharBuffer&  uncompressed_buffer() { return uncompressed_buffer_; }
  CharBuffer&  compressed_buffer()   { return compressed_buffer_;   }
  SHA_CTX&     sha1_context()        { return sha1_context_;        }
  std::string& sha1()                { return sha1_;                }

  bool         IsDummy() const       { return path_.empty();        }

  void SpawnTbbTask(IoDispatcher *io_dispatcher) {
    CruncherTask *t = new(task::allocate_root()) CruncherTask(this, io_dispatcher);
    task::enqueue(*t);
  }

 public:
  bool Read() {
    //return ReadOldfashion();
    //return ReadMmap();
    return ReadFd();
  }

  bool Write() {
    assert (sha1_.size() > 0);
    assert (compressed_buffer_.Initialized());

    return WriteOldfashion();
    //return WriteFd();
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

    compressed_buffer_.SetUsedBytes(stream.total_out);
    return stream.total_out;
  }

  void Hash() {
    assert (uncompressed_buffer_.Initialized());

    SHA1_Init(&sha1_context_);
    SHA1_Update(&sha1_context_,
                uncompressed_buffer_.ptr(),
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


 protected:
  bool ReadOldfashion() {
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
      const size_t read_bytes = fread(uncompressed_buffer_.ptr(), 1, size, f);
    } while(!feof(f) && !ferror(f));

    // close the file
    fclose(f);

    return true;
  }

  bool ReadMmap() {
    // open the file
    int fd;
    if ((fd = open(path_.c_str(), O_RDONLY, 0)) == -1) {
      std::cout << "cannot open file: " << path_ << std::endl;
      return false;
    }

    // get file size
    struct stat64 filesize;
    if (fstat64(fd, &filesize) != 0) {
      std::cout << "failed to fstat file: " << path_ << std::endl;
      close(fd);
      return false;
    }

    void *mapping = NULL;
    if (filesize.st_size > 0) {
      // map the given file into memory
      mapping = mmap(NULL, filesize.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
      if (mapping == MAP_FAILED) {
        std::cout << "failed to mmap file: " << path_ << std::endl;
        close(fd);
        return false;
      }
    }

    // save results
    uncompressed_buffer_.Allocate(filesize.st_size);
    memcpy(uncompressed_buffer_.ptr(), mapping, filesize.st_size);

    // do unmap
    if ((munmap(static_cast<void*>(mapping), filesize.st_size) != 0) ||
        (close(fd) != 0))
    {
      std::cout << "failed to unmap file: " << path_ << std::endl;
    }
    return true;
  }

  bool ReadFd() {
    // open the file
    int fd;
    if ((fd = open(path_.c_str(), O_RDONLY, 0)) == -1) {
      std::cout << "cannot open file: " << path_ << std::endl;
      return false;
    }

    // get file size
    struct stat64 filesize;
    if (fstat64(fd, &filesize) != 0) {
      std::cout << "failed to fstat file: " << path_ << std::endl;
      close(fd);
      return false;
    }

    // allocate space
    uncompressed_buffer_.Allocate(filesize.st_size);

    if (read(fd, uncompressed_buffer_.ptr(), filesize.st_size) != filesize.st_size) {
      std::cout << "failed to read file: " << path_ << std::endl;
      close(fd);
      return false;
    }

    // close the file
    close(fd);

    return true;
  }

  bool WriteOldfashion() {
    const std::string path = output_path + "/" + sha1_;

    // open file
    FILE *f = fopen(path.c_str(), "w+");
    if (f == NULL) {
      std::cout << "cannot open file: " << path
                << " errno: " << errno << std::endl;
      return false;
    }

    // write buffer
    const size_t to_write = compressed_buffer_.used_bytes();
    const size_t written = fwrite(compressed_buffer_.ptr(), 1, to_write, f);
    if (written != to_write) {
      std::cout << "failed to write " << to_write << " bytes "
                << "to file: " << path << std::endl;
      return false;
    }

    // close the file
    fclose(f);

    return true;
  }

  bool WriteFd() {
    // open the file
    int fd;
    if ((fd = open(path_.c_str(), O_WRONLY | O_CREAT, 0)) == -1) {
      std::cout << "cannot open file: " << path_ << std::endl;
      return false;
    }

    // write to file
    const size_t bytes = compressed_buffer_.used_bytes();
    if (write(fd, compressed_buffer_.ptr(), bytes) != bytes) {
      std::cout << "failed to read file: " << path_ << std::endl;
      close(fd);
      return false;
    }

    // close the file
    close(fd);
    return true;
  }


 private:
  std::string   path_;
  CruncherTask *cruncher_task_;
  CharBuffer    uncompressed_buffer_;
  CharBuffer    compressed_buffer_;
  SHA_CTX       sha1_context_;
  std::string   sha1_;
};
typedef std::vector<File> FileVector;


#define MEASURE_IO_TIME


class IoDispatcher {
 public:
  IoDispatcher() :
    files_in_flight_(0),
    file_count_(0),
    all_enqueued_(false),
    read_thread_(IoDispatcher::ReadThread, this),
    write_thread_(IoDispatcher::WriteThread, this)
  {}

  ~IoDispatcher() {
    Wait();

    std::cout << "Reads took:  " << read_time_ << std::endl
              << "  average:   " << (read_time_ / file_count_) << std::endl
              << "Writes took: " << write_time_ << std::endl
              << "  average:   " << (write_time_ / file_count_) << std::endl;
  }

  void Wait() {
    all_enqueued_ = true;

    read_queue_.push(nullptr);

    if (read_thread_.joinable()) {
      read_thread_.join();
    }

    if (write_thread_.joinable()) {
      write_thread_.join();
    }
  }

  void Read(File *file) {
    read_queue_.push(file);
    ++file_count_;
  }
  void Write(File *file) {
    write_queue_.push(file);
  }

 protected:
  static void ReadThread(IoDispatcher *dispatcher) {
    task_scheduler_init sched(3);

    while (true) {
      File *file;
      dispatcher->read_queue_.pop(file);
      if (file == nullptr) {
        break;
      }

      dispatcher->files_in_flight_++;

#ifdef MEASURE_IO_TIME
      tick_count start = tick_count::now();
      file->Read();
      tick_count end = tick_count::now();
      dispatcher->read_time_ += (end - start).seconds();
#else
      file->Read();
#endif

      file->SpawnTbbTask(dispatcher);
    }
  }

  static void WriteThread(IoDispatcher *dispatcher) {
    while (true) {
      File *file;
      dispatcher->write_queue_.pop(file);
      if (file == nullptr) {
        break;
      }

#ifdef MEASURE_IO_TIME
      tick_count start = tick_count::now();
      file->Write();
      tick_count end = tick_count::now();
      dispatcher->write_time_ += (end - start).seconds();
#else
      file->Write();
#endif

      dispatcher->files_in_flight_--;
      if (dispatcher->files_in_flight_ == 0 && dispatcher->all_enqueued_) {
        break;
      }
    }
  }

 private:
  atomic<unsigned int> files_in_flight_;
  atomic<bool>         all_enqueued_;
  unsigned int         file_count_;

  double read_time_;
  double write_time_;

  concurrent_bounded_queue<File*> read_queue_;
  concurrent_bounded_queue<File*> write_queue_;

  tbb_thread read_thread_;
  tbb_thread write_thread_;
};



task* CruncherTask::execute() {
  file_->Compress();
  file_->Hash();
  io_dispatcher_->Write(file_);
  return NULL;
}


class TraversalDelegate {
 public:
  TraversalDelegate(IoDispatcher *io_dispatcher) :
    io_dispatcher_(io_dispatcher) {}

  void FileCb(const std::string &relative_path,
              const std::string &file_name) {
    const std::string path = relative_path + "/" + file_name;
    File *file = new File(path);
    io_dispatcher_->Read(file);
  }

 private:
  IoDispatcher *io_dispatcher_;
};


class Observer : public task_scheduler_observer {
  void on_scheduler_entry(bool is_worker) {
    Print("ENTER");
  }

  void on_scheduler_exit(bool is_worker) {
    Print("EXIT");
  }
};

int main() {
  tick_count start, end;
  tick_count all_start, all_end;

  Observer observer;
  observer.observe();

  all_start = tick_count::now();

  IoDispatcher io_dispatcher;

  TraversalDelegate delegate(&io_dispatcher);

  start = tick_count::now();
  FileSystemTraversal<TraversalDelegate> t(&delegate, "", true);
  t.fn_new_file = &TraversalDelegate::FileCb;
  t.Recurse(input_path);
  end = tick_count::now();
  std::cout << "recursion took:   " << (end - start).seconds() << " seconds" << std::endl;

  Print("going to wait now...");
  io_dispatcher.Wait();
  Print("waited...");

  all_end = tick_count::now();
  std::cout << "overall time:     " << (all_end - all_start).seconds() << " seconds" << std::endl;

  return 0;
}
