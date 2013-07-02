
#include <iostream>
#include <cmath>
#include <cerrno>
#include <cassert>
#include <vector>
#include <sstream>

#include <tbb/parallel_for.h>
#include <tbb/blocked_range.h>
#include <tbb/tick_count.h>
#include <tbb/scalable_allocator.h>
#include <tbb/concurrent_queue.h>
#include <tbb/compat/thread>
#include <tbb/task.h>

#include <zlib.h>

#include <openssl/sha.h>

#include "cvmfs/fs_traversal.h"

using namespace tbb;

static const std::string output_path = "/Volumes/ramdisk";

void Print(const std::string &msg) {
  static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
  pthread_mutex_lock(&mutex);
  std::cout << msg << std::endl;
  pthread_mutex_unlock(&mutex);
}

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



class File;

class IoRequest {
 public:
  enum RequestType {
    Read,
    Write,
    Terminate,
    Unknown
  };

 public:
  IoRequest(const RequestType type = Unknown, File *file = nullptr) :
    type_(type), file_(file)
  {
    if (file != nullptr) {
      assert (type == Read || type == Write);
    } else {
      assert (type == Terminate || type == Unknown);
    }
  }

  IoRequest(const IoRequest &other) :
    type_(other.type_), file_(other.file_) {}

  bool        IsValid()      const { return type_ != Unknown;   }
  bool        IsTerminator() const { return type_ == Terminate; }
  File*       file()               { return file_;              }
  RequestType type()         const { return type_;              }

 private:
  RequestType  type_;
  File        *file_;
};


class CruncherTask : public task {
  public:
  CruncherTask(File *file, concurrent_bounded_queue<IoRequest> &io_queue) :
    file_(file), io_queue_(io_queue) {}

  task* execute();

 private:
  File                                 *file_;
  concurrent_bounded_queue<IoRequest>  &io_queue_;
};


class File {
 public:
  File() : path_(""),
           uncompressed_buffer_(NULL),
           compressed_buffer_(NULL),
           cruncher_task_(nullptr) {}
  explicit File(const std::string &path) :
    path_(path),
    cruncher_task_(nullptr) {}

  const std::string& path() const { return path_; }

  CharBuffer&  uncompressed_buffer() { return uncompressed_buffer_; }
  CharBuffer&  compressed_buffer()   { return compressed_buffer_;   }
  SHA_CTX&     sha1_context()        { return sha1_context_;        }
  std::string& sha1()                { return sha1_;                }
  task*        cruncher_task()       { return cruncher_task_;       }

  bool         IsDummy() const       { return path_.empty();        }

  void CreateTbbTask(task *parent, concurrent_bounded_queue<IoRequest> &io_queue) {
    CruncherTask *t = new(parent->allocate_child()) CruncherTask(this,
                                                                 io_queue);
    cruncher_task_ = t;
  }

  void SpawnTbbTask() {
    assert (cruncher_task_ != nullptr);

    task *parent = cruncher_task_->parent();
    parent->increment_ref_count();
    task::spawn(*cruncher_task_);
  }

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
      const size_t read_bytes = fread(uncompressed_buffer_.ptr(), 1, size, f);
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
    const size_t written = fwrite(compressed_buffer_.ptr(), 1,
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


 private:
  std::string   path_;
  CruncherTask *cruncher_task_;
  CharBuffer    uncompressed_buffer_;
  CharBuffer    compressed_buffer_;
  SHA_CTX       sha1_context_;
  std::string   sha1_;
};
typedef std::vector<File> FileVector;


task* CruncherTask::execute() {
  std::stringstream ss;
  ss << pthread_self();
  Print(ss.str());
  file_->Compress();
  file_->Hash();
  io_queue_.push(IoRequest(IoRequest::Write, file_));
  return NULL;
}


class TraversalDelegate {
 public:
  TraversalDelegate(concurrent_bounded_queue<IoRequest> &io_queue,
                    task *dummy_task) :
    io_queue_(io_queue), dummy_task_(dummy_task) {}
  ~TraversalDelegate() {}

  void FileCb(const std::string &relative_path,
              const std::string &file_name) {
    const std::string path = relative_path + "/" + file_name;
    File *file = new File(path);
    file->CreateTbbTask(dummy_task_, io_queue_);
    io_queue_.push(IoRequest(IoRequest::Read, file));
  }

 private:
  concurrent_bounded_queue<IoRequest>  &io_queue_;
  task                                 *dummy_task_;
};


static void IO(concurrent_bounded_queue<IoRequest>  *io_queue,
               task                                 *dummy_task) {

  Print("hello from IO");

  bool run = true;
  task *crunch_task;

  unsigned int files = 0;

  while (run) {
    IoRequest request;
    io_queue->pop(request);

    //Print("processing request... " + ((request.type() != IoRequest::Terminate) ? request.file()->path() : ""));

    switch (request.type()) {
      case IoRequest::Terminate:
        run = false;
        break;
      case IoRequest::Read:
        // Print("reading " + request.file()->path());
        request.file()->Read();
        request.file()->SpawnTbbTask();
        ++files;
        break;
      case IoRequest::Write:
        //Print("writing " + request.file()->path() + " (" + request.file()->sha1() + ")");
        request.file()->Write();
        delete request.file();
        --files;
        if (files == 0) {
          //dummy_task->decrement_ref_count();
        }
        break;
      default:
        assert (false);
    }
  }

  Print("ciao from IO");
}


int main() {
  tick_count start, end;
  tick_count all_start, all_end;

  concurrent_bounded_queue<IoRequest> io_queue;

  all_start = tick_count::now();

  task* dummy_task = new(task::allocate_root()) empty_task;
  dummy_task->increment_ref_count();
  tbb_thread io_thread(IO, &io_queue, dummy_task);

  TraversalDelegate delegate(io_queue, dummy_task);


  start = tick_count::now();
  FileSystemTraversal<TraversalDelegate> t(&delegate, "", true);
  t.fn_new_file = &TraversalDelegate::FileCb;
  t.Recurse("../benchmark_repo");
  end = tick_count::now();
  std::cout << "recursion took:   " << (end - start).seconds() << " seconds" << std::endl;

  Print("going to wait now...");

  dummy_task->spawn_and_wait_for_all(*dummy_task);
  dummy_task->destroy(*dummy_task);

  Print("waited...");

  io_queue.push(IoRequest(IoRequest::Terminate));
  io_thread.join();

  all_end = tick_count::now();
  std::cout << "overall time:     " << (all_end - all_start).seconds() << " seconds" << std::endl;

  return 0;
}
