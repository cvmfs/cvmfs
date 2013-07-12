
#include <stdint.h>
#include <memory>
#include <cassert>
#include <iostream>
#include <algorithm>
#include <sstream>

#include <zlib.h>
#include <openssl/sha.h>

#include <tbb/atomic.h>
#include <tbb/concurrent_queue.h>
#include <tbb/concurrent_vector.h>
#include <tbb/parallel_invoke.h>
#include <tbb/scalable_allocator.h>
#include <tbb/task.h>
#include <tbb/task_scheduler_init.h>
#include <tbb/tbb_thread.h>
#include <tbb/tick_count.h>

#include "cvmfs/platform.h"
#include "cvmfs/fs_traversal.h"




static const std::string input_path  = "/Volumes/ramdisk/input";
static const std::string output_path = "output";


static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

void Print(const std::string &msg) {
  pthread_mutex_lock(&mutex);
  std::cout << msg << std::endl;
  pthread_mutex_unlock(&mutex);
}

void PrintErr(const std::string &msg) {
  pthread_mutex_lock(&mutex);
  std::cerr << msg << std::endl;
  pthread_mutex_unlock(&mutex);
}



std::string ShaToString(const unsigned char *sha_digest) {
  char sha_string[41];
  for (unsigned i = 0; i < 20; ++i) {
    char dgt1 = (unsigned)sha_digest[i] / 16;
    char dgt2 = (unsigned)sha_digest[i] % 16;
    dgt1 += (dgt1 <= 9) ? '0' : 'a' - 10;
    dgt2 += (dgt2 <= 9) ? '0' : 'a' - 10;
    sha_string[i*2] = dgt1;
    sha_string[i*2+1] = dgt2;
  }
  sha_string[40] = '\0';
  return std::string(sha_string);
}







#define MEASURE_ALLOCATION_TIME
static const uint64_t kTimeResolution = 1000000000;

template<typename T, class A = std::allocator<T> >
class Buffer {
 public:
  Buffer() : base_offset_(0), size_(0), used_bytes_(0), buffer_(NULL) {
    Retain();
  }

  Buffer(const size_t size) : base_offset_(0),
                              size_(0),
                              used_bytes_(0),
                              buffer_(NULL) {
    Retain();
    Allocate(size);
  }

  ~Buffer() {
    assert (refcount_ == 0);
    Deallocate();
  }

  void Retain() { ++refcount_; }
  void Release() {
    if (--refcount_ == 0) {
      Deallocate();
    }
  }

  void Allocate(const size_t size) {
    assert (!IsInitialized());
#ifdef MEASURE_ALLOCATION_TIME
    tbb::tick_count start = tbb::tick_count::now();
#endif
    size_ = size;
    buffer_ = A().allocate(size_);
#ifdef MEASURE_ALLOCATION_TIME
    tbb::tick_count end = tbb::tick_count::now();
    allocation_time_ += (uint64_t)((end - start).seconds() * kTimeResolution);
#endif
  }

  bool IsInitialized() const { return size_ > 0; }

  typename A::pointer ptr() {
    assert (IsInitialized());
    return buffer_;
  }
  const typename A::pointer ptr() const {
    assert (IsInitialized());
    return buffer_;
  }

  void SetUsedBytes(const size_t bytes)  { used_bytes_ = bytes; }
  void SetBaseOffset(const off_t offset) { base_offset_ = offset; }

  size_t size()        const { return size_;        }
  size_t used_bytes()  const { return used_bytes_;  }
  off_t  base_offset() const { return base_offset_; }

 private:
  Buffer(const Buffer &other) { assert (false); } // no copy!
  Buffer& operator=(const Buffer& other) { assert (false); }

  void Deallocate() {
#ifdef MEASURE_ALLOCATION_TIME
    tbb::tick_count start = tbb::tick_count::now();
#endif
    if (size_ == 0) {
      return;
    }
    A().deallocate(buffer_, size_);
    buffer_     = NULL;
    size_       = 0;
    used_bytes_ = 0;
#ifdef MEASURE_ALLOCATION_TIME
    tbb::tick_count end = tbb::tick_count::now();
    deallocation_time_ += (uint64_t)((end - start).seconds() * kTimeResolution);
#endif
  }

 private:
  tbb::atomic<unsigned int>    refcount_;
  off_t                        base_offset_;
  size_t                       used_bytes_;
  size_t                       size_;
  typename A::pointer          buffer_;

 public:
  static tbb::atomic<uint64_t> allocation_time_;
  static tbb::atomic<uint64_t> deallocation_time_;
};
typedef Buffer<unsigned char, tbb::scalable_allocator<unsigned char> > CharBuffer;

template<typename T, class A>
tbb::atomic<uint64_t> Buffer<T, A>::allocation_time_;

template<typename T, class A>
tbb::atomic<uint64_t> Buffer<T, A>::deallocation_time_;

typedef std::vector<CharBuffer*> CharBufferVector;












class Chunk {
 public:
  Chunk(const off_t offset, const size_t size, const std::string &path) :
    path_(path),
    file_offset_(offset), chunk_size_(size),
    zlib_output_marker_(0), zlib_initialized_(false),
    sha1_digest_(""), sha1_initialized_(false)
  {
    Initialize();
  }

  bool IsInitialized() const { return zlib_initialized_ && sha1_initialized_; }

  off_t          offset()                 const { return file_offset_;        }
  size_t         size()                   const { return chunk_size_;         }

  SHA_CTX&       sha1_context()                 { return sha1_context_;       }
  unsigned char* sha1_digest()                  { return sha1_digest_;        }

  z_stream&      zlib_context()                 { return zlib_context_;       }
  off_t          zlib_output_marker()     const { return zlib_output_marker_; }
  void       set_zlib_output_marker(const off_t new_marker) {
    zlib_output_marker_ = new_marker;
  }

 protected:
  void Initialize() {
    const int sha1_retval = SHA1_Init(&sha1_context_);
    assert (sha1_retval == 1);

    zlib_context_.zalloc   = Z_NULL;
    zlib_context_.zfree    = Z_NULL;
    zlib_context_.opaque   = Z_NULL;
    zlib_context_.next_in  = Z_NULL;
    zlib_context_.avail_in = 0;
    const int zlib_retval = deflateInit(&zlib_context_, Z_DEFAULT_COMPRESSION);
    assert (zlib_retval == 0);

    zlib_initialized_ = true;
    sha1_initialized_ = true;
  }

 public:
  const std::string path_;

 private:
  off_t         file_offset_;
  size_t        chunk_size_;

  off_t         zlib_output_marker_;
  z_stream      zlib_context_;
  bool          zlib_initialized_;

  SHA_CTX       sha1_context_;
  unsigned char sha1_digest_[SHA_DIGEST_LENGTH];
  bool          sha1_initialized_;
};




class File {
 public:
  File(const std::string &path, const platform_stat64 &info) :
    path_(path), size_(info.st_size),
    bulk_chunk_(0, info.st_size, path) {}

  ~File() {
    tbb::concurrent_vector<Chunk*>::const_iterator i    = chunks_.begin();
    tbb::concurrent_vector<Chunk*>::const_iterator iend = chunks_.end();
    for (; i != iend; ++i) {
      delete *i;
    }
    chunks_.clear();
  }

  size_t size()             const { return size_; }
  const std::string& path() const { return path_; }

  const Chunk* bulk_chunk() const { return &bulk_chunk_; }
        Chunk* bulk_chunk()       { return &bulk_chunk_; }

 private:
  const std::string              path_;
  const size_t                   size_;

  tbb::concurrent_vector<Chunk*> chunks_;
  Chunk                          bulk_chunk_;
};














#define MEASURE_IO_TIME

class IoDispatcher {
 protected:
  static const size_t kMaxBufferSize;

 public:
  IoDispatcher() :
    read_thread_(IoDispatcher::ReadThread, this),
    write_thread_(IoDispatcher::WriteThread, this)
  {
    file_count_      = 0;
    files_in_flight_ = 0;
    all_enqueued_    = false;
  }

  ~IoDispatcher() {
    Wait();

#ifdef MEASURE_IO_TIME
    std::cout << "Reads took:  " << read_time_ << std::endl
              << "  average:   " << (read_time_ / file_count_) << std::endl
              << "Writes took: " << write_time_ << std::endl
              << "  average:   " << (write_time_ / file_count_) << std::endl;
#endif
  }

  void Wait() {
    all_enqueued_ = true;

    read_queue_.push(NULL);

    if (read_thread_.joinable()) {
      read_thread_.join();
    }

    if (write_thread_.joinable()) {
      write_thread_.join();
    }
  }

  void ScheduleRead(File *file) {
    read_queue_.push(file);
    ++file_count_;
  }

  void ScheduleWrite(Chunk *chunk, CharBuffer *buffer) {
    buffer->Retain();
    write_queue_.push(std::make_pair(chunk, buffer));
  }

 protected:
  static void ReadThread(IoDispatcher *dispatcher) {
    tbb::task_scheduler_init sched(
      tbb::task_scheduler_init::default_num_threads() + 1);

    while (true) {
      File *file;
      dispatcher->read_queue_.pop(file);
      if (file == NULL) {
        break;
      }

      dispatcher->files_in_flight_++;

#ifdef MEASURE_IO_TIME
      tbb::tick_count start = tbb::tick_count::now();
#endif
      ReadFileAndSpawnTasks(file, dispatcher);
#ifdef MEASURE_IO_TIME
      tbb::tick_count end = tbb::tick_count::now();
      dispatcher->read_time_ += (end - start).seconds();
#endif
    }
  }

  static void WriteThread(IoDispatcher *dispatcher) {
    while (true) {
      std::pair<Chunk*, CharBuffer*> writable_item;
      dispatcher->write_queue_.pop(writable_item);
      Chunk *chunk       = writable_item.first;
      CharBuffer *buffer = writable_item.second;
      if (chunk == NULL || buffer == NULL) {
        break;
      }

#ifdef MEASURE_IO_TIME
      tbb::tick_count start = tbb::tick_count::now();
#endif
      WriteBufferToChunk(chunk, buffer);
#ifdef MEASURE_IO_TIME
      tbb::tick_count end = tbb::tick_count::now();
      dispatcher->write_time_ += (end - start).seconds();
#endif

      dispatcher->files_in_flight_--;
      if (dispatcher->files_in_flight_ == 0 && dispatcher->all_enqueued_) {
        break;
      }
    }
  }

  static bool ReadFileAndSpawnTasks(File *file, IoDispatcher *dispatcher);
  static bool WriteBufferToChunk(Chunk* chunk, CharBuffer *buffer) {
    buffer->Release();
    return true;
  }

 private:
  tbb::atomic<unsigned int> files_in_flight_;
  tbb::atomic<bool>         all_enqueued_;
  tbb::atomic<unsigned int> file_count_;

  double read_time_;
  double write_time_;

  tbb::concurrent_bounded_queue<File*> read_queue_;
  tbb::concurrent_bounded_queue<std::pair<Chunk*, CharBuffer*> > write_queue_;

  tbb::tbb_thread read_thread_;
  tbb::tbb_thread write_thread_;
};

const size_t IoDispatcher::kMaxBufferSize = 1048576;














template <class CruncherT>
class ChunkCruncher {
 public:
  ChunkCruncher(Chunk         *chunk,
                CharBuffer    *buffer,
                IoDispatcher  *io_dispatcher) :
    chunk_(chunk), buffer_(buffer), cruncher_(io_dispatcher)
  {
    buffer_->Retain();
  }

  ~ChunkCruncher() {
    buffer_->Release();
  }

  void operator()() const {
    // Hack: parallel_invoke expects a const callable operator... wtf
    ChunkCruncher  *self     = const_cast<ChunkCruncher*>(this);
    CruncherT      &cruncher = self->cruncher();
    Chunk          *chunk    = self->chunk();
    CharBuffer     *buffer   = self->buffer();

    assert (chunk->IsInitialized());
    assert (buffer->IsInitialized());

    const off_t internal_offset =
      std::max(off_t(0), chunk->offset() - buffer->base_offset());
    assert (internal_offset < buffer->used_bytes());
    const unsigned char *data = buffer->ptr() + internal_offset;

    const size_t byte_count = (chunk->size() == 0)
      ? buffer->used_bytes()
      :   std::min(buffer->base_offset() + buffer->used_bytes(),
                   chunk->offset()       + chunk->size())
        - std::max(buffer->base_offset(), chunk->offset());
    assert (byte_count <= buffer->used_bytes() - internal_offset);

    const bool finalize = (
      (chunk->size() > 0) &&
      (buffer->base_offset() + internal_offset + byte_count
        == chunk->offset() + chunk->size())
    );

    cruncher.Crunch(chunk, data, byte_count, finalize);
  }

 protected:
  Chunk*      chunk()    { return chunk_;    }
  CharBuffer* buffer()   { return buffer_;   }
  CruncherT&  cruncher() { return cruncher_; }

 private:
  ChunkCruncher(const ChunkCruncher &other) { assert (false); } // no copy!
  ChunkCruncher& operator=(const ChunkCruncher& other) { assert (false); }

 private:
  Chunk       *chunk_;
  CharBuffer  *buffer_;
  CruncherT    cruncher_;
};


class ChunkHasher {
 public:
  ChunkHasher(IoDispatcher *io_dispatcher) {}

  void Crunch(Chunk                *chunk,
              const unsigned char  *data,
              const size_t          bytes,
              const bool            finalize) {
    const int upd_rc = SHA1_Update(&chunk->sha1_context(), data, bytes);
    assert (upd_rc == 1);

    if (finalize) {
      const int fin_rc = SHA1_Final(chunk->sha1_digest(),
                                    &chunk->sha1_context());
      assert (fin_rc == 1);
    }
  }
};


class ChunkCompressor {
 public:
  ChunkCompressor(IoDispatcher *io_dispatcher) :
    io_dispatcher_(io_dispatcher) {}

  void Crunch(Chunk                *chunk,
              const unsigned char  *data,
              const size_t          bytes,
              const bool            finalize) {
    off_t marker     = chunk->zlib_output_marker();
    z_stream &stream = chunk->zlib_context();

    const size_t max_output_size = deflateBound(&stream, bytes);
    CharBuffer *compress_buffer  = new CharBuffer(max_output_size);

    stream.avail_in  = bytes;
    stream.next_in   = const_cast<unsigned char*>(data); // sry, but zlib forces me...
    const int flush = (finalize) ? Z_FINISH : Z_NO_FLUSH;

    int retcode = -1;
    bool done = false;
    while (true) {
      stream.avail_out = compress_buffer->size();
      stream.next_out  = compress_buffer->ptr();

      retcode = deflate(&stream, flush);
      assert (retcode == Z_OK || retcode == Z_STREAM_END);

      const size_t bytes_written = compress_buffer->size() - stream.avail_out;
      compress_buffer->SetUsedBytes(bytes_written);
      compress_buffer->SetBaseOffset(marker);
      io_dispatcher_->ScheduleWrite(chunk, compress_buffer);
      marker += bytes_written;
      compress_buffer->Release();

      if ((flush == Z_NO_FLUSH && retcode == Z_OK) ||
          (flush == Z_FINISH   && retcode == Z_STREAM_END)) {
        break;
      }

      if (stream.avail_out == 0) {
        compress_buffer = new CharBuffer(32768);
      }
    }

    if (finalize) {
      assert (flush == Z_FINISH);
      assert (stream.avail_in == 0);

      retcode = deflateEnd(&stream);
      assert (retcode == Z_OK);
    }
  }

 private:
  IoDispatcher *io_dispatcher_;
};



class FileScrubbingTask : public tbb::task {
 public:
  static const size_t kMinChunkSize;
  static const size_t kAvgChunkSize;
  static const size_t kMaxChunkSize;

 public:
  FileScrubbingTask(File *file, CharBuffer *buffer, IoDispatcher *io_dispatcher) :
    file_(file), buffer_(buffer), io_dispatcher_(io_dispatcher), next_(NULL)
  {
    buffer_->Retain();
  }

  void SetNext(FileScrubbingTask *next) {
    next->increment_ref_count();
    next_ = next;
  }

  tbb::task* Next() {
    return (next_ != NULL && next_->decrement_ref_count() == 0)
      ? next_
      : NULL;
  }

  tbb::task* execute() {
    // if (file_->size() > kAvgChunkSize) {
    //   const off_t cut_mark = TryToFindFileCutMark();
    //   if (cut_mark > 0) {

    //   }
    // }

    ChunkCruncher<ChunkHasher>     hasher    (file_->bulk_chunk(),
                                              buffer_,
                                              io_dispatcher_);
    ChunkCruncher<ChunkCompressor> compressor(file_->bulk_chunk(),
                                              buffer_,
                                              io_dispatcher_);

    tbb::parallel_invoke(hasher, compressor);
    buffer_->Release();
    return Next();
  }

  std::string GetIdString() {
    std::stringstream ss;
    ss << file_->path() << " " << buffer_->base_offset() << " "
       << buffer_->used_bytes() << " refcnt: " << ref_count();
    return ss.str();
  }

 protected:
  off_t TryToFindFileCutMark() {
    return 0;
  }

 public:
  File               *file_;
  CharBuffer         *buffer_;
  IoDispatcher       *io_dispatcher_;
  FileScrubbingTask  *next_;
};

const size_t FileScrubbingTask::kMinChunkSize =  4 * 1024 * 1024;
const size_t FileScrubbingTask::kAvgChunkSize =  8 * 1024 * 1024;
const size_t FileScrubbingTask::kMaxChunkSize = 16 * 1024 * 1024;


















bool IoDispatcher::ReadFileAndSpawnTasks(File *file, IoDispatcher *dispatcher) {
  const std::string &path = file->path();
  const size_t       size = file->size();

  // open the file
  int fd;
  if ((fd = open(path.c_str(), O_RDONLY, 0)) == -1) {
    std::cerr << "cannot open file: " << path << std::endl;
    return false;
  }

  // read the file chunk-wise
  size_t       file_marker    = 0;
  unsigned int chunks_to_read = (size / kMaxBufferSize) +
                                std::min(size_t(1), size % kMaxBufferSize);

  FileScrubbingTask  *previous_task      = NULL;
  tbb::task          *previous_sync_task = NULL;

  for (unsigned int i = 0; i < chunks_to_read; ++i) {
    const size_t bytes_to_read_now = std::min(kMaxBufferSize,
                                              size - file_marker);
    CharBuffer *buffer = new CharBuffer(bytes_to_read_now);
    buffer->SetBaseOffset(file_marker);

    const size_t bytes_read = read(fd, buffer->ptr(), bytes_to_read_now);
    if (bytes_read != bytes_to_read_now) {
      std::cerr << "failed to read " << bytes_to_read_now << " from " << path
                << std::endl;
      close(fd);
      return false;
    }

    buffer->SetUsedBytes(bytes_read);

    // create an asynchronous task to process the data chunk, together with
    // a synchronisation task that ensures the correct execution order of the
    // processing tasks
    // Note: the last task for any given file does not need to wait for a
    //       synchronisation task since it has no successor.
    FileScrubbingTask  *new_task =
      new(tbb::task::allocate_root()) FileScrubbingTask(file,
                                                        buffer,
                                                        dispatcher);
    new_task->increment_ref_count();
    tbb::task *sync_task = new(new_task->allocate_child()) tbb::empty_task();

    // decorate the predecessor task (i-1) with it's successor (i) and allow
    // it to be scheduled by TBB
    // Note: all asynchronous tasks for a single file need to be processed in
    //       the correct order, since they depend on each other
    //       (This inherently kills parallelism in the first stage but every
    //        asynchronous task spawned here may spawn additional independent
    //        sub-tasks)
    if (previous_task != NULL) {
      previous_task->SetNext(new_task);
      tbb::task::enqueue(*previous_sync_task);
    }

    previous_task      = new_task;
    previous_sync_task = sync_task;
    file_marker       += bytes_read;
  }

  tbb::task::enqueue(*previous_sync_task);

  // close the file
  assert (file_marker == size);
  close(fd);

  return true;
}
















class TraversalDelegate {
 public:
  TraversalDelegate(IoDispatcher *io_dispatcher) :
    io_dispatcher_(io_dispatcher) {}

  void FileCb(const std::string      &relative_path,
              const std::string      &file_name,
              const platform_stat64  &info) {
    const std::string path = relative_path + "/" + file_name;
    File *file = new File(path, info);
    io_dispatcher_->ScheduleRead(file);
  }

 private:
  IoDispatcher *io_dispatcher_;
};






int main() {
  tbb::tick_count start, end;
  tbb::tick_count all_start, all_end;

  all_start = tbb::tick_count::now();

  IoDispatcher io_dispatcher;

  TraversalDelegate delegate(&io_dispatcher);

  start = tbb::tick_count::now();
  FileSystemTraversal<TraversalDelegate> t(&delegate, "", true);
  t.fn_new_file = &TraversalDelegate::FileCb;
  t.Recurse(input_path);
  end = tbb::tick_count::now();
  std::cout << "recursion took:   " << (end - start).seconds() << " seconds" << std::endl;

  Print("going to wait now...");
  io_dispatcher.Wait();
  Print("waited...");

#ifdef MEASURE_ALLOCATION_TIME
  std::cout << "allocations took: " << ((double)CharBuffer::allocation_time_   / (double)kTimeResolution) << " seconds" << std::endl;
  std::cout << "deallocs took:    " << ((double)CharBuffer::deallocation_time_ / (double)kTimeResolution) << " seconds" << std::endl;
#endif

  all_end = tbb::tick_count::now();
  std::cout << "overall time:     " << (all_end - all_start).seconds() << " seconds" << std::endl;

  return 0;
}






