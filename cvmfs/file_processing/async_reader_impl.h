/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FILE_PROCESSING_ASYNC_READER_IMPL_H_
#define CVMFS_FILE_PROCESSING_ASYNC_READER_IMPL_H_

#include <algorithm>
#include <cerrno>

#include "../logging.h"

// TODO(remeusel): remove this... wrong namespace (for testing)
namespace upload {

template <class FileScrubbingTaskT, class FileT>
bool Reader<FileScrubbingTaskT, FileT>::Initialize() {
  // TODO(rmeusel): exactly the same Initialize()/TearDown() concept is
  // implemented in AbstractUploader. It might be worth to facter out that code
  // into an extra template that handles clean thread creation/destruction
  // and perhaps the connection of the thread using a tbb::[...]queue
  tbb::tbb_thread thread(&ThreadProxy<Reader>,
                          this,
                         &Reader<FileScrubbingTaskT, FileT>::ReadThread);

  assert(!read_thread_.joinable());
  read_thread_ = thread;
  assert(read_thread_.joinable());
  assert(!thread.joinable());

  // wait for the thread to call back...
  const bool successful_startup = thread_started_executing_.Get();
  if (successful_startup) {
    running_ = true;
  }

  return successful_startup;
}


template <class FileScrubbingTaskT, class FileT>
void Reader<FileScrubbingTaskT, FileT>::ReadThread() {
  thread_started_executing_.Set(true);

  while (HasData()) {
    // acquire a new job from the job queue:
    // -> if the queue is empty, just continue on the work currently in flight
    // -> if the popped value is NULL, the drainout is initiated
    FileJob job;
    const bool popped_new_job = TryToAcquireNewJob(&job);
    if (popped_new_job) {
      if (!job.terminate) {
        OpenNewFile(job.file);
      } else {
        EnableDraining();
      }
    }

    // read File Blocks in a round robin fashion and schedule these blocks for
    // processing
    typename OpenFileList::iterator       i    = open_files_.begin();
    typename OpenFileList::const_iterator iend = open_files_.end();
    for (; i != iend; ++i) {
      const bool finished_reading = ReadAndScheduleNextBuffer(&(*i));
      if (finished_reading) {
        OpenFile file = *i;
        i = open_files_.erase(i);
        CloseFile(&file);
      }
    }
  }
}


template <class FileScrubbingTaskT, class FileT>
bool Reader<FileScrubbingTaskT, FileT>::TryToAcquireNewJob(FileJob *next_job) {
  // In draining mode we only process files that are already open
  if (draining_) {
    return false;
  }

  bool popped = false;

  // If we have no more work, we allow the thread to block otherwise we just
  // acquire what we can get and continue working
  if (open_files_.empty()) {
    queue_.pop(*next_job);
    popped = true;
  } else {
    popped = queue_.try_pop(*next_job);
  }

  return popped;
}


template <class FileScrubbingTaskT, class FileT>
void Reader<FileScrubbingTaskT, FileT>::OpenNewFile(FileT *file) {
  const int fd = open(file->path().c_str(), O_RDONLY, 0);
  if (fd < 0) {
    switch (errno) {
      case EMFILE:
        LogCvmfs(kLogSpooler, kLogStderr, "File open() failed due to a lack of "
                                          "file descriptors! Please increase "
                                          "this limit. (see ulimit -n)");
        break;
      case EACCES:
      case EPERM:
        LogCvmfs(kLogSpooler, kLogStderr,
                 "File open() failed due to permission issues. "
                 "Please check '%s' and retry", file->path().c_str());
    }
  }
  assert(fd > 0);

  OpenFile open_file;
  open_file.file            = file;
  open_file.file_descriptor = fd;
  open_files_.push_back(open_file);
}


template <class FileScrubbingTaskT, class FileT>
void Reader<FileScrubbingTaskT, FileT>::CloseFile(OpenFile *file) {
  const int retval = close(file->file_descriptor);
  assert(retval == 0);
}


template <class FileScrubbingTaskT, class FileT>
void Reader<FileScrubbingTaskT, FileT>::FinalizedFile(AbstractFile *file) {
  assert(file != NULL);

  // notify subscribed callbacks for a finalized file
  FileT *concrete_file = static_cast<FileT*>(file);
  this->NotifyListeners(concrete_file);

  --files_in_flight_counter_;
}


template <class FileScrubbingTaskT, class FileT>
bool Reader<FileScrubbingTaskT, FileT>::
  ReadAndScheduleNextBuffer(OpenFile *open_file)
{
  assert(open_file->file != NULL);
  assert(open_file->file_descriptor > 0);


  // All asynchronous tasks for a single File need to be processed sequentially,
  // since they depend on each other. We use the following TBB task graph to
  // produce this behaviour.
  //
  // => Task graph for one specific File with multipe data Blocks:
  //      FileScrubbingTaskT -> FST
  //      tbb::empty_task    -> SyncTask
  //
  // +----------+     +----------+     +----------+     +----------+
  // | SyncTask |     | SyncTask |     | SyncTask |     | SyncTask |
  // +----------+     +----------+     +----------+     +----------+
  //      |                |                |                |
  //     `|´              `|´              `|´              `|´
  //   +-----+          +-----+          +-----+          +-----+
  //   | FST |--------->| FST |--------->| FST |--------->| FST |
  //   +-----+          +-----+          +-----+          +-----+
  //
  // Each FST is associated with a SyncTask as well as it's successing FST. FSTs
  // depend on their SyncTasks, thus they only run after this SyncTask has been
  // executed.
  // An FST must not be executed by TBB before the association to it's successor
  // has been established and thus cannot be simply spawned directly after
  // creation (race condition - TBB against Reader thread).
  //
  // Given a big File that needs to be read in multiple data Blocks we will see
  // the following synchronization pattern after the first data Block has been
  // loaded into memory:
  // We create FST-0 together with SyncTask-0 and store both without spawning
  // them in TBB (thus they will not run yet). As soon as the second data Block
  // of the File arrives, FST-1 together with SyncTask-1 is created and FST-0 is
  // given a pointer to FST-1. We then enqueue SyncTask-0 to TBB and thus allow
  // the associated FST-0 to run and process the first data Block.
  // When FST-0 finishes, it will return FST-1 to TBB for execution (forcing the
  // sequential order of FSTs).
  // Now FST-1 needs to wait for SyncTask-1 to run before it can execute. Given
  // that the third data Block was already loaded in the mean time (associating
  // FST-1 with it's successor FST-2 and enqueuing SyncTask-1) TBB can run FST-1
  // immediately. Otherwise TBB needs to suspend the processing of this file and
  // wait until more data arrives.



  // figure out how many bytes need to be read in this step and create a
  // CharBuffer to accomodate these bytes
  const size_t file_size     = open_file->file->size();
  const size_t bytes_to_read =
    std::min(file_size - static_cast<size_t>(open_file->file_marker),
             max_buffer_size_);
  CharBuffer *buffer = CreateBuffer(bytes_to_read);
  buffer->SetBaseOffset(open_file->file_marker);

  // read the next data Block into the just created CharBuffer and check if
  // everything worked as expected
  const size_t bytes_read = read(open_file->file_descriptor,
                                 buffer->ptr(),
                                 bytes_to_read);
  assert(bytes_to_read == bytes_read);
  buffer->SetUsedBytes(bytes_read);
  open_file->file_marker += bytes_read;

  // check if the file has been fully read
  const bool finished_reading =
    (static_cast<size_t>(open_file->file_marker) == file_size);

  // create an asynchronous task (FileScrubbingTaskT) to process the data chunk,
  // together with a synchronization task that ensures the correct execution
  // order of the FileScrubbingTasks
  FileScrubbingTaskT *new_task =
    new(tbb::task::allocate_root()) FileScrubbingTaskT(open_file->file,
                                                       buffer,
                                                       finished_reading,
                                                       this);
  new_task->increment_ref_count();
  tbb::task *sync_task = new(new_task->allocate_child()) tbb::empty_task();

  // decorate the predecessor task (i-1) with it's successor (i) and allow the
  // predecessor to be scheduled by TBB (task::enqueue)
  if (open_file->previous_task != NULL) {
    open_file->previous_task->SetNext(new_task);
    tbb::task::enqueue(*open_file->previous_sync_task);
  }

  open_file->previous_task      = new_task;
  open_file->previous_sync_task = sync_task;

  // make sure that the last chunk is processed
  if (finished_reading) {
    tbb::task::enqueue(*open_file->previous_sync_task);
  }

  return finished_reading;
}

}  // namespace upload

#endif  // CVMFS_FILE_PROCESSING_ASYNC_READER_IMPL_H_
