/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NETWORK_PARALLEL_DOWNLOAD_COORDINATOR_H_
#define CVMFS_NETWORK_PARALLEL_DOWNLOAD_COORDINATOR_H_

#include "duplex_curl.h"
#include "util/tube.h"

namespace download {

class JobInfo;

/**
 * Describes what kind of action should be performed on a given DataTubeElement
 */
enum DataTubeAction {
  kActionStop = 0,
  kActionUnused,        // unused data element
  kActionData,
  kActionEndOfData,
  kActionDownloadDone,  // download processing is completely finished
                        // (download be success or failure)
  // hand-shake commands for failed downloads, must be done in order
  kActionCheckRepeat,   // download failure, check if repeat should be performed
  kActionShouldRetry,   // download failure, check if retry should be executed
  kActionRetry          // download failure, do retry
};

/**
 * Struct with additional info for the data tube to transfer data
 * from CallbackCurlData() [MainDownload()-thread] to Fetch() [fuse thread]
 *
 * It is also used to communicate synchronization. For this only DataTubeAction
 * is used.
 *
 * DataTubeElement is the owner of "data" and will free it on destruction.
 */
struct DataTubeElement : SingleCopy {
  char* data;
  size_t size;
  DataTubeAction action;

  explicit DataTubeElement(DataTubeAction xact) :
                                           data(NULL), size(0), action(xact) { }
  DataTubeElement(char* mov_data, size_t xsize, DataTubeAction xact) :
                                   data(mov_data), size(xsize), action(xact) { }

  ~DataTubeElement() {
    free(data);
  }
};

/**
 * Struct used in download.cc::MainDownload()
 * Contains jobs where CURL has finished downloading all chunks, but that might
 * not yet be finished processing/decompressing all chunks in Fetch() of the
 * fuse-thread
 */
struct TupelJobDone {
  JobInfo* info;
  CURL *easy_handle;

  TupelJobDone(JobInfo* i, CURL *handle) : info(i), easy_handle(handle) { }
};

class ParallelDownloadCoordinator {
 public:
  ParallelDownloadCoordinator(int64_t min_buffers,
                              int64_t max_buffers,
                              int64_t inflight_buffers,
                              size_t buffer_size);
  DataTubeElement* GetUnusedElement();
  void PutElementToReuse(DataTubeElement* ele);
  size_t buffer_size() const { return buffer_size_; }
  int64_t min_buffers() const { return min_buffers_; }
  int64_t max_buffers() const { return max_buffers_; }
  int64_t inflight_buffers() const { return inflight_buffers_; }

 private:
  int64_t min_buffers_;  // prefilled #elements in data_tube_empty_elements_
  int64_t max_buffers_;  // max size of data_tube_empty_elements_

  /**
   * For each jobinfo/download request:
   * How many DataTubeElements the Jobinfo.data_tube_ can take between
   * CallbackCurlData() and Fetch() before CallbackCurlData() will block and
   * wait for processing to finish in Fetch()
   */
  int64_t inflight_buffers_;

  /**
   * Buffer size of data buffer in DataTubeElement
   * Should be CURL_MAX_WRITE_SIZE if used with CallbackCurlData() as this is
   * the maximum size a single CallbackCurlData() CURL data buffer will have
   */
  size_t buffer_size_;

  /**
   * Tube to hold empty elements use in JobInfo data_tube_
   * Shared with all DownloadManagers
   */
  UniquePtr<Tube<DataTubeElement> > data_tube_empty_elements_;
};  // ParallelDownloadCoordinator

}  // namespace download

#endif  // CVMFS_NETWORK_PARALLEL_DOWNLOAD_COORDINATOR_H_
