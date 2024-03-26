/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NETWORK_PARALLEL_DOWNLOAD_COORDINATOR_H_
#define CVMFS_NETWORK_PARALLEL_DOWNLOAD_COORDINATOR_H_

#include "duplex_curl.h"
#include "util/tube.h"

namespace download {

class JobInfo;

enum DataTubeAction {
  kActionStop = 0,
  kActionContinue,
  kActionEndOfData,
  kActionUnused,
  kActionData
};

/**
 * Wrapper for the data tube to transfer data from CallbackCurlData() that is
 * executed in MainDownload() Thread to Fetch() called by a fuse thread
 *
 * TODO(heretherebedragons): do we want to have a pool of those datatubeelements?
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

struct TupelJobDone {
  JobInfo* info;
  int curl_error;
  CURL *easy_handle;

  TupelJobDone(JobInfo* i, int error, CURL *handle) :
                             info(i), curl_error(error), easy_handle(handle) { }
};

class ParallelDownloadCoordinator {
 public:
  ParallelDownloadCoordinator(int64_t min_buffers,
                              int64_t max_buffers,
                              int64_t inflight_buffers,
                              size_t buffer_size);
  DataTubeElement* GetUnusedDataTubeElement();
  void PutDataTubeElementToReuse(DataTubeElement* ele);
  size_t buffer_size() const { return buffer_size_; }

 private:
  int64_t min_buffers_;
  int64_t max_buffers_;
  int64_t inflight_buffers_;
  size_t buffer_size_;

  /**
   * Tube to hold empty elements use in JobInfo data_tube_
   * Shared with all DownloadManagers
   * Must be static because of CallbackCurlData
   */
  UniquePtr<Tube<DataTubeElement> > data_tube_empty_elements_;
};  // ParallelDownloadCoordinator

}  // namespace download

#endif  // CVMFS_NETWORK_PARALLEL_DOWNLOAD_COORDINATOR_H_
