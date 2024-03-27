/**
 * This file is part of the CernVM File System.
 */

#include "network/parallel_download_coordinator.h"
#include "util/logging.h"
#include "util/smalloc.h"

namespace download {
ParallelDownloadCoordinator::ParallelDownloadCoordinator(
                                          int64_t min_buffers,
                                          int64_t max_buffers,
                                          int64_t inflight_buffers,
                                          size_t buffer_size) :
                                            min_buffers_(min_buffers),
                                            max_buffers_(max_buffers),
                                            inflight_buffers_(inflight_buffers),
                                            buffer_size_(buffer_size) {
  assert(min_buffers >= 0);
  assert(max_buffers >= 0);
  assert(inflight_buffers >= 1);

  data_tube_empty_elements_ = new Tube<DataTubeElement>(max_buffers);

  if (min_buffers_ > max_buffers_) {
    LogCvmfs(kLogDownload, kLogDebug, "Parallel downloads: "
                              "min empty buffers cached (%ld) is larger than "
                              "max empty buffers cached (%ld). Therefore "
                              "limiting min empty buffers to max empty buffers",
                              min_buffers_, max_buffers_);

    min_buffers_ = max_buffers_;
  }

  for (int64_t i = 0; i < min_buffers_; i++) {
    char *data = static_cast<char*>(smalloc(buffer_size_));
    DataTubeElement *ele = new DataTubeElement(data, buffer_size_,
                                               kActionUnused);
    data_tube_empty_elements_->EnqueueBack(ele);
  }

  LogCvmfs(kLogDownload, kLogDebug, "Parallel downloads activated with "
                                    "%ld min empty buffers cached, "
                                    "%ld max empty buffers cached, "
                                    "%ld max buffers inflight per download, "
                                    "%zu KiB buffer size",
                                    min_buffers_, max_buffers_,
                                    inflight_buffers_, buffer_size_);
}

DataTubeElement* ParallelDownloadCoordinator::GetUnusedDataTubeElement() {
  DataTubeElement* ele = data_tube_empty_elements_->TryPopFront();

  if (ele == NULL) {
    char *data = static_cast<char*>(smalloc(buffer_size_));
    ele = new DataTubeElement(data, buffer_size_, kActionUnused);
  }

  return ele;
}

void ParallelDownloadCoordinator::PutDataTubeElementToReuse(
                                                        DataTubeElement* ele) {
  ele->action = kActionUnused;
  Tube<DataTubeElement>::Link *link =
                                 data_tube_empty_elements_->TryEnqueueBack(ele);
  if (link == NULL) {  // queue is at max capacity
    delete ele;
  }
}

}  // namespace download
