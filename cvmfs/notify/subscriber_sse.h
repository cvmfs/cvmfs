
/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NOTIFY_SUBSCRIBER_SSE_H_
#define CVMFS_NOTIFY_SUBSCRIBER_SSE_H_

#include "subscriber.h"

#include <string>

#include "atomic.h"
#include "duplex_curl.h"

namespace notify {

/**
 * Notification system subscriber based on server-sent events (SSE)
 */
class SubscriberSSE : public Subscriber {
 public:
  explicit SubscriberSSE(const std::string& server_url);
  virtual ~SubscriberSSE();

  virtual bool Subscribe(const std::string& topic);
  virtual void Unsubscribe();

  const std::string& topic() const { return topic_; }
  const std::string& buffer() const { return buffer_; }
  bool should_quit() const { return atomic_read32(&should_quit_); }

  void AppendToBuffer(const std::string& s);
  void ClearBuffer();
  void RequestQuit() { atomic_write32(&should_quit_, 1); }

 private:
  static size_t CurlRecvCB(void* buffer, size_t size, size_t nmemb,
                           void* userp);
  static int CurlProgressCB(void* clientp, curl_off_t dltotal, curl_off_t dlnow,
                            curl_off_t ultotal, curl_off_t ulnow);

  std::string server_url_;
  std::string topic_;
  std::string buffer_;

  mutable atomic_int32 should_quit_;
};

}  // namespace notify

#endif  // CVMFS_NOTIFY_SUBSCRIBER_SSE_H_
