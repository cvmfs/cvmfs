/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NOTIFY_SUBSCRIBER_SSE_H_
#define CVMFS_NOTIFY_SUBSCRIBER_SSE_H_

#include <string>

#include "duplex_curl.h"
#include "subscriber.h"
#include "util/atomic.h"

namespace notify {

/**
 * Notification system subscriber based on server-sent events (SSE)
 *
 * This class uses Curl to establish a long-lived HTTP connection to the
 * notification system server. The expected response is of type
 * "text/event-stream". The repository activity messages arrive as text lines
 * with the following format:
 *
 * data: <MESSAGE BODY>
 *
 * The class is responsible for extracting the body of each message from the
 * responses and consuming the messages when needed.
 */
class SubscriberSSE : public Subscriber {
 public:
  explicit SubscriberSSE(const std::string &server_url);
  virtual ~SubscriberSSE();

  virtual bool Subscribe(const std::string &topic);
  virtual void Unsubscribe();

 private:
  bool ShouldQuit() const;

  /**
   * Append a string to the internal receive buffer
   *
   * The given string is stripped of any event stream protocol prefix
   * ("data: "), before being appended to the internal buffer used by
   * the Curl receive callback.
   */
  void AppendToBuffer(const std::string &s);

  void ClearBuffer();

  static size_t CurlRecvCB(void *buffer, size_t size, size_t nmemb,
                           void *userp);
  static int CurlProgressCB(void *clientp, curl_off_t dltotal, curl_off_t dlnow,
                            curl_off_t ultotal, curl_off_t ulnow);

  std::string server_url_;
  std::string topic_;
  std::string buffer_;

  mutable atomic_int32 should_quit_;
};

}  // namespace notify

#endif  // CVMFS_NOTIFY_SUBSCRIBER_SSE_H_
