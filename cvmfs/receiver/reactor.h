/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_RECEIVER_REACTOR_H_
#define CVMFS_RECEIVER_REACTOR_H_

#include <cstdlib>
#include <string>

namespace receiver {

class PayloadProcessor;

class Reactor {
 public:
  enum Request {
    kQuit = 0,
    kEcho,
    kGenerateToken,
    kGetTokenId,
    kCheckToken,
    kSubmitPayload,
    kError
  };

  static Request ReadRequest(int fd, std::string* data);
  static bool WriteRequest(int fd, Request req, const std::string& data);

  static bool ReadReply(int fd, std::string* data);
  static bool WriteReply(int fd, const std::string& data);

  Reactor(int fdin, int fdout);
  virtual ~Reactor();

  bool run();

 protected:
  virtual int HandleGenerateToken(const std::string& req, std::string* reply);
  virtual int HandleGetTokenId(const std::string& req, std::string* reply);
  virtual int HandleCheckToken(const std::string& req, std::string* reply);
  virtual int HandleSubmitPayload(int fdin, const std::string& req,
                                  std::string* reply);

  virtual PayloadProcessor* MakePayloadProcessor();

 private:
  bool HandleRequest(Request req, const std::string& data);

  int fdin_;
  int fdout_;
};

}  // namespace receiver

#endif  // CVMFS_RECEIVER_REACTOR_H_
