/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_RECEIVER_REACTOR_H_
#define CVMFS_RECEIVER_REACTOR_H_

#include <cstdlib>
#include <map>
#include <string>

#include "json_document.h"
#include "statistics.h"

namespace receiver {

class CommitProcessor;
class PayloadProcessor;

/**
 * This is the main event loop of the `cvmfs_receiver` program.
 *
 * It implements a synchronous protocol, reading JSON requests from the fdin_
 * file descriptor and writing JSON responses to fdout_. It handles the framing
 * of the protocol and the dispatching of the different types of events to
 * specific handler methods.
 */
class Reactor {
 public:
  enum Request {
    kQuit = 0,
    kEcho,
    kGenerateToken,
    kGetTokenId,
    kCheckToken,
    kSubmitPayload,
    kCommit,
    kError,
    kTestCrash  // use to test the gateway
  };

  static Request ReadRequest(int fd, std::string *data);
  static bool WriteRequest(int fd, Request req, const std::string &data);

  static bool ReadReply(int fd, std::string *data);
  static bool WriteReply(int fd, const std::string &data);

  static bool ExtractStatsFromReq(JsonDocument *req,
                                  perf::Statistics *stats,
                                  std::string *start_time);

  Reactor(int fdin, int fdout);
  virtual ~Reactor();

  bool Run();

 protected:
  // NOTE: These methods are virtual such that they can be mocked for the
  // purpose of unit testing
  virtual bool HandleGenerateToken(const std::string &req, std::string *reply);
  virtual bool HandleGetTokenId(const std::string &req, std::string *reply);
  virtual bool HandleCheckToken(const std::string &req, std::string *reply);
  virtual bool HandleSubmitPayload(int fdin, const std::string &req,
                                   std::string *reply);
  virtual bool HandleCommit(const std::string &req, std::string *reply);

  virtual PayloadProcessor *MakePayloadProcessor();
  virtual CommitProcessor *MakeCommitProcessor();

 private:
  bool HandleRequest(Request req, const std::string &data);

  int fdin_;
  int fdout_;

  std::map<std::string, perf::Statistics *> statistics_map_;
};

}  // namespace receiver

#endif  // CVMFS_RECEIVER_REACTOR_H_
