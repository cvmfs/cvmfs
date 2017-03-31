/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_RECEIVER_REACTOR_H_
#define CVMFS_RECEIVER_REACTOR_H_

#include <cstdlib>

namespace receiver {

class Reactor {
 public:
  Reactor(int fdin, int fdout);
  ~Reactor();

  bool run();

 private:
  int fdin_;
  int fdout_;
};

}  // namespace receiver

#endif  // CVMFS_RECEIVER_REACTOR_H_
