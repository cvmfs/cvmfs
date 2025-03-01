/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_INTERRUPT_H_
#define CVMFS_INTERRUPT_H_

/**
 * Allows to query for interrupts of active file system requests.  Used
 * to handle canceled fuse requests with the inherited class FuseInterruptCue.
 */
class InterruptCue {
 public:
  InterruptCue() { }
  virtual ~InterruptCue() { }
  virtual bool IsCanceled() { return false; }
};

#endif  // CVMFS_INTERRUPT_H_
