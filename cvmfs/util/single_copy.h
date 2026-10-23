/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_SINGLE_COPY_H_
#define CVMFS_UTIL_SINGLE_COPY_H_

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif


/**
 * Generic base class to mark an inheriting class as 'non-copyable'
 */
class SingleCopy {
 public:
  SingleCopy(const SingleCopy &other) = delete;
  SingleCopy &operator=(const SingleCopy &rhs) = delete;

 protected:
  // Prevent SingleCopy from being instantiated on its own
  SingleCopy() = default;
};


#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_SINGLE_COPY_H_
