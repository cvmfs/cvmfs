/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SANITIZER_H_
#define CVMFS_SANITIZER_H_

#include <string>

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

class InputSanitizer {
};


class AlphaNumSanitizer : public InputSanitizer {
};


#ifdef CVMFS_NAMESPACE_GUARD
}
#endif

#endif  // CVMFS_SANITIZER_H_
