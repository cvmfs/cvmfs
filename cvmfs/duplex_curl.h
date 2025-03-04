/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_DUPLEX_CURL_H_
#define CVMFS_DUPLEX_CURL_H_

#ifdef _BUILT_IN_LIBCURL
  #include "curl/curl.h" // NOLINT
#else
  #include <curl/curl.h> // NOLINT
#endif

#endif  // CVMFS_DUPLEX_CURL_H_
