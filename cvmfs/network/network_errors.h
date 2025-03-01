/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NETWORK_NETWORK_ERRORS_H_
#define CVMFS_NETWORK_NETWORK_ERRORS_H_

namespace download {

/**
 * Possible return values.  Adjust ObjectFetcher error handling if new network
 * error conditions are added.
 */
enum Failures {
  kFailOk = 0,
  kFailLocalIO,
  kFailBadUrl,
  kFailProxyResolve,
  kFailHostResolve,
  // artificial failure code.  Try other host even though
  // failure seems to be at the proxy
  kFailHostAfterProxy,
  kFailProxyConnection,
  kFailHostConnection,
  kFailProxyHttp,
  kFailHostHttp,
  kFailBadData,
  kFailTooBig,
  kFailOther,
  kFailUnsupportedProtocol,
  kFailProxyTooSlow,
  kFailHostTooSlow,
  kFailProxyShortTransfer,
  kFailHostShortTransfer,
  kFailCanceled,

  kFailNumEntries
};  // Failures


inline bool IsHostTransferError(const Failures error) {
  switch (error) {
    case kFailHostConnection:
    case kFailHostTooSlow:
    case kFailHostShortTransfer:
      return true;
    default:
      break;
  }
  return false;
}

inline bool IsProxyTransferError(const Failures error) {
  switch (error) {
    case kFailProxyConnection:
    case kFailProxyTooSlow:
    case kFailProxyShortTransfer:
      return true;
    default:
      break;
  }
  return false;
}

inline const char *Code2Ascii(const Failures error) {
  const char *texts[kFailNumEntries + 1];
  texts[0] = "OK";
  texts[1] = "local I/O failure";
  texts[2] = "malformed URL";
  texts[3] = "failed to resolve proxy address";
  texts[4] = "failed to resolve host address";
  texts[5] = "all proxies failed, trying host fail-over";
  texts[6] = "proxy connection problem";
  texts[7] = "host connection problem";
  texts[8] = "proxy returned HTTP error";
  texts[9] = "host returned HTTP error";
  texts[10] = "corrupted data received";
  texts[11] = "resource too big to download";
  texts[12] = "unknown network error";
  texts[13] = "Unsupported URL in protocol";
  texts[14] = "proxy serving data too slowly";
  texts[15] = "host serving data too slowly";
  texts[16] = "proxy data transfer cut short";
  texts[17] = "host data transfer cut short";
  texts[18] = "request canceled";
  texts[19] = "no text";
  return texts[error];
}

}  // namespace download

#endif  // CVMFS_NETWORK_NETWORK_ERRORS_H_
