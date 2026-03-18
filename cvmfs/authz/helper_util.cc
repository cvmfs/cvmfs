/**
 * This file is part of the CernVM File System.
 */

#include "helper_util.h"

#include <alloca.h>
#include <errno.h>
#include <stdint.h>
#include <unistd.h>

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "authz/helper_log.h"
#include "json_document.h"

#ifdef __APPLE__
#define strdupa(s)                    \
  strcpy(/* NOLINT(runtime/printf) */ \
         reinterpret_cast<char *>(alloca(strlen((s)) + 1)), (s))
#endif

using namespace std;  // NOLINT

/**
 * Helper binaries are supposed to be called from the cvmfs client, not
 * stand-alone.
 */
void CheckCallContext() {
  if (getenv("CVMFS_AUTHZ_HELPER") == NULL) {
    printf("This program is supposed to be called from the CernVM-FS client.");
    printf("\n");
    abort();
  }
}


void ParseHandshakeInit(const string &msg) {
  JSON j = JSON::parse(msg);

  if (j.contains("cvmfs_authz_v1")) {
    const JSON &config = j["cvmfs_authz_v1"];

    if (config.contains("debug_log")) {
      SetLogAuthzDebug(config["debug_log"].get<string>() + ".authz");
    }
    if (config.contains("fqrn")) {
      const string fqrn = config["fqrn"].get<string>();
      LogAuthz(kLogAuthzDebug, "fqrn is %s", fqrn.c_str());
      SetLogAuthzSyslogPrefix(fqrn);
    }
    if (config.contains("syslog_level")) {
      SetLogAuthzSyslogLevel(config["syslog_level"].get<int>());
    }
    if (config.contains("syslog_facility")) {
      SetLogAuthzSyslogFacility(config["syslog_facility"].get<int>());
    }
  }
}


void ParseRequest(const string &msg) {
  JSON j = JSON::parse(msg);

  if (j.contains("cvmfs_authz_v1")) {
    const JSON &req = j["cvmfs_authz_v1"];

    if (req.contains("msgid")) {
      if (req["msgid"].get<int>() == 4) { /* kAuthzMsgQuit */
        LogAuthz(kLogAuthzDebug, "shut down");
        exit(0);
      }
    }
  }
}


/**
 * Get bytes from stdin.
 */
static void Read(void *buf, size_t nbyte) {
  int num_bytes;
  do {
    num_bytes = read(fileno(stdin), buf, nbyte);
  } while ((num_bytes < 0) && (errno == EINTR));
  assert((num_bytes >= 0) && (static_cast<size_t>(num_bytes) == nbyte));
}


/**
 * Reads a complete message from the cvmfs client.
 */
string ReadMsg() {
  uint32_t version;
  uint32_t length;
  Read(&version, sizeof(version));
  assert(version == kProtocolVersion);
  Read(&length, sizeof(length));
  if (length == 0)
    return "";
  char *buf = reinterpret_cast<char *>(alloca(length));
  Read(buf, length);
  return string(buf, length);
}


/**
 * Send bytes to stdout.
 */
static void Write(const void *buf, size_t nbyte) {
  int num_bytes;
  do {
    num_bytes = write(fileno(stdout), buf, nbyte);
  } while ((num_bytes < 0) && (errno == EINTR));
  assert((num_bytes >= 0) && (static_cast<size_t>(num_bytes) == nbyte));
}


/**
 * Sends a (JSON formatted) message back to the cvmfs client.
 */
void WriteMsg(const string &msg) {
  struct {
    uint32_t version;
    uint32_t length;
  } header;
  header.version = kProtocolVersion;
  header.length = msg.length();
  Write(&header, sizeof(header));
  Write(msg.data(), header.length);
}
