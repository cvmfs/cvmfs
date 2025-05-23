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
#include "json.h"
typedef struct json_value JSON;

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
  block_allocator allocator(2048);
  char *err_pos;
  char *err_desc;
  int err_line;
  JSON *json = json_parse(strdupa(msg.c_str()), &err_pos, &err_desc, &err_line,
                          &allocator);
  assert((json != NULL) && (json->first_child != NULL));
  json = json->first_child;
  assert((string(json->name) == "cvmfs_authz_v1"));
  json = json->first_child;
  while (json) {
    string name(json->name);
    if (name == "debug_log") {
      SetLogAuthzDebug(string(json->string_value) + ".authz");
    } else if (name == "fqrn") {
      LogAuthz(kLogAuthzDebug, "fqrn is %s", json->string_value);
      SetLogAuthzSyslogPrefix(string(json->string_value));
    } else if (name == "syslog_level") {
      SetLogAuthzSyslogLevel(json->int_value);
    } else if (name == "syslog_facility") {
      SetLogAuthzSyslogFacility(json->int_value);
    }
    json = json->next_sibling;
  }
}


void ParseRequest(const string &msg) {
  block_allocator allocator(2048);
  char *err_pos;
  char *err_desc;
  int err_line;
  JSON *json = json_parse(strdupa(msg.c_str()), &err_pos, &err_desc, &err_line,
                          &allocator);
  assert((json != NULL) && (json->first_child != NULL));
  json = json->first_child;
  assert((string(json->name) == "cvmfs_authz_v1"));
  json = json->first_child;
  while (json) {
    string name(json->name);
    if (name == "msgid") {
      if (json->int_value == 4) { /* kAuthzMsgQuit */
        LogAuthz(kLogAuthzDebug, "shut down");
        exit(0);
      }
    }
    json = json->next_sibling;
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
