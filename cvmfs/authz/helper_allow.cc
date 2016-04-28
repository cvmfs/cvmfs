/**
 * This file is part of the CernVM File System.
 */
#include <unistd.h>

#include <string>

#include "helper_log.h"
#include "helper_util.h"
#include "json.h"
typedef struct json_value JSON;

using namespace std;  // NOLINT


int main() {
  CheckCallContext();

  // Handshake
  string msg = ReadMsg();
  ParseHandshakeInit(msg);
  WriteMsg("{\"cvmfs_authz_v1\":{\"msgid\":1,\"revision\":0}}");
  LogAuthz(kLogAuthzDebug | kLogAuthzSyslog,
           "authz allow helper invoked, connected to cvmfs process %d",
           getppid());

  while (true) {
    msg = ReadMsg();
    LogAuthz(kLogAuthzDebug, "got authz request %s", msg.c_str());
    ParseRequest(msg);
    WriteMsg("{\"cvmfs_authz_v1\":{\"msgid\":3,\"revision\":0,\"status\":0}}");
  }

  return 0;
}
