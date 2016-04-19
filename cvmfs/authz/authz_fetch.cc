/**
 * This file is part of the CernVM File System.
 */

#define __STDC_FORMAT_MACROS
#include "authz_fetch.h"

#include <errno.h>
#include <signal.h>
#include <sys/wait.h>
#include <syslog.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cstring>

#include "clientctx.h"
#include "logging.h"
#include "platform.h"
#include "smalloc.h"
#include "util_concurrency.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/string.h"

using namespace std;  // NOLINT


const int AuthzExternalFetcher::kMinTtl = 0;
const uint32_t AuthzExternalFetcher::kProtocolVersion = 1;


AuthzExternalFetcher::AuthzExternalFetcher(
  const string &fqrn,
  const string &progname)
  : fqrn_(fqrn)
  , progname_(progname)
  , fd_send_(-1)
  , fd_recv_(-1)
  , pid_(-1)
  , fail_state_(false)
{
  InitLock();
}

AuthzExternalFetcher::AuthzExternalFetcher(
  const string &fqrn,
  int fd_send,
  int fd_recv)
  : fqrn_(fqrn)
  , fd_send_(fd_send)
  , fd_recv_(fd_recv)
  , pid_(-1)
  , fail_state_(false)
{
  InitLock();
}


AuthzExternalFetcher::~AuthzExternalFetcher() {
  int retval = pthread_mutex_destroy(&lock_);
  assert(retval == 0);

  if (fd_send_ >= 0)
    close(fd_send_);
  if (fd_recv_ >= 0)
    close(fd_recv_);

  if (pid_ > 0) {
    uint64_t now = platform_monotonic_time();
    int statloc;
    do {
      retval = waitpid(pid_, &statloc, WNOHANG);
      if (platform_monotonic_time() > (now + kChildTimeout)) {
        LogCvmfs(kLogAuthz, kLogSyslogWarn | kLogDebug,
                 "authz helper %s unresponsive, killing", progname_.c_str());
        kill(pid_, SIGKILL);
        break;
      }
    } while (retval == 0);
  }
}


void AuthzExternalFetcher::EnterFailState() {
  LogCvmfs(kLogAuthz, kLogSyslogErr | kLogDebug,
           "authz helper %s enters fail state, no more authorization",
           progname_.c_str());
  fail_state_ = true;
}


/**
 * Uses execve to start progname_.  The started program has stdin and stdout
 * connected to fd_send_ and fd_recv_ and the CVMFS_... environment variables
 * set.  Special care must be taken when we call fork here in an unknown state
 * of the client.  Therefore we can't use ManagedExec (we can't use malloc).
 *
 * A failed execve is not caught by this routine.  It will be caught in the
 * next step, when mother and child start talking.
 */
void AuthzExternalFetcher::ExecHelper() {
  int pipe_send[2];
  int pipe_recv[2];
  MakePipe(pipe_send);
  MakePipe(pipe_recv);
  char *argv0 = strdupa(progname_.c_str());
  char *argv[] = {argv0, NULL};
  char *envp[] = {NULL};
  int max_fd = sysconf(_SC_OPEN_MAX);
  assert(max_fd > 0);
  LogCvmfs(kLogAuthz, kLogDebug | kLogSyslog, "starting authz helper %s",
           argv0);

  pid_t pid = fork();
  if (pid == 0) {
    // Child process, close file descriptors and run the helper
    int retval = dup2(pipe_send[0], 0);
    assert(retval == 0);
    retval = dup2(pipe_recv[1], 1);
    assert(retval == 1);
    for (int fd = 2; fd < max_fd; fd++)
      close(fd);

    execve(argv0, argv, envp);
    syslog(LOG_USER | LOG_ERR, "failed to start authz helper %s (%d)",
           argv0, errno);
    abort();
  }
  assert(pid > 0);
  close(pipe_send[0]);
  close(pipe_recv[1]);

  // Don't receive a signal if the helper terminates
  signal(SIGPIPE, SIG_IGN);
  pid_ = pid;
  fd_send_ = pipe_send[1];
  fd_recv_ = pipe_recv[0];
}


AuthzStatus AuthzExternalFetcher::FetchWithinClientCtx(
  const std::string &membership,
  AuthzToken *authz_token,
  unsigned *ttl)
{
  assert(ClientCtx::GetInstance()->IsSet());
  uid_t uid;
  gid_t gid;
  pid_t pid;
  ClientCtx::GetInstance()->Get(&uid, &gid, &pid);
  *ttl = kDefaultTtl;

  MutexLockGuard lock_guard(lock_);
  if (fail_state_)
    return kAuthzNoHelper;

  bool retval;

  if (fd_send_ < 0) {
    ExecHelper();
    retval = Handshake();
    if (!retval)
      return kAuthzNoHelper;
  }
  assert((fd_send_ >= 0) && (fd_recv_ >= 0));

  string json_msg = string("{\"cvmfs_authz_v1\":{") +
    "\"msgid\":" + StringifyInt(kAuthzMsgVerify) + "," +
    "\"revision\":0," +
    "\"uid\":" +  StringifyInt(uid) + "," +
    "\"gid\":" +  StringifyInt(gid) + "," +
    "\"pid\":" +  StringifyInt(pid) + "," +
    "\"membership\":\"" +  JsonDocument::EscapeString(membership) + "\"" +
    "}}";
  retval = Send(json_msg) && Recv(&json_msg);
  if (!retval)
    return kAuthzNoHelper;
  AuthzExternalMsg binary_msg;
  retval = ParseMsg(json_msg, kAuthzMsgPermit, &binary_msg);
  if (!retval)
    return kAuthzNoHelper;

  if (binary_msg.permit.status == kAuthzOk) {
    *authz_token = binary_msg.permit.token;
    *ttl = binary_msg.permit.ttl;
    LogCvmfs(kLogAuthz, kLogDebug, "got token of type %d and size %u",
             binary_msg.permit.token.type, binary_msg.permit.token.size);
  }

  return binary_msg.permit.status;
}


/**
 * Establish communication link with a forked authz helper.
 */
bool AuthzExternalFetcher::Handshake() {
  string debug_log = GetLogDebugFile();
  string json_debug_log;
  if (debug_log != "")
    json_debug_log = ",\"debug_log\":\"" + debug_log + "\"";
  string json_msg = string("{") +
    "\"cvmfs_authz_v1\":{" +
    "\"msgid\":" + StringifyInt(0) + "," +
    "\"revision\":0," +
    "\"fqrn\":\"" + fqrn_ + "\"," +
    "\"syslog_facility\":" + StringifyInt(GetLogSyslogFacility()) + "," +
    "\"syslog_level\":" + StringifyInt(GetLogSyslogLevel()) +
    json_debug_log +
    "}}";
  bool retval = Send(json_msg);
  if (!retval)
    return false;

  retval = Recv(&json_msg);
  if (!retval)
    return false;
  AuthzExternalMsg binary_msg;
  retval = ParseMsg(json_msg, kAuthzMsgReady, &binary_msg);
  if (!retval)
    return false;

  return true;
}


void AuthzExternalFetcher::InitLock() {
  int retval = pthread_mutex_init(&lock_, NULL);
  assert(retval == 0);
}


bool AuthzExternalFetcher::Send(const string &msg) {
  // Line format: 4 byte protocol version, 4 byte length, message
  struct {
    uint32_t version;
    uint32_t length;
  } header;
  header.version = kProtocolVersion;
  header.length = msg.length();
  unsigned raw_length = sizeof(header) + msg.length();
  unsigned char *raw_msg = reinterpret_cast<unsigned char *>(
    alloca(raw_length));
  memcpy(raw_msg, &header, sizeof(header));
  memcpy(raw_msg + sizeof(header), msg.data(), header.length);

  bool retval = SafeWrite(fd_send_, raw_msg, raw_length);
  if (!retval)
    EnterFailState();
  return retval;
}


/**
 * We want to see valid JSON in the form
 * { "cvmfs_authz_v1" : {
 *     "msgid":
 *     "revision":
 *     ...
 *   }
 *   ...
 * }
 *
 * The contents of "cvmfs_authz_v1" depends on the msgid.  Additional fields
 * are ignored.  The protocol revision should indicate changes in the fields.
 */
bool AuthzExternalFetcher::ParseMsg(
  const std::string &json_msg,
  const AuthzExternalMsgIds expected_msgid,
  AuthzExternalMsg *binary_msg)
{
  assert(binary_msg != NULL);

  UniquePtr<JsonDocument> json_document(JsonDocument::Create(json_msg));
  if (!json_document.IsValid()) {
    LogCvmfs(kLogAuthz, kLogSyslogErr | kLogDebug,
             "invalid json from authz helper %s: %s",
             progname_.c_str(), json_msg.c_str());
    EnterFailState();
    return false;
  }

  JSON *json_authz = JsonDocument::SearchInObject(
    json_document->root(), "cvmfs_authz_v1", JSON_OBJECT);
  if (json_authz == NULL) {
    LogCvmfs(kLogAuthz, kLogSyslogErr | kLogDebug,
             "\"cvmfs_authz_v1\" not found in json from authz helper %s: %s",
             progname_.c_str(), json_msg.c_str());
    EnterFailState();
    return false;
  }

  if (!ParseMsgId(json_authz, binary_msg) ||
      (binary_msg->msgid != expected_msgid))
  {
    EnterFailState();
    return false;
  }
  if (!ParseRevision(json_authz, binary_msg)) {
    EnterFailState();
    return false;
  }
  if (binary_msg->msgid == kAuthzMsgPermit) {
    if (!ParsePermit(json_authz, binary_msg)) {
      EnterFailState();
      return false;
    }
  }
  return true;
}


bool AuthzExternalFetcher::ParseMsgId(
  JSON *json_authz,
  AuthzExternalMsg *binary_msg)
{
  JSON *json_msgid = JsonDocument::SearchInObject(
    json_authz, "msgid", JSON_INT);
  if (json_msgid == NULL) {
    LogCvmfs(kLogAuthz, kLogSyslogErr | kLogDebug,
             "\"msgid\" not found in json from authz helper %s",
             progname_.c_str());
    EnterFailState();
    return false;
  }

  if ((json_msgid->int_value < 0) ||
      (json_msgid->int_value >= kAuthzMsgInvalid))
  {
    LogCvmfs(kLogAuthz, kLogSyslogErr | kLogDebug,
             "invalid \"msgid\" in json from authz helper %s: %d",
             progname_.c_str(), json_msgid->int_value);
    EnterFailState();
    return false;
  }

  binary_msg->msgid = static_cast<AuthzExternalMsgIds>(json_msgid->int_value);
  return true;
}


/**
 * A permit must contain the authorization status.  Optionally it can come with
 * a "time to live" of the answer and a token (e.g. X.509 proxy certificate).
 */
bool AuthzExternalFetcher::ParsePermit(
  JSON *json_authz,
  AuthzExternalMsg *binary_msg)
{
  JSON *json_status =
    JsonDocument::SearchInObject(json_authz, "status", JSON_INT);
  if (json_status == NULL) {
    LogCvmfs(kLogAuthz, kLogSyslogErr | kLogDebug,
             "\"status\" not found in json from authz helper %s",
             progname_.c_str());
    EnterFailState();
    return false;
  }
  if ((json_status->int_value < 0) || (json_status->int_value > kAuthzUnknown))
  {
    binary_msg->permit.status = kAuthzUnknown;
  } else {
    binary_msg->permit.status = static_cast<AuthzStatus>(
      json_status->int_value);
  }

  JSON *json_ttl = JsonDocument::SearchInObject(json_authz, "ttl", JSON_INT);
  if (json_ttl == NULL) {
    LogCvmfs(kLogAuthz, kLogDebug, "no ttl, using default");
    binary_msg->permit.ttl = kDefaultTtl;
  } else {
    binary_msg->permit.ttl = std::max(kMinTtl, json_ttl->int_value);
  }

  JSON *json_token =
    JsonDocument::SearchInObject(json_authz, "x509_proxy", JSON_STRING);
  if (json_token != NULL) {
    binary_msg->permit.token.type = kTokenX509;
    string token_binary;
    bool valid_base64 = Debase64(json_token->string_value, &token_binary);
    if (!valid_base64) {
      LogCvmfs(kLogAuthz, kLogSyslogErr | kLogDebug,
               "invalid Base64 in 'x509_proxy' from authz helper %s",
               progname_.c_str());
      EnterFailState();
      return false;
    }
    unsigned size = token_binary.size();
    binary_msg->permit.token.size = size;
    if (size > 0) {
      // The token is passed to the AuthzSessionManager, which takes care of
      // freeing the memory
      binary_msg->permit.token.data = smalloc(size);
      memcpy(binary_msg->permit.token.data, token_binary.data(), size);
    }
  }

  return true;
}


bool AuthzExternalFetcher::ParseRevision(
  JSON *json_authz,
  AuthzExternalMsg *binary_msg)
{
  JSON *json_revision = JsonDocument::SearchInObject(
    json_authz, "revision", JSON_INT);
  if (json_revision == NULL) {
    LogCvmfs(kLogAuthz, kLogSyslogErr | kLogDebug,
             "\"revision\" not found in json from authz helper %s",
             progname_.c_str());
    EnterFailState();
    return false;
  }

  if (json_revision->int_value < 0) {
    LogCvmfs(kLogAuthz, kLogSyslogErr | kLogDebug,
             "invalid \"revision\" in json from authz helper %s: %d",
             progname_.c_str(), json_revision->int_value);
    EnterFailState();
    return false;
  }

  binary_msg->protocol_revision = json_revision->int_value;
  return true;
}


bool AuthzExternalFetcher::Recv(string *msg) {
  uint32_t version;
  ssize_t retval = SafeRead(fd_recv_, &version, sizeof(version));
  if (retval != int(sizeof(version))) {
    EnterFailState();
    return false;
  }
  if (version != kProtocolVersion) {
    LogCvmfs(kLogAuthz, kLogSyslogErr | kLogDebug,
             "authz helper uses unknown protocol version %u", version);
    EnterFailState();
    return false;
  }

  uint32_t length;
  retval = SafeRead(fd_recv_, &length, sizeof(length));
  if (retval != int(sizeof(length))) {
    EnterFailState();
    return false;
  }

  msg->clear();
  char buf[kPageSize];
  unsigned nbytes = 0;
  while (nbytes < length) {
    const unsigned remaining = length - nbytes;
    retval = SafeRead(fd_recv_, buf, std::min(kPageSize, remaining));
    if (retval < 0) {
      LogCvmfs(kLogAuthz, kLogSyslogErr | kLogDebug,
               "read failure from authz helper %s", progname_.c_str());
      EnterFailState();
      return false;
    }
    nbytes += retval;
    msg->append(buf, retval);
  }

  return true;
}
