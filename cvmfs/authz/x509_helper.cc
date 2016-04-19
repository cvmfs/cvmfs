/**
 * This file is part of the CernVM File System.
 */
#define __STDC_FORMAT_MACROS

#include <alloca.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <unistd.h>

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "authz/x509_helper_base64.h"
#include "authz/x509_helper_fetch.h"
#include "authz/x509_helper_log.h"
#include "authz/x509_helper_req.h"
#include "json.h"
typedef struct json_value JSON;

#ifdef __APPLE__
#define strdupa(s) strcpy(/* NOLINT(runtime/printf) */\
  reinterpret_cast<char *>(alloca(strlen((s)) + 1)), (s))
#endif

using namespace std;  // NOLINT


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
 * Reads a complete message from the cvmfs client.
 */
static string ReadMsg() {
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
 * Sends a (JSON formatted) message back to the cvmfs client.
 */
static void WriteMsg(const string &msg) {
  struct {
    uint32_t version;
    uint32_t length;
  } header;
  header.version = kProtocolVersion;
  header.length = msg.length();
  Write(&header, sizeof(header));
  Write(msg.data(), header.length);
}


static void ParseHandshakeInit(const string &msg) {
  block_allocator allocator(2048);
  char *err_pos; char *err_desc; int err_line;
  JSON *json = json_parse(strdupa(msg.c_str()),
                          &err_pos, &err_desc, &err_line,
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
      SetLogAuthzSyslogPrefix(string(json->string_value));
    } else if (name == "syslog_level") {
      SetLogAuthzSyslogLevel(json->int_value);
    } else if (name == "syslog_facility") {
      SetLogAuthzSyslogFacility(json->int_value);
    }
    json = json->next_sibling;
  }
}


/**
 * Extracts the information from a "verify" request from the cvmfs client.
 */
static AuthzRequest ParseRequest(const string &msg) {
  block_allocator allocator(2048);
  char *err_pos; char *err_desc; int err_line;
  JSON *json = json_parse(strdupa(msg.c_str()),
                          &err_pos, &err_desc, &err_line,
                          &allocator);
  assert((json != NULL) && (json->first_child != NULL));
  AuthzRequest result;
  json = json->first_child;
  assert((string(json->name) == "cvmfs_authz_v1"));
  json = json->first_child;
  while (json) {
    string name(json->name);
    if (name == "uid") {
      result.uid = json->int_value;
    } else if (name == "gid") {
      result.gid = json->int_value;
    } else if (name == "pid") {
      result.pid = json->int_value;
    } else if (name == "membership") {
      result.membership = string(json->string_value);
    }
    json = json->next_sibling;
  }
  return result;
}


/**
 * This binary is supposed to be called from the cvmfs client, not stand-alone.
 */
static void CheckCallContext() {
  int retval = fcntl(fileno(stderr), F_GETFD, 0);
  if (retval != -1) {
    printf("This program is supposed to be called from the CernVM-FS client.");
    printf("\n");
    abort();
  }
}


int main() {
  CheckCallContext();

  // Handshake
  string msg = ReadMsg();
  ParseHandshakeInit(msg);
  WriteMsg("{\"cvmfs_authz_v1\":{\"msgid\":1,\"revision\":0}}");
  LogAuthz(kLogAuthzDebug | kLogAuthzSyslog,
           "x509 authz helper invoked, connected to cvmfs process %d",
           getppid());

  while (true) {
    msg = ReadMsg();
    LogAuthz(kLogAuthzDebug, "got authz request %s", msg.c_str());
    AuthzRequest request = ParseRequest(msg);
    string proxy;
    FILE *fp_proxy = GetX509Proxy(request, &proxy);
    if (fp_proxy == NULL) {
      // kAuthzNotFound
      WriteMsg("{\"cvmfs_authz_v1\":{\"msgid\":3,\"revision\":0,"
               "\"status\":1}}");
      continue;
    }

    fclose(fp_proxy);
    string reply = "{\"cvmfs_authz_v1\":{\"msgid\":3,\"revision\":0,"
                   "\"status\":0,\"x509_proxy\":\"" + Base64(proxy) + "\"}}";
    LogAuthz(kLogAuthzDebug, "reply %s", reply.c_str());
    WriteMsg(reply);
  }

  return 0;
}



/**
 * Create the VOMS data structure to the best of our abilities.
 *
 * Resulting memory is owned by caller and must be destroyed with VOMS_Destroy.
 */
/*struct authz_state {
  FILE *m_fp;
  struct vomsdata *m_voms;
  globus_gsi_cred_handle_t m_cred;
  BIO *m_bio;
  X509 *m_cert;
  EVP_PKEY *m_pkey;
  STACK_OF(X509) *m_chain;
  char *m_subject;
  globus_gsi_callback_data_t m_callback;

  authz_state() :
    m_fp(NULL),
    m_voms(NULL),
    m_cred(NULL),
    m_bio(NULL),
    m_cert(NULL),
    m_pkey(NULL),
    m_chain(NULL),
    m_subject(NULL),
    m_callback(NULL)
  {}

  ~authz_state() {
    if (m_fp) {fclose(m_fp);}
    if (m_voms) {(*g_VOMS_Destroy)(m_voms);}
    if (m_cred) {(*g_globus_gsi_cred_handle_destroy)(m_cred);}
    if (m_bio) {BIO_free(m_bio);}
    if (m_cert) {X509_free(m_cert);}
    if (m_pkey) {EVP_PKEY_free(m_pkey);}
    if (m_chain) {sk_X509_pop_free(m_chain, X509_free);}
    if (m_subject) {OPENSSL_free(m_subject);}
    if (m_callback) {(*g_globus_gsi_callback_data_destroy)(m_callback);}
  }
};


authz_data*
CredentialsFetcher::GenerateVOMSData(uid_t uid, gid_t gid, pid_t pid)
{
  authz_state state;

  state.m_fp = GetProxyFileInternal(pid, uid, gid);
  if (!state.m_fp) {
    LogCvmfs(kLogVoms, kLogDebug, "Could not find process's proxy file.");
    return NULL;
  }

  // Start of Globus proxy parsing and verification...
  globus_result_t result =
      (*g_globus_gsi_cred_handle_init)(&state.m_cred, NULL);
  if (GLOBUS_SUCCESS != result) {
    GlobusLib::GetInstance().PrintError(result);
    return NULL;
  }

  state.m_bio = BIO_new_fp(state.m_fp, 0);
  if (!state.m_bio) {
    LogCvmfs(kLogVoms, kLogDebug, "Unable to allocate new BIO object");
    return NULL;
  }

  result = (*g_globus_gsi_cred_read_proxy_bio)(state.m_cred, state.m_bio);
  if (GLOBUS_SUCCESS != result) {
    LogCvmfs(kLogVoms, kLogDebug, "Failed to parse credentials");
    GlobusLib::GetInstance().PrintError(result);
    return NULL;
  }

  // Setup Globus callback object.
  result = (*g_globus_gsi_callback_data_init)(&state.m_callback);
  if (GLOBUS_SUCCESS != result) {
    GlobusLib::GetInstance().PrintError(result);
    return NULL;
  }
  char *cert_dir;
  result = (*g_globus_gsi_sysconfig_get_cert_dir_unix)(&cert_dir);
  if (GLOBUS_SUCCESS != result) {
    LogCvmfs(kLogVoms, kLogDebug, "Failed to determine trusted certificates "
                                  "directory.");
    GlobusLib::GetInstance().PrintError(result);
    return NULL;
  }
  result = (*g_globus_gsi_callback_set_cert_dir)(state.m_callback, cert_dir);
  free(cert_dir);
  if (GLOBUS_SUCCESS != result) {
    GlobusLib::GetInstance().PrintError(result);
    return NULL;
  }

  // Verify credential chain.
  result = (*g_globus_gsi_cred_verify_cert_chain)(state.m_cred,
                                                  state.m_callback);
  if (GLOBUS_SUCCESS != result) {
    LogCvmfs(kLogVoms, kLogDebug, "Failed to validate credentials");
    GlobusLib::GetInstance().PrintError(result);
    return NULL;
  }

  // Load key and certificate from Globus handle
  result = (*g_globus_gsi_cred_get_key)(state.m_cred, &state.m_pkey);
  if (GLOBUS_SUCCESS != result) {
    LogCvmfs(kLogVoms, kLogDebug, "Failed to get process private key.");
    GlobusLib::GetInstance().PrintError(result);
    return NULL;
  }
  result = (*g_globus_gsi_cred_get_cert)(state.m_cred, &state.m_cert);
  if (GLOBUS_SUCCESS != result) {
    LogCvmfs(kLogVoms, kLogDebug, "Failed to get process certificate.");
    GlobusLib::GetInstance().PrintError(result);
    return NULL;
  }

  // Check proxy public key and private key match.
  if (!X509_check_private_key(state.m_cert, state.m_pkey)) {
    LogCvmfs(kLogVoms, kLogDebug, "Process certificate and key do not match");
    return NULL;
  }

  // Load certificate chain
  result = (*g_globus_gsi_cred_get_cert_chain)(state.m_cred, &state.m_chain);
  if (GLOBUS_SUCCESS != result) {
    LogCvmfs(kLogVoms, kLogDebug, "Process does not have cert chain.");
    GlobusLib::GetInstance().PrintError(result);
    return NULL;
  }

  // Look through certificates to find an EEC (which has the subject)
  globus_gsi_cert_utils_cert_type_t cert_type;
  X509 *eec_cert = state.m_cert;
  result = (*g_globus_gsi_cert_utils_get_cert_type)(state.m_cert, &cert_type);
  if (GLOBUS_SUCCESS != result) {
    GlobusLib::GetInstance().PrintError(result);
    return NULL;
  }
  if (!(cert_type & GLOBUS_GSI_CERT_UTILS_TYPE_EEC)) {
    result = (*g_globus_gsi_cert_utils_get_identity_cert)(state.m_chain,
                                                          &eec_cert);
    if (GLOBUS_SUCCESS != result) {
      GlobusLib::GetInstance().PrintError(result);
      return NULL;
    }
  }
  // From the EEC, use OpenSSL to determine the subject
  char *dn = X509_NAME_oneline(X509_get_subject_name(eec_cert), NULL, 0);
  if (!dn) {
    LogCvmfs(kLogVoms, kLogDebug, "Unable to determine certificate DN.");
    return NULL;
  }
  state.m_subject = strdup(dn);
  OPENSSL_free(dn);

  state.m_voms = (*g_VOMS_Init)(NULL, NULL);
  if (!state.m_voms) {
    return NULL;
  }

  int voms_error = 0;
  const int retval = (*g_VOMS_Retrieve)(state.m_cert, state.m_chain,
                                        RECURSE_CHAIN,
                                        state.m_voms, &voms_error);
  // If there is no VOMS extension (VERR_NOEXT), this shouldn't be fatal.
  if (!retval && (voms_error != VERR_NOEXT)) {
    char *err_str = (*g_VOMS_ErrorMessage)(state.m_voms, voms_error, NULL, 0);
    LogCvmfs(kLogVoms, kLogDebug, "Unable to parse VOMS file: %s\n",
             err_str);
    free(err_str);
    return NULL;
  }

  // Move pointers to returned authz_data structure
  authz_data *authz = new authz_data();
  authz->dn_ = state.m_subject;
  state.m_subject = NULL;
  if (voms_error != VERR_NOEXT) {
    authz->voms_ = state.m_voms;
    state.m_voms = NULL;
  }
  return authz;
}

*/



/**
 * For a given pid, extracts the X509_USER_PROXY path from the foreign
 * process' environment.  Stores the resulting path in the user provided buffer
 * path.
 */
/*bool CredentialsFetcher::GetProxyFileFromEnv(
  const pid_t pid,
  const size_t path_len,
  char *path)
{
  assert(path_len > 0);
  static const char * const X509_USER_PROXY = "\0X509_USER_PROXY=";
  size_t X509_USER_PROXY_LEN = strlen(X509_USER_PROXY + 1) + 1;

  if (snprintf(path, path_len, "/proc/%d/environ", pid) >=
      static_cast<int>(path_len))
  {
    if (errno == 0) {errno = ERANGE;}
    return false;
  }
  // TODO(jblomer): this shouldn't be necessary anymore.  Since this runs
  // as a separate process, at the beginning the process can acquire root
  // privileges.
  int olduid = geteuid();
  // NOTE: we ignore return values of these syscalls; this code path
  // will work if cvmfs is FUSE-mounted as an unprivileged user.
  seteuid(0);

  FILE *fp = fopen(path, "r");
  if (!fp) {
    LogCvmfs(kLogVoms, kLogSyslogErr | kLogDebug,
             "failed to open environment file for pid %d.", pid);
    seteuid(olduid);  // TODO(jblomer): remove
    return false;
  }

  // Look for X509_USER_PROXY in the environment and store the value in path
  int c = '\0';
  size_t idx = 0, key_idx = 0;
  bool set_env = false;
  while (1) {
    if (c == EOF) {break;}
    if (key_idx == X509_USER_PROXY_LEN) {
      if (idx >= path_len - 1) {break;}
      if (c == '\0') {set_env = true; break;}
      path[idx++] = c;
    } else if (X509_USER_PROXY[key_idx++] != c) {
      key_idx = 0;
    }
    c = fgetc(fp);
  }
  fclose(fp);
  seteuid(olduid);  // TODO(jblomer): remove me

  if (set_env) {path[idx] = '\0';}
  return set_env;
}
*/



/**
 * Opens a read-only file pointer to the proxy certificate as a given user.
 * The path is either taken from X509_USER_PROXY environment from the given pid
 * or it is the default location /tmp/x509up_u<UID>
 */
/*FILE *CredentialsFetcher::GetProxyFileInternal(pid_t pid, uid_t uid, gid_t gid)
{
  char path[PATH_MAX];
  if (!GetProxyFileFromEnv(pid, PATH_MAX, path)) {
    LogCvmfs(kLogVoms, kLogDebug,
             "could not find proxy in environment; using default location "
             "in /tmp/x509up_u%d.", uid);
    if (snprintf(path, PATH_MAX, "/tmp/x509up_u%d", uid) >= PATH_MAX) {
      if (errno == 0) {errno = ERANGE;}
      return NULL;
    }
  }
  LogCvmfs(kLogVoms, kLogDebug, "looking for proxy in file %s", path);

  int olduid = geteuid();  // TODO(jblomer): remove me
  int oldgid = getegid();
  // NOTE the sequencing: we must be eUID 0
  // to change the UID and GID.
  seteuid(0);
  setegid(gid);
  seteuid(uid);

  FILE *fp = fopen(path, "r");

  seteuid(0);
  setegid(oldgid);
  seteuid(olduid);

  return fp;
}


*/

/**
 * Send a the serialized form of the DN and VOMS credential
 */
/*int SendAuthzData(pid_t pid, uid_t uid, gid_t gid)
{
  authz_data* myauthz = CredentialsFetcher::GenerateVOMSData(uid, gid, pid);

  struct msghdr msg_send;
  memset(&msg_send, '\0', sizeof(msg_send));
  int command = 4;
  int result = 0;
  size_t dn_len = 0;
  size_t voms_len = 0;
  iovec iov[4];
  iov[0].iov_base = &command;
  iov[0].iov_len = sizeof(command);
  iov[1].iov_base = &result;
  iov[1].iov_len = sizeof(result);
  iov[2].iov_base = &dn_len;
  iov[2].iov_len = sizeof(dn_len);
  iov[3].iov_base = &voms_len;
  iov[3].iov_len = sizeof(voms_len);
  msg_send.msg_iov = iov;
  msg_send.msg_iovlen = 4;

  std::string mydn;
  char *buf = NULL;
  int buflen = 0;
  if (!myauthz || !myauthz->dn_) {
    result = 1;
  } else {
    mydn = myauthz->dn_;
    dn_len = mydn.size();

    if (myauthz->voms_) {
      int voms_error = 0;
      if (!(*g_VOMS_Export)(&buf, &buflen, myauthz->voms_, &voms_error)) {
        char *err_str = (*g_VOMS_ErrorMessage)(myauthz->voms_, voms_error,
                                               NULL, 0);
        LogCvmfs(kLogVoms, kLogDebug, "Unable to export VOMS credential: %s\n",
                 err_str);
        free(err_str);
        result = 2;
      }
    }
    voms_len = buflen;
  }
  delete myauthz;

  errno = 0;
  while (-1 == sendmsg(3, &msg_send, 0) && errno == EINTR) {}
  if (errno) {
    LogCvmfs(kLogVoms, kLogSyslogErr | kLogDebug,
             "failed to send authz messaage to parent: %s (errno=%d)",
             strerror(errno), errno);
    delete buf;
    return 1;
  }
  // No credential available - but sending to parent worked.
  if (result) {
    return 0;
  }

  iov[0].iov_base = &mydn[0];
  iov[0].iov_len = dn_len;
  iov[1].iov_base = buf;
  iov[1].iov_len = buflen;
  msg_send.msg_iovlen = 2;
  errno = 0;
  while (-1 == sendmsg(3, &msg_send, 0) && errno == EINTR) {}
  delete buf;
  if (errno) {
    LogCvmfs(kLogVoms, kLogSyslogErr | kLogDebug,
             "failed to send authz messaage to parent: %s (errno=%d)",
             strerror(errno), errno);
    return 1;
  }

  return 0;
}

*/

/**
 * A command-response server.  It reveices the triplet pid, uid, gid and returns
 * a read-only file descriptor for the user's proxy certificate, taking into
 * account the environment of the pid.
 */
/*int CredentialsFetcher::MainCredentialsFetcher(int argc, char *argv[]) {
  LogCvmfs(kLogVoms, kLogDebug, "starting credentials fetcher");

  // TODO(jblomer): apply logging parameters
  // int foreground = String2Int64(argv[2]);
  // int syslog_level = String2Int64(argv[3]);
  // int syslog_facility = String2Int64(argv[4]);
  // vector<string> logfiles = SplitString(argv[5], ':');

  */

  /*SetLogSyslogLevel(syslog_level);
  SetLogSyslogFacility(syslog_facility);
  if ((logfiles.size() > 0) && (logfiles[0] != ""))
    SetLogDebugFile(logfiles[0] + ".cachemgr");
  if (logfiles.size() > 1)
    SetLogMicroSyslog(logfiles[1]);

  if (!foreground)
    Daemonize();*/
/*
  while (true) {
    struct msghdr msg_recv;
    memset(&msg_recv, '\0', sizeof(msg_recv));
    int command = 0;  // smallest command id is 1
    pid_t value = 0;
    uid_t uid;
    gid_t gid;
    iovec iov[4];
    iov[0].iov_base = &command;
    iov[0].iov_len = sizeof(command);
    iov[1].iov_base = &value;
    iov[1].iov_len = sizeof(value);
    iov[2].iov_base = &uid;
    iov[2].iov_len = sizeof(uid);
    iov[3].iov_base = &gid;
    iov[3].iov_len = sizeof(gid);
    msg_recv.msg_iov = iov;
    msg_recv.msg_iovlen = 4;

    errno = 0;
    // TODO(bbockelm): Implement timeouts.
    while (-1 == recvmsg(kTransportFd, &msg_recv, 0) && errno == EINTR) {}
    if (errno) {
      LogCvmfs(kLogVoms, kLogSyslogErr | kLogDebug,
               "failed to receive messaage from child: %s (errno=%d)",
               strerror(errno), errno);
      return 1;
    }

    if (command == kCmdChildExit) {
      LogCvmfs(kLogVoms, kLogDebug,
               "got exit message from parent; exiting %d.", value);
      return value;
    } else if (command == kCmdAuthzReq) {
        int result = SendAuthzData(value, uid, gid);
        if (result) {return 1;}
        continue;
    } else if (command != kCmdCredReq) {
      LogCvmfs(kLogVoms, kLogSyslogErr | kLogDebug, "got unknown command %d",
               command);
      abort();
    }

    // Parent has requested a new credential.
    FILE *fp = GetProxyFileInternal(value, uid, gid);
    int fd;
    if (fp == NULL) {
      fd = -1;
      value = ENOENT;
      if (errno) {value = errno;}
    } else {
      fd = fileno(fp);
      value = 0;
    }
    LogCvmfs(kLogVoms, kLogDebug, "sending FD %d back to parent", fd);

    command = kCmdCredHandle;
    struct msghdr msg_send;
    memset(&msg_send, '\0', sizeof(msg_send));
    struct cmsghdr *cmsg = NULL;
    char cbuf[CMSG_SPACE(sizeof(fd))];
    msg_send.msg_iov = iov;
    msg_send.msg_iovlen = 2;

    if (fd > -1) {
      msg_send.msg_control = cbuf;
      msg_send.msg_controllen = CMSG_SPACE(sizeof(fd));
      cmsg = CMSG_FIRSTHDR(&msg_send);
      cmsg->cmsg_level = SOL_SOCKET;
      cmsg->cmsg_type  = SCM_RIGHTS;
      cmsg->cmsg_len   = CMSG_LEN(sizeof(fd));
      *(reinterpret_cast<int*>(CMSG_DATA(cmsg))) = fd;
    }

    errno = 0;
    while (-1 == sendmsg(3, &msg_send, 0) && errno == EINTR) {}
    if (errno) {
      LogCvmfs(kLogVoms, kLogSyslogErr | kLogDebug,
               "failed to send messaage to parent: %s (errno=%d)",
               strerror(errno), errno);
      return 1;
    }
    if (fp != NULL) {
      fclose(fp);
      fd = -1;
    }
  }  // command loop
}
*/