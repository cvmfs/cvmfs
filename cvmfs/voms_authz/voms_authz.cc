/**
 * This file is part of the CernVM File System.
 *
 * This file implement the interaction with VOMS library and credentials.
 */

#include "voms_authz.h"

#include <dlfcn.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <time.h>

#include <algorithm>
#include <cstring>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "fuse/fuse_lowlevel.h"
#include "voms/voms_apic.h"

#include "globus/globus_gsi_credential.h"
#include "globus/globus_gsi_cert_utils.h"
#include "globus/globus_module.h"

#include "../logging.h"
#include "../platform.h"
#include "../util.h"

// TODO(jblomer): add unit tests for static functions

extern "C" {
// VOMS API declarations
struct vomsdata * (*g_VOMS_Init)(char *voms, char *cert) = NULL;
void (*g_VOMS_Destroy)(struct vomsdata *vd) = NULL;
int (*g_VOMS_Retrieve)(X509 *cert, STACK_OF(X509) *chain, int how,
                         struct vomsdata *vd, int *error) = NULL;
char * (*g_VOMS_ErrorMessage)(struct vomsdata *vd, int error, char *buffer,
                              int len) = NULL;

// Globus API declarations

// For activating / deactivating modules.
int (*g_globus_module_activate)(
    globus_module_descriptor_t * module_descriptor) = NULL;
int (*g_globus_module_deactivate)(
    globus_module_descriptor_t * module_descriptor) = NULL;
int (*g_globus_thread_set_model)(
    const char *                        model) = NULL;
globus_object_t * (*g_globus_error_get)(
    globus_result_t                     result) = NULL;
char * (*g_globus_error_print_chain)(
    globus_object_t *                   error) = NULL;
globus_object_t *g_GLOBUS_ERROR_BASE_STATIC_PROTOTYPE = NULL;
// Modules we'll want to use.
globus_module_descriptor_t *g_globus_i_gsi_cert_utils_module = NULL;
globus_module_descriptor_t *g_globus_i_gsi_credential_module = NULL;
globus_module_descriptor_t *g_globus_i_gsi_callback_module = NULL;
globus_module_descriptor_t *g_globus_i_gsi_sysconfig_module = NULL;

// Actual functions we need to invoke.
globus_result_t (*g_globus_gsi_cred_handle_init)(
    globus_gsi_cred_handle_t *          handle,
    globus_gsi_cred_handle_attrs_t      handle_attrs) = NULL;
globus_result_t (*g_globus_gsi_cred_handle_destroy)(
    globus_gsi_cred_handle_t            handle) = NULL;
globus_result_t (*g_globus_gsi_cred_read_proxy_bio)(
    globus_gsi_cred_handle_t            handle,
    BIO *                               bio) = NULL;
globus_result_t (*g_globus_gsi_cred_get_cert)(
    globus_gsi_cred_handle_t            handle,
    X509 **                             cert) = NULL;
globus_result_t (*g_globus_gsi_cred_get_key)(
    globus_gsi_cred_handle_t            handle,
    EVP_PKEY **                         key) = NULL;
globus_result_t (*g_globus_gsi_cred_get_cert_chain)(
    globus_gsi_cred_handle_t            handle,
    STACK_OF(X509) **                   cert_chain) = NULL;
globus_result_t (*g_globus_gsi_cred_get_subject_name)(
    globus_gsi_cred_handle_t            handle,
    char **                             subject_name) = NULL;
globus_result_t (*g_globus_gsi_cred_verify_cert_chain)(
    globus_gsi_cred_handle_t            cred_handle,
    globus_gsi_callback_data_t          callback_data) = NULL;
globus_result_t (*g_globus_gsi_cert_utils_get_cert_type)(
    X509 *                              cert,
    globus_gsi_cert_utils_cert_type_t * type) = NULL;
globus_result_t (*g_globus_gsi_callback_data_init)(
    globus_gsi_callback_data_t *        callback_data) = NULL;
globus_result_t (*g_globus_gsi_callback_data_destroy)(
    globus_gsi_callback_data_t          callback_data) = NULL;
globus_result_t (*g_globus_gsi_callback_set_cert_dir)(
    globus_gsi_callback_data_t          callback_data,
    char *                              cert_dir) = NULL;
globus_result_t (*g_globus_gsi_sysconfig_get_cert_dir_unix)(
    char **                             cert_dir) = NULL;



// Although it is OK to use strcmp for 99% of DNs, there are some string rules
// in X509 about handling of multiple spaces ("  "); this will take care of
// the last 1% of cases.
int (*g_globus_i_gsi_cert_utils_dn_cmp)(
    const char *                        dn1,
    const char *                        dn2) = NULL;
bool g_globus_ok = false;  // Set to true if all globus initialization worked.
}


static bool
OpenDynLib(void **handle, const char *name, const char *friendly_name) {
  if (!handle) {return false;}
  *handle = dlopen(name, RTLD_LAZY);
  if (!(*handle)) {
    LogCvmfs(kLogVoms, kLogDebug, "Failed to load %s library; "
             "authz will not be available.  %s", friendly_name, dlerror());
  }
  return *handle;
}


static void
CloseDynLib(void **handle, const char *name) {
  if (!handle) {return;}
  if (*handle && dlclose(*handle)) {
    LogCvmfs(kLogVoms, kLogDebug, "Failed to unload %s library: %s",
                                  name, dlerror());
  }
  *handle = NULL;
}


template<typename T>
bool LoadSymbol(void *lib_handle, T *sym, const char *name) {
  if (!sym) {return false;}
  *sym = reinterpret_cast<typeof(T)>(dlsym(lib_handle, name));
  if (!(*sym)) {
    LogCvmfs(kLogVoms, kLogDebug, "Failed to load %s: %s", name, dlerror());
  }
  return *sym;
}


static void
PrintGlobusError(globus_result_t result) {
  globus_object_t *error_obj = (*g_globus_error_get)(result);
  if (error_obj == g_GLOBUS_ERROR_BASE_STATIC_PROTOTYPE) {
    LogCvmfs(kLogVoms, kLogDebug, "Globus error occurred (no further "
                                  "information available)");
    return;
  }
  char *error_full = (*g_globus_error_print_chain)(error_obj);
  if (!error_full) {
    LogCvmfs(kLogVoms, kLogDebug, "Globus error occurred (error unprintable)");
    return;
  }
  LogCvmfs(kLogVoms, kLogDebug, "Globus error: %s", error_full);
  free(error_full);
}


// TODO(jblomer): add comment
// TODO(jblomer): unit test the class
// TODO(jblomer): Member functions should start with a capital letter
// TODO(jblomer): put into anonymous namespace
// TODO(jblomer): replace pthread_mutex_lock by MutexGuard
// TODO(jblomer): member naming: trailing underscore, no m_ prefix
class AuthzSessionCache {

  AuthzSessionCache()
    : m_zombie(true),
      m_last_clean(platform_monotonic_time()),
      m_globus_module_handle(NULL),
      m_globus_gsi_cert_utils_handle(NULL),
      m_globus_gsi_credential_handle(NULL),
      m_globus_gsi_callback_handle(NULL)
  {
    pthread_mutex_init(&m_mutex, NULL);
    load_voms_library();
    if (!m_zombie) {
      m_zombie = true;
      load_globus_library();
    }
    LogCvmfs(kLogVoms, kLogDebug|kLogSyslog, "Support for authz is %senabled.",
      m_zombie ? "NOT " : "");
  }

  ~AuthzSessionCache() {
    for (KeyToVOMS::const_iterator it = m_map.begin();
         it != m_map.end();
         it++)
    {
      (*g_VOMS_Destroy)(it->second.first);
    }
    close_voms_library();
    close_globus_library();
    pthread_mutex_destroy(&m_mutex);
  }

 public:
  // TODO(jblomer): change type of vomsdata
  bool get(pid_t pid, struct vomsdata *&vomsinfo) {  // NOLINT
    if (m_zombie) {return false;}
    time_t now = platform_monotonic_time();
    KeyType mykey;
    if (!lookup(pid, mykey)) {return false;}
    LogCvmfs(kLogVoms, kLogDebug, "PID %d maps to session %d, UID %d, "
             "GID %d, bday %llu", pid, mykey.pid, mykey.uid, mykey.gid,
             mykey.bday);
    pthread_mutex_lock(&m_mutex);
    // TODO(jblomer): remove magic number
    if (now > m_last_clean + 100) {clean_tables();}
    KeyToVOMS::const_iterator iter = m_map.find(mykey);
    pthread_mutex_unlock(&m_mutex);
    if (iter == m_map.end()) {
      return false;
    }
    LogCvmfs(kLogVoms, kLogDebug, "Session %d, UID %d, GID %d, bday %llu "
             "has cached VOMS data", mykey.pid, mykey.uid, mykey.gid,
             mykey.bday);
    vomsinfo = iter->second.first;
    return true;
  }

  /**
   * Put the VOMS data into the map.  Map owns the pointer after this returns.
   * This function is thread safe.
   *
   * The returned pointer is the data in the map which is possibly not the
   * input vomsdata.
   *
   * If result is NULL, then the operation failed and errno is set.
   */
  struct vomsdata *try_put(pid_t pid, struct vomsdata *voms_ptr)
  {
    if (m_zombie) {return NULL;}
    KeyType mykey;
    if (!lookup(pid, mykey)) {
      LogCvmfs(kLogVoms, kLogDebug, "Failed to determine session key "
               "for PID %d.", pid);
      return NULL;
    }
    time_t now = platform_monotonic_time();

    pthread_mutex_lock(&m_mutex);
    std::pair<KeyToVOMS::iterator, bool> result =
      m_map.insert(std::make_pair(mykey, std::make_pair(voms_ptr, now)));
    pthread_mutex_unlock(&m_mutex);
    if (!result.second)
    {
      (*g_VOMS_Destroy)(voms_ptr);
    }
    LogCvmfs(kLogVoms, kLogDebug, "Cached VOMS data for session %d, "
             "UID %d, GID %d, bday %llu.", mykey.pid, mykey.uid,
             mykey.gid, mykey.bday);
    return result.first->second.first;
  }

  static AuthzSessionCache &GetInstance()
  {
    return g_cache;
  }

 private:
  struct KeyType {
    KeyType() : pid(-1), uid(-1), gid(-1), bday(0) {}
    // TODO(jblomer): replace type unsigned long long
    KeyType(pid_t p, uid_t u, gid_t g, unsigned long long b)  // NOLINT
     : pid(p), uid(u), gid(g), bday(b) {}

    bool operator< (const KeyType & other) const {
      return (pid < other.pid) ||
             ((pid == other.pid) && ((uid < other.uid) ||
               ((uid == other.uid) && ((gid < other.gid) ||
                 ((gid == other.gid) && (bday < other.bday))))));
    }

    pid_t pid;
    uid_t uid;
    gid_t gid;
    unsigned long long bday;  // NOLINT
  };
  typedef std::pair<struct vomsdata*, time_t> ValueType;
  typedef std::map<KeyType, ValueType> KeyToVOMS;

  typedef std::map<KeyType, KeyType> PidToSid;
  PidToSid m_pid_map;

  AuthzSessionCache(const AuthzSessionCache&);

  // Lookup pid's sid and birthdy.
  // Returns false on failure and sets errno.
  // TODO(jblomer): change reference to pointer
  bool lookup(pid_t pid, KeyType &mykey) {  // NOLINT
    char pidpath[PATH_MAX];
    if (snprintf(pidpath, PATH_MAX, "/proc/%d/stat", pid) >= PATH_MAX) {
      errno = ERANGE;
      return false;
    }
    FILE *fp;
    if (NULL == (fp = fopen(pidpath, "r"))) {
      LogCvmfs(kLogVoms, kLogDebug, "Failed to open status file.");
      return false;
    }

    int fd = fileno(fp);
    struct stat st;
    if (-1 == fstat(fd, &st)) {
      fclose(fp);
      LogCvmfs(kLogVoms, kLogDebug, "Failed to get stat information of "
               "running process.");
      return false;
    }
    uid_t uid = st.st_uid;
    gid_t gid = st.st_gid;

    pid_t sid;
    // TODO(jblomer): make this a function in platform
    unsigned long long birthday;  // NOLINT
    int result;
    // TODO(bbockelm): EINTR handling
    result = fscanf(fp, "%*d %*s %*c %*d %*d %d %*d %*d %*u %*u %*u %*u "
                    "%*u %*u %*u %*d %*d %*d %*d %*d %*d %llu", &sid,
                    &birthday);
    fclose(fp);
    if (result != 2) {
      if (errno == 0) {errno = EINVAL;}
      LogCvmfs(kLogVoms, kLogDebug, "Failed to parse status file for "
               "pid %d: (errno=%d) %s, fscanf result %d", pid, errno,
               strerror(errno), result);
      return false;
    }

    KeyType pidkey(pid, uid, gid, birthday);

    pthread_mutex_lock(&m_mutex);
    PidToSid::iterator it = m_pid_map.find(pidkey);
    pthread_mutex_unlock(&m_mutex);
    if (it == m_pid_map.end()) {
      if (snprintf(pidpath, PATH_MAX, "/proc/%d/stat", sid) >= PATH_MAX) {
        errno = ERANGE;
        return false;
      }
      if (NULL == (fp = fopen(pidpath, "r"))) {
        LogCvmfs(kLogVoms, kLogDebug,
                 "Failed to open session's status file.");
        return false;
      }
      result = fscanf(fp, "%*d %*s %*c %*d %*d %d %*d %*d %*u %*u %*u "
                      "%*u %*u %*u %*u %*d %*d %*d %*d %*d %*d %llu",
                      &sid, &birthday);
      fclose(fp);
      if (result != 2) {
        if (errno == 0) {errno = EINVAL;}
        LogCvmfs(kLogVoms, kLogDebug, "Failed to parse status file "
                 "for sid %d: (errno=%d) %s, fscanf result %d",
                 pid, errno, strerror(errno), result);
        return false;
      }
      mykey.pid = sid;
      mykey.bday = birthday;
      mykey.uid = uid;
      mykey.gid = gid;
      pthread_mutex_lock(&m_mutex);
      m_pid_map.insert(std::make_pair(pidkey, mykey));
      pthread_mutex_unlock(&m_mutex);
    } else {
      mykey = it->second;
    }

    LogCvmfs(kLogVoms, kLogDebug, "Lookup key; sid=%d, bday=%llu",
             sid, birthday);
    return true;
  }

  /**
   * MUST CALL LOCKED
   */
  void clean_tables() {
    LogCvmfs(kLogVoms, kLogDebug, "Expiring VOMS credential tables.");
    m_pid_map.clear();
    m_last_clean = platform_monotonic_time();
    // TODO(jblomer): remove magic number
    time_t expiry = m_last_clean + 100;
    KeyToVOMS::iterator it = m_map.begin();
    while (it != m_map.end()) {
      if (it->second.second < expiry) {
        (*g_VOMS_Destroy)(it->second.first);
        m_map.erase(it++);
      } else {
        ++it;
      }
    }
  }

  void load_voms_library() {
    if (!OpenDynLib(&m_libvoms_handle, "libvomsapi.so.1", "VOMS")) {return;}
    if (
       !LoadSymbol(m_libvoms_handle, &g_VOMS_Init, "VOMS_Init") ||
       !LoadSymbol(m_libvoms_handle, &g_VOMS_Destroy, "VOMS_Destroy") ||
       !LoadSymbol(m_libvoms_handle, &g_VOMS_Retrieve, "VOMS_Retrieve") ||
       !LoadSymbol(m_libvoms_handle, &g_VOMS_Destroy, "VOMS_Destroy") ||
       !LoadSymbol(m_libvoms_handle, &g_VOMS_ErrorMessage, "VOMS_ErrorMessage")
      ) {
        g_VOMS_Init = NULL;
        return;
    }
    LogCvmfs(kLogVoms, kLogDebug, "Successfully loaded VOMS library");
    m_zombie = false;
  }


  void load_globus_library() {
    if (!OpenDynLib(&m_globus_module_handle, "libglobus_common.so.0",
                    "Globus common")) {return;}
    if (!OpenDynLib(&m_globus_gsi_cert_utils_handle,
                    "libglobus_gsi_cert_utils.so.0",
                    "Globus GSI cert utils")) {return;}
    if (!OpenDynLib(&m_globus_gsi_credential_handle,
                    "libglobus_gsi_credential.so.1",
                    "Globus GSI credential")) {return;}
    if (!OpenDynLib(&m_globus_gsi_callback_handle,
                    "libglobus_gsi_callback.so.0",
                    "Globus GSI callback")) {return;}
    if (!OpenDynLib(&m_globus_gsi_sysconfig_handle,
                    "libglobus_gsi_sysconfig.so.1",
                    "Globus GSI sysconfig")) {return;}
    if (
       !LoadSymbol(m_globus_module_handle, &g_globus_module_activate,
                   "globus_module_activate") ||
       !LoadSymbol(m_globus_module_handle, &g_globus_module_deactivate,
                   "globus_module_deactivate") ||
       !LoadSymbol(m_globus_module_handle, &g_globus_thread_set_model,
                   "globus_thread_set_model") ||
       !LoadSymbol(m_globus_module_handle, &g_globus_error_get,
                   "globus_error_get") ||
       !LoadSymbol(m_globus_module_handle, &g_globus_error_print_chain,
                   "globus_error_print_chain") ||
       !LoadSymbol(m_globus_module_handle,
                   &g_GLOBUS_ERROR_BASE_STATIC_PROTOTYPE,
                   "GLOBUS_ERROR_BASE_STATIC_PROTOTYPE") ||
       !LoadSymbol(m_globus_gsi_cert_utils_handle,
                   &g_globus_i_gsi_cert_utils_module,
                   "globus_i_gsi_cert_utils_module") ||
       !LoadSymbol(m_globus_gsi_credential_handle,
                   &g_globus_i_gsi_credential_module,
                   "globus_i_gsi_credential_module") ||
       !LoadSymbol(m_globus_gsi_callback_handle,
                   &g_globus_i_gsi_callback_module,
                   "globus_i_gsi_callback_module") ||
       !LoadSymbol(m_globus_gsi_sysconfig_handle,
                   &g_globus_i_gsi_sysconfig_module,
                   "globus_i_gsi_sysconfig_module") ||
       !LoadSymbol(m_globus_gsi_credential_handle,
                   &g_globus_gsi_cred_handle_init,
                   "globus_gsi_cred_handle_init") ||
       !LoadSymbol(m_globus_gsi_credential_handle,
                   &g_globus_gsi_cred_handle_destroy,
                   "globus_gsi_cred_handle_destroy") ||
       !LoadSymbol(m_globus_gsi_credential_handle,
                   &g_globus_gsi_cred_read_proxy_bio,
                   "globus_gsi_cred_read_proxy_bio") ||
       !LoadSymbol(m_globus_gsi_credential_handle,
                   &g_globus_gsi_cred_verify_cert_chain,
                   "globus_gsi_cred_verify_cert_chain") ||
       !LoadSymbol(m_globus_gsi_credential_handle,
                   &g_globus_gsi_cred_get_cert,
                   "globus_gsi_cred_get_cert") ||
       !LoadSymbol(m_globus_gsi_credential_handle,
                   &g_globus_gsi_cred_get_key,
                   "globus_gsi_cred_get_key") ||
       !LoadSymbol(m_globus_gsi_credential_handle,
                   &g_globus_gsi_cred_get_cert_chain,
                   "globus_gsi_cred_get_cert_chain") ||
       !LoadSymbol(m_globus_gsi_credential_handle,
                   &g_globus_gsi_cred_get_subject_name,
                   "globus_gsi_cred_get_subject_name") ||
       !LoadSymbol(m_globus_gsi_cert_utils_handle,
                   &g_globus_gsi_cert_utils_get_cert_type,
                   "globus_gsi_cert_utils_get_cert_type") ||
       !LoadSymbol(m_globus_gsi_cert_utils_handle,
                   &g_globus_i_gsi_cert_utils_dn_cmp,
                   "globus_i_gsi_cert_utils_dn_cmp") ||
       !LoadSymbol(m_globus_gsi_callback_handle,
                   &g_globus_gsi_callback_data_init,
                   "globus_gsi_callback_data_init") ||
       !LoadSymbol(m_globus_gsi_callback_handle,
                   &g_globus_gsi_callback_data_destroy,
                   "globus_gsi_callback_data_destroy") ||
       !LoadSymbol(m_globus_gsi_callback_handle,
                   &g_globus_gsi_callback_set_cert_dir,
                   "globus_gsi_callback_set_cert_dir") ||
       !LoadSymbol(m_globus_gsi_sysconfig_handle,
                   &g_globus_gsi_sysconfig_get_cert_dir_unix,
                   "globus_gsi_sysconfig_get_cert_dir_unix")
      ) {return;}
    if (GLOBUS_SUCCESS !=
        (g_globus_thread_set_model)("none")) {
      LogCvmfs(kLogVoms, kLogDebug, "Failed to enable Globus thread model.");
      return;
    }
    if (GLOBUS_SUCCESS !=
        (*g_globus_module_activate)(g_globus_i_gsi_cert_utils_module)) {
      LogCvmfs(kLogVoms, kLogDebug, "Failed to activate Globus GSI cert utils"
                                    " module.");
      return;
    }
    if (GLOBUS_SUCCESS !=
        (*g_globus_module_activate)(g_globus_i_gsi_credential_module)) {
      LogCvmfs(kLogVoms, kLogDebug, "Failed to activate Globus GSI credential"
                                    " module.");
      (*g_globus_module_deactivate)(g_globus_i_gsi_cert_utils_module);
      return;
    }
    if (GLOBUS_SUCCESS !=
        (*g_globus_module_activate)(g_globus_i_gsi_callback_module)) {
      (*g_globus_module_deactivate)(g_globus_i_gsi_cert_utils_module);
      (*g_globus_module_deactivate)(g_globus_i_gsi_credential_module);
      return;
    }
    if (GLOBUS_SUCCESS !=
        (*g_globus_module_activate)(g_globus_i_gsi_sysconfig_module)) {
      (*g_globus_module_deactivate)(g_globus_i_gsi_cert_utils_module);
      (*g_globus_module_deactivate)(g_globus_i_gsi_credential_module);
      (*g_globus_module_deactivate)(g_globus_i_gsi_callback_module);
    }
    LogCvmfs(kLogVoms, kLogDebug, "Successfully loaded Globus library");
    m_zombie = false;
    g_globus_ok = true;
  }


  void close_voms_library() {
    CloseDynLib(&m_libvoms_handle, "VOMS");
    g_VOMS_Init = NULL;
    g_VOMS_Destroy = NULL;
    g_VOMS_Retrieve = NULL;
    g_VOMS_ErrorMessage = NULL;
  }


  void close_globus_library() {
    if (g_globus_ok) {
      // TODO(bbockelm): Does not mix well with fork; re-enable but add fork-handlers.
      (*g_globus_module_deactivate)(g_globus_i_gsi_cert_utils_module);
      (*g_globus_module_deactivate)(g_globus_i_gsi_credential_module);
      (*g_globus_module_deactivate)(g_globus_i_gsi_callback_module);
      (*g_globus_module_deactivate)(g_globus_i_gsi_sysconfig_module);
    }
    CloseDynLib(&m_globus_module_handle, "Globus module");
    CloseDynLib(&m_globus_gsi_cert_utils_handle, "Globus GSI cert utils");
    CloseDynLib(&m_globus_gsi_credential_handle, "Globus GSI credential");
    CloseDynLib(&m_globus_gsi_callback_handle, "Globus GSI callback");
    CloseDynLib(&m_globus_gsi_sysconfig_handle, "Globus GSI sysconfig");
    g_globus_thread_set_model = NULL;
    g_globus_module_activate = NULL;
    g_globus_module_deactivate = NULL;
    g_globus_error_get = NULL;
    g_globus_error_print_chain = NULL;
    g_GLOBUS_ERROR_BASE_STATIC_PROTOTYPE = NULL;
    g_globus_i_gsi_cert_utils_module = NULL;
    g_globus_i_gsi_credential_module = NULL;
    g_globus_i_gsi_callback_module = NULL;
    g_globus_i_gsi_sysconfig_module = NULL;
    g_globus_gsi_cred_handle_init = NULL;
    g_globus_gsi_cred_handle_destroy = NULL;
    g_globus_gsi_cred_get_key = NULL;
    g_globus_gsi_cred_verify_cert_chain = NULL;
    g_globus_gsi_cred_get_subject_name = NULL;
    g_globus_gsi_cert_utils_get_cert_type = NULL;
    g_globus_i_gsi_cert_utils_dn_cmp = NULL;
    g_globus_gsi_callback_data_init = NULL;
    g_globus_gsi_callback_data_destroy = NULL;
    g_globus_gsi_callback_set_cert_dir = NULL;
    g_globus_gsi_sysconfig_get_cert_dir_unix = NULL;
    g_globus_ok = false;
  }


  bool m_zombie;
  time_t m_last_clean;
  pthread_mutex_t m_mutex;

  KeyToVOMS m_map;

  // Various library handles.
  void *m_libvoms_handle;
  void *m_globus_module_handle;
  void *m_globus_gsi_cert_utils_handle;
  void *m_globus_gsi_credential_handle;
  void *m_globus_gsi_callback_handle;
  void *m_globus_gsi_sysconfig_handle;

  static AuthzSessionCache g_cache;
};


AuthzSessionCache AuthzSessionCache::g_cache;


static FILE *
GetProxyFile(const struct fuse_ctx *ctx) {
  return GetProxyFile(ctx->pid, ctx->uid, ctx->gid);
}


/**
 * Create the VOMS data structure to the best of our abilities.
 *
 * Resulting memory is owned by caller and must be destroyed with VOMS_Destroy.
 */
struct authz_state {
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


struct authz_data {
  struct vomsdata *voms_;
  char *dn_;

  authz_data() :
    voms_(NULL),
    dn_(NULL)
  {}

  ~authz_data() {
    if (voms_) {(*g_VOMS_Destroy)(voms_);}
    if (dn_) {OPENSSL_free(dn_);}
  }
};


static authz_data*
GenerateVOMSData(const struct fuse_ctx *ctx)
{
  authz_state state;

  state.m_fp = GetProxyFile(ctx);
  if (!state.m_fp) {
    LogCvmfs(kLogVoms, kLogDebug, "Could not find process's proxy file.");
    return NULL;
  }

  // Start of Globus proxy parsing and verification...
  globus_result_t result =
      (*g_globus_gsi_cred_handle_init)(&state.m_cred, NULL);
  if (GLOBUS_SUCCESS != result) {
    PrintGlobusError(result);
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
    PrintGlobusError(result);
    return NULL;
  }

  // Setup Globus callback object.
  result = (*g_globus_gsi_callback_data_init)(&state.m_callback);
  if (GLOBUS_SUCCESS != result) {
    PrintGlobusError(result);
    return NULL;
  }
  char *cert_dir;
  result = (*g_globus_gsi_sysconfig_get_cert_dir_unix)(&cert_dir);
  if (GLOBUS_SUCCESS != result) {
    LogCvmfs(kLogVoms, kLogDebug, "Failed to determine trusted certificates "
                                  "directory.");
    PrintGlobusError(result);
    return NULL;
  }
  result = (*g_globus_gsi_callback_set_cert_dir)(state.m_callback, cert_dir);
  free(cert_dir);
  if (GLOBUS_SUCCESS != result) {
    PrintGlobusError(result);
    return NULL;
  }

  // Verify credential chain.
  result = (*g_globus_gsi_cred_verify_cert_chain)(state.m_cred,
                                                  state.m_callback);
  if (GLOBUS_SUCCESS != result) {
    LogCvmfs(kLogVoms, kLogDebug, "Failed to validate credentials");
    PrintGlobusError(result);
    return NULL;
  }

  // Load key and certificate from Globus handle
  result = (*g_globus_gsi_cred_get_key)(state.m_cred, &state.m_pkey);
  if (GLOBUS_SUCCESS != result) {
    LogCvmfs(kLogVoms, kLogDebug, "Failed to get process private key.");
    PrintGlobusError(result);
    return NULL;
  }
  result = (*g_globus_gsi_cred_get_cert)(state.m_cred, &state.m_cert);
  if (GLOBUS_SUCCESS != result) {
    LogCvmfs(kLogVoms, kLogDebug, "Failed to get process certificate.");
    PrintGlobusError(result);
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
    PrintGlobusError(result);
    return NULL;
  }

  result = (*g_globus_gsi_cred_get_subject_name)(state.m_cred, &state.m_subject);
  if (GLOBUS_SUCCESS != result) {
    PrintGlobusError(result);
    return NULL;
  }

  state.m_voms = (*g_VOMS_Init)(NULL, NULL);
  if (!state.m_voms) {
    return NULL;
  }

  int error = 0;
  const int retval = (*g_VOMS_Retrieve)(state.m_cert, state.m_chain, RECURSE_CHAIN,
                                        state.m_voms, &error);
  if (!retval) {
    char *err_str = (*g_VOMS_ErrorMessage)(state.m_voms, error, NULL, 0);
    LogCvmfs(kLogVoms, kLogDebug, "Unable to parse VOMS file: %s\n",
             err_str);
    free(err_str);
    return NULL;
  }

  // Move pointers to returned authz_data structure
  authz_data *authz = new authz_data();
  authz->voms_ = state.m_voms;
  authz->dn_ = state.m_subject;
  state.m_voms = NULL;
  state.m_subject = NULL;
  return authz;
}


// TODO(jblomer): can probably be replaced by SplitString from util
// TODO(jblomer): hierarchy should be a pointer
static void
SplitGroupToPaths(const std::string &group,
                  std::vector<std::string> &hierarchy)  // NOLINT
{
  size_t start = 0, end = 0;
  while ((end = group.find('/', start)) != std::string::npos) {
    if (end-start) {
      hierarchy.push_back(group.substr(start, end-start));
    }
    start = end+1;
  }
  if (start != group.size()-1) {hierarchy.push_back(group.substr(start));}
}


static bool
IsSubgroupOf(const std::vector<std::string> &group1,
             const std::vector<std::string> &group2)
{
  if (group1.size() < group2.size()) {
    return false;
  }
  std::vector<std::string>::const_iterator it1 = group1.begin();
  for (std::vector<std::string>::const_iterator it2 = group2.begin();
       it2 != group2.end();
       it1++, it2++)
  {
    if (*it1 != *it2) {return false;}
  }
  return true;
}


static bool
IsRoleMatching(const char *role1, const char *role2) {
  if ((role2 == NULL) || (strlen(role2) == 0) ||
      !strcmp(role2, "NULL"))
  {
    return true;
  }
  if ((role1 == NULL) || !strcmp(role1, "NULL")) {return false;}

  return !strcmp(role1, role2);
}


static bool CheckSingleAuthz(const struct vomsdata *voms_ptr,
                             const std::string & authz);


static bool CheckMultipleAuthz(const struct vomsdata *voms_ptr,
                               const std::string  &authz_list);


bool CheckVOMSAuthz(const struct fuse_ctx *ctx, const std::string & authz) {
  if (g_VOMS_Init == NULL || !g_globus_ok) {
    LogCvmfs(kLogVoms, kLogSyslog | kLogDebug,
             "VOMS library not present; failing VOMS authz.");
    return false;
  }
  LogCvmfs(kLogVoms, kLogDebug,
           "Checking whether user with UID %d has VOMS auth %s.",
           ctx->uid, authz.c_str());

  // Get VOMS information from cache; if not present, store it.
  struct vomsdata *voms_ptr;
  if (!AuthzSessionCache::GetInstance().get(ctx->pid, voms_ptr)) {
    authz_data *authz = GenerateVOMSData(ctx);
    voms_ptr = authz ? authz->voms_ : NULL;
    if (voms_ptr) {
      voms_ptr = AuthzSessionCache::GetInstance().try_put(ctx->pid, voms_ptr);
      LogCvmfs(kLogVoms, kLogDebug,
               "Caching user's VOMS credentials at address %p.", voms_ptr);
    } else {
      LogCvmfs(kLogVoms, kLogSyslog | kLogDebug,
               "User has no VOMS credentials.");
      return false;
    }
  } else {
    LogCvmfs(kLogVoms, kLogDebug, "Using cached VOMS credentials.");
  }
  if (!voms_ptr) {
    LogCvmfs(kLogVoms, kLogSyslog | kLogDebug,
             "ERROR: Failed to generate VOMS data.");
    return false;
  }
  return CheckMultipleAuthz(voms_ptr, authz);
}


static bool
CheckMultipleAuthz(const struct vomsdata *voms_ptr,
                   const std::string &authz_list)
{
  // Check all authorizations against our information
  size_t last_delim = 0;
  size_t delim = authz_list.find('\n');
  while (delim != std::string::npos) {
    std::string next_authz = authz_list.substr(last_delim, delim-last_delim);
    last_delim = delim + 1;
    delim = authz_list.find('\n', last_delim);

    if (CheckSingleAuthz(voms_ptr, next_authz)) {return true;}
  }
  std::string next_authz = authz_list.substr(last_delim);
  return CheckSingleAuthz(voms_ptr, next_authz);
}


static bool
CheckSingleAuthz(const struct vomsdata *voms_ptr, const std::string & authz)
{
  // An empty entry should authorize nobody.
  if (authz.empty()) {return false;}

  // Break the authz into VOMS VO, groups, and roles.
  // We will compare the required auth against the cached session VOMS info.
  // Roles must match exactly; Sub-groups are authorized in their parent
  // group.
  // TODO(jblomer): move to a unit testable function
  std::string vo, role, group;
  bool is_dn = false;
  if (authz[0] != '/') {
        size_t delim = authz.find(':');
        if (delim != std::string::npos) {
            vo = authz.substr(0, delim);
            size_t delim2 = authz.find("/Role=", delim+1);
            if (delim2 != std::string::npos) {
                role = authz.substr(delim2 + 6);
                group = authz.substr(delim + 1, delim2 - delim - 1);
            } else {
                group = authz.substr(delim + 1);;
            }
        }
  } else {
        // No VOMS info in the authz; it is a DN.
        is_dn = true;
  }
  // Quick sanity check of group name.
  if (!group.empty() && group[0] != '/') {return false;}

  std::vector<std::string> group_hierarchy;
  SplitGroupToPaths(group, group_hierarchy);

  // Now we have valid VOMS data, check authz.
  // Iterator through the VOs
  for (int idx=0; voms_ptr->data[idx] != NULL; idx++) {
    struct voms *it = voms_ptr->data[idx];
    // Check first against the full DN.
    if (is_dn) {
      if (it->user && !strcmp(it->user, authz.c_str())) {
        return true;
      } else {
        break;
      }
    }
    if (!it->voname) {continue;}
    if (strcmp(vo.c_str(), it->voname)) {continue;}

    // Iterate through the FQANs.
    for (int idx2=0; it->std[idx2] != NULL; idx2++) {
      struct data *it2 = it->std[idx2];
      if (!it2->group) {continue;}
      LogCvmfs(kLogVoms, kLogDebug, "Checking (%s Role=%s) against group"
               " %s, role %s.", group.c_str(), role.c_str(), it2->group,
               it2->role);
      std::vector<std::string> avail_hierarchy;
      SplitGroupToPaths(it2->group, avail_hierarchy);
      if (IsSubgroupOf(avail_hierarchy, group_hierarchy) &&
              IsRoleMatching(it2->role, role.c_str()))
      {
        return true;
      }
    }
  }

  return false;
}
