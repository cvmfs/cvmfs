
/**
 * This file is part of the CernVM File System.
 *
 * This file implement the interaction with VOMS library
 * and credentials.
 */

#include "voms_authz.h"

#include <dlfcn.h>
#include <errno.h>
#include <limits.h>
#include <time.h>

#include <algorithm>
#include <cstring>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "fuse/fuse_lowlevel.h"
#include "voms/voms_apic.h"

#include "../logging.h"

static time_t get_mono_time()
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);
    return ts.tv_sec;
}

extern "C" {
void (*g_VOMS_Destroy)(struct vomsdata *vd) = NULL;
struct vomsdata * (*g_VOMS_Init)(char *voms, char *cert) = NULL;
int (*g_VOMS_RetrieveFromFile)(FILE *file, int how, struct vomsdata *vd,
                               int *error) = NULL;
char * (*g_VOMS_ErrorMessage)(struct vomsdata *vd, int error, char *buffer,
                              int len) = NULL;
}

class VOMSSessionCache {
 public:
    VOMSSessionCache()
      : m_zombie(true),
        m_last_clean(get_mono_time())
    {
        pthread_mutex_init(&m_mutex, NULL);
        load_voms_library();
    }

    ~VOMSSessionCache() {
        for (KeyToVOMS::const_iterator it = m_map.begin();
             it != m_map.end();
             it++)
        {
            (*g_VOMS_Destroy)(it->second.first);
        }
        close_voms_library();
        pthread_mutex_destroy(&m_mutex);
    }

    bool get(pid_t pid, struct vomsdata *&vomsinfo) {  // NOLINT
        if (m_zombie) {return false;}
        time_t now = get_mono_time();
        KeyType mykey;
        if (!lookup(pid, mykey)) {return false;}
        LogCvmfs(kLogVoms, kLogDebug, "PID %d maps to session %d, UID %d, "
                 "GID %d, bday %llu", pid, mykey.pid, mykey.uid, mykey.gid,
                 mykey.bday);
        pthread_mutex_lock(&m_mutex);
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

    // Put the VOMS data into the map.  Map owns the pointer
    // after this returns.  This function is thread safe.
    //
    // The returned pointer is the data in the map which is possibly
    // not the input vomsdata.
    //
    // If result is NULL, then the operation failed and errno is set.
    struct vomsdata *try_put(pid_t pid, struct vomsdata *voms_ptr)
    {
        if (m_zombie) {return false;}
        KeyType mykey;
        if (!lookup(pid, mykey))
        {
            LogCvmfs(kLogVoms, kLogDebug, "Failed to determine session key "
                     "for PID %d.", pid);
            return false;
        }
        time_t now = get_mono_time();

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

 private:
    struct KeyType {
      KeyType() : pid(-1), uid(-1), gid(-1), bday(0) {}
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

    // Lookup pid's sid and birthdy.
    // Returns false on failure and sets errno.
    bool lookup(pid_t pid, KeyType &mykey)  // NOLINT
    {
        char pidpath[PATH_MAX];
        if (snprintf(pidpath, PATH_MAX, "/proc/%d/stat", pid) >= PATH_MAX)
        {
            errno = ERANGE;
            return false;
        }
        FILE *fp;
        if (NULL == (fp = fopen(pidpath, "r")))
        {
            LogCvmfs(kLogVoms, kLogDebug, "Failed to open status file.");
            return false;
        }

        int fd = fileno(fp);
        struct stat st;
        if (-1 == fstat(fd, &st))
        {
            fclose(fp);
            LogCvmfs(kLogVoms, kLogDebug, "Failed to get stat information of "
                     "running process.");
            return false;
        }
        uid_t uid = st.st_uid;
        gid_t gid = st.st_gid;

        pid_t sid;
        unsigned long long birthday;  // NOLINT
        int result;
        // TODO(bbockelm): EINTR handling
        result = fscanf(fp, "%*d %*s %*c %*d %*d %d %*d %*d %*u %*u %*u %*u "
                        "%*u %*u %*u %*d %*d %*d %*d %*d %*d %llu", &sid,
                        &birthday);
        fclose(fp);
        if (result != 2)
        {
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
        if (it == m_pid_map.end())
        {
            if (snprintf(pidpath, PATH_MAX, "/proc/%d/stat",
                         sid) >= PATH_MAX)
            {
              errno = ERANGE;
              return false;
            }
            if (NULL == (fp = fopen(pidpath, "r"))) {
              LogCvmfs(kLogVoms, kLogDebug, "Failed to open session's status "
                       "file.");
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

        LogCvmfs(kLogVoms, kLogDebug, "Lookup key; sid=%d, bday=%llu", sid,
                 birthday);
        return true;
    }

    // MUST CALL LOCKED
    void clean_tables() {
        LogCvmfs(kLogVoms, kLogDebug, "Expiring VOMS credential tables.");
        m_pid_map.clear();
        m_last_clean = get_mono_time();
        time_t expiry = m_last_clean + 100;
        KeyToVOMS::iterator it = m_map.begin();
        while (it != m_map.end())
        {
            if (it->second.second < expiry)
            {
              (*g_VOMS_Destroy)(it->second.first);
              m_map.erase(it++);
            } else {
              ++it;
            }
        }
    }

    void load_voms_library()
    {
        m_libvoms_handle = dlopen("libvomsapi.so.1", RTLD_LAZY);
        if (!m_libvoms_handle)
        {
            LogCvmfs(kLogVoms, kLogDebug, "Failed to load VOMS library; VOMS "
                     "authz will not be available.  %s", dlerror());
            return;
        }
        if (!(g_VOMS_Init =
              reinterpret_cast<struct vomsdata*(*)(char *, char *)>
                (dlsym(m_libvoms_handle, "VOMS_Init"))))
        {
            LogCvmfs(kLogVoms, kLogDebug, "Failed to load VOMS_Init from VOMS "
                     "library: %s", dlerror());
            return;
        }
        if (!(g_VOMS_Destroy =
              reinterpret_cast<void (*)(struct vomsdata *vd)>
              (dlsym(m_libvoms_handle, "VOMS_Destroy"))))
        {
            LogCvmfs(kLogVoms, kLogDebug, "Failed to load VOMS_Destroy from "
                     "VOMS library: %s", dlerror());
            return;
        }
        if (!(g_VOMS_RetrieveFromFile =
              reinterpret_cast<int (*)(FILE *file, int how, struct vomsdata *vd,
                                       int *error)>
              (dlsym(m_libvoms_handle, "VOMS_RetrieveFromFile"))))
        {
            LogCvmfs(kLogVoms, kLogDebug, "Failed to load VOMS_RetrieveFromFile"
                     " from VOMS library: %s", dlerror());
            return;
        }
        if (!(g_VOMS_ErrorMessage =
              reinterpret_cast<char * (*)(struct vomsdata *vd, int error,
                                          char *buffer, int len)>
              (dlsym(m_libvoms_handle, "VOMS_ErrorMessage"))))
        {
            LogCvmfs(kLogVoms, kLogDebug, "Failed to load VOMS_ErrorMessage "
                     "from VOMS library: %s", dlerror());
            return;
        }
        LogCvmfs(kLogVoms, kLogDebug, "Successfully loaded VOMS library; VOMS "
                 "authz will be available.");
        m_zombie = false;
    }

    void close_voms_library()
    {
        if (m_libvoms_handle && dlclose(m_libvoms_handle))
        {
            LogCvmfs(kLogVoms, kLogDebug, "Failed to unload VOMS library: %s",
                     dlerror());
        }
        m_libvoms_handle = NULL;
        g_VOMS_Init = NULL;
        g_VOMS_Destroy = NULL;
        g_VOMS_RetrieveFromFile = NULL;
        g_VOMS_ErrorMessage = NULL;
    }

    bool m_zombie;
    time_t m_last_clean;
    pthread_mutex_t m_mutex;

    KeyToVOMS m_map;

    void *m_libvoms_handle;
};


static VOMSSessionCache g_cache;


static FILE *
GetProxyFile(const struct fuse_ctx *ctx)
{
    return GetProxyFile(ctx->pid, ctx->uid, ctx->gid);
}


/*
 * Create the VOMS data structure to the best of our abilities.
 *
 * Resulting memory is owned by caller and must be destroyed with VOMS_Destroy.
 */
static struct vomsdata*
GenerateVOMSData(const struct fuse_ctx *ctx)
{
    FILE *fp = GetProxyFile(ctx);
    if (!fp) {
        LogCvmfs(kLogVoms, kLogDebug, "Could not find process's proxy file.");
        return NULL;
    }

    struct vomsdata *voms_ptr = (*g_VOMS_Init)(NULL, NULL);
    int error = 0;
    if (!(*g_VOMS_RetrieveFromFile)(fp, RECURSE_CHAIN, voms_ptr, &error)) {
        char *err_str = (*g_VOMS_ErrorMessage)(voms_ptr, error, NULL, 0);
        LogCvmfs(kLogVoms, kLogDebug, "Unable to parse VOMS file: %s\n",
                 err_str);
        free(err_str);
        (*g_VOMS_Destroy)(voms_ptr);
        return NULL;
    }
    return voms_ptr;
}


static void
SplitGroupToPaths(const std::string &group,
                  std::vector<std::string> &hierarchy)  // NOLINT
{
    size_t start = 0, end = 0;
    while ((end = group.find('/', start)) != std::string::npos)
    {
        if (end-start)
        {
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
IsRoleMatching(const char *role1, const char *role2)
{
    if ((role2 == NULL) || (strlen(role2) == 0) ||
        !strcmp(role2, "NULL"))
    {
      return true;
    }
    if ((role1 == NULL) || !strcmp(role1, "NULL")) {return false;}

    return !strcmp(role1, role2);
}


bool
CheckVOMSAuthz(const struct fuse_ctx *ctx, const std::string & authz)
{
    if (g_VOMS_Init == NULL) {
        LogCvmfs(kLogVoms, kLogDebug, "VOMS library not present; failing VOMS "
                 "authz.");
        return false;
    }

    // Break the authz into VOMS and roles.
    // We will compare the required auth against the cached session VOMS info.
    // Roles must match exactly; Sub-groups are authorized in their parent
    // group.

    // NOTE: Trim out everything past the first ',' or '\n'; this will allow
    // us to extend the authz in the future.
    size_t delim = std::min(authz.find(","), authz.find("\n"));
    std::string first_authz = (delim != std::string::npos) ?
                              authz.substr(0, delim) : authz;
    delim = first_authz.find("/Role=");
    std::string role, group;
    if (delim != std::string::npos) {
        role = first_authz.substr(delim+6);
        group = first_authz.substr(0, delim);
    } else {
        group = first_authz;
    }
    if (group[0] != '/') {return false;}
    std::vector<std::string> group_hierarchy;
    SplitGroupToPaths(group, group_hierarchy);

    LogCvmfs(kLogVoms, kLogDebug, "Checking whether user with UID %d has VOMS"
             " auth %s.", ctx->uid, authz.c_str());

    struct vomsdata *voms_ptr;
    if (!g_cache.get(ctx->pid, voms_ptr))
    {
        voms_ptr = GenerateVOMSData(ctx);
        if (voms_ptr)
        {
            voms_ptr = g_cache.try_put(ctx->pid, voms_ptr);
            LogCvmfs(kLogVoms, kLogDebug, "Caching user's VOMS credentials at"
                     " address %p.", voms_ptr);
        } else {
            LogCvmfs(kLogVoms, kLogDebug, "User has no VOMS credentials.");
            return false;
        }
    } else {
        LogCvmfs(kLogVoms, kLogDebug, "Using cached VOMS credentials.");
    }
    if (!voms_ptr) {
        LogCvmfs(kLogVoms, kLogDebug, "ERROR: VOMS credentials are null "
                 "pointer.");
        return false;
    }

    // Now we have valid VOMS data, check authz.
    for (int idx=0; voms_ptr->data[idx] != NULL; idx++)
    {  // Iterator through the VOs
        struct voms *it = voms_ptr->data[idx];
        // Iterate through the FQANs.
        for (int idx2=0; it->std[idx2] != NULL; idx2++)
        {
            struct data *it2 = it->std[idx2];
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
