
#include <sys/types.h>
#include <stdio.h>
#include <errno.h>
#include <limits.h>
#include <sys/syscall.h>

#include <cstring>

#include "../logging.h"


static bool
GetProxyFileFromEnv(pid_t pid, char *path, size_t pathLen)
{
    static const char * const X509_USER_PROXY = "\0X509_USER_PROXY=";
    size_t X509_USER_PROXY_LEN = strlen(X509_USER_PROXY+1)+1;

    if (snprintf(path, pathLen, "/proc/%d/environ", pid) >= static_cast<int>(pathLen))
    {
        if (errno == 0) {errno = ERANGE;}
        return false;
    }
    int olduid = geteuid();
    // NOTE: we use the underlying syscall here as it is PER-THREAD
    // while the glibc call is PER-PROCESS
    //
    // ALSO NOTE: we ignore return values of these syscalls; this code path
    // will work if cvmfs is FUSE-mounted as an unprivileged user.
    syscall(SYS_setresuid, -1, 0, -1);
    FILE *fp = fopen(path, "r");
    if (!fp)
    {
        LogCvmfs(kLogVoms, kLogDebug, "Failed to open environment file for pid %d.\n", pid);
        syscall(SYS_setresuid, -1, olduid, -1);
        return false;
    }

    char c = '\0';
    size_t idx = 0, keyIdx = 0;
    bool set_env = false;
    while (1)
    {
        if (c == EOF) {break;}
        if (keyIdx == X509_USER_PROXY_LEN)
        {
            if (idx >= pathLen - 1) {break;}
            if (c == '\0') {set_env = true; break;}
            path[idx++] = c;
        }
        else if (X509_USER_PROXY[keyIdx++] != c)
        {
            keyIdx = 0;
        }
        c = fgetc(fp);
    }
    fclose(fp);
    syscall(SYS_setresuid, -1, olduid, -1);
    if (set_env) {path[idx] = '\0';}
    return set_env;
}


FILE *
GetProxyFile(pid_t pid, uid_t uid, gid_t gid)
{
    char path[PATH_MAX];
    if (!GetProxyFileFromEnv(pid, path, PATH_MAX))
    {
        LogCvmfs(kLogVoms, kLogDebug, "Could not find proxy in environment; using default location in /tmp/x509up_u%d.", uid);
        if (snprintf(path, PATH_MAX, "/tmp/x509up_u%d", uid) >= PATH_MAX)
        {
            if (errno == 0) {errno = ERANGE;}
            return NULL;
        }
    }
    LogCvmfs(kLogVoms, kLogDebug, "Looking for proxy in file %s.", path);


    int olduid = geteuid();
    int oldgid = getegid();
    // NOTE the sequencing: we must be eUID 0
    // to change the UID and GID.
    syscall(SYS_setresuid, -1, 0, -1);
    syscall(SYS_setresgid, -1, gid, -1);
    syscall(SYS_setresuid, -1, uid, -1);

    FILE *fp = fopen(path, "r");
    syscall(SYS_setresuid, -1, 0, -1);
    syscall(SYS_setresgid, -1, oldgid, -1);
    syscall(SYS_setresuid, -1, olduid, -1);

    return fp;
}

