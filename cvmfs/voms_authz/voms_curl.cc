
#include "voms_authz.h"

#include <curl/curl.h>

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

#include <cstring>

bool
ConfigureCurlHandle(CURL *curl_handle, pid_t pid, uid_t uid, gid_t gid, char *&info_fname)
{
    int fd = -1;
    FILE *fp = GetProxyFile(pid, uid, gid);
    if (fp == NULL) {return false;}
    int fd_proxy = fileno(fp);
    if (info_fname) {delete info_fname; info_fname = NULL;}
    char fname[] = "/tmp/cvmfs_credential_XXXXXX";
    fd = mkstemp(fname);
    if (fd == -1) {return false;}

    char buf[1024];
    while (1)
    {
        ssize_t count;
        count = read(fd_proxy, buf, 1024);
        if (count == -1)
        {
            if (errno == EINTR) {continue;}
            close(fd);
            fclose(fp);
            return false;
        }
        if (count == 0) {break;}
        char *buf2 = buf;
        while (count)
        {
            ssize_t count2 = count;
            count2 = write(fd, buf, count);
            if (count2 == -1)
            {
                if (errno == EINTR) {continue;}
                close(fd);
                fclose(fp);
                return false;
            }
            count -= count2;
            buf2 += count2;
        }
    }

    info_fname = strdup(fname);
    fclose(fp);
    close(fd);
    // We cannot rely on libcurl to pipeline, as cvmfs may
    // bounce between different auth handles.
    curl_easy_setopt(curl_handle, CURLOPT_FRESH_CONNECT, 1);
    curl_easy_setopt(curl_handle, CURLOPT_FORBID_REUSE, 1);
    curl_easy_setopt(curl_handle, CURLOPT_SSL_SESSIONID_CACHE, 0);
    curl_easy_setopt(curl_handle, CURLOPT_SSLCERT, fname);
    curl_easy_setopt(curl_handle, CURLOPT_SSLKEY, fname);
    return true;
}

