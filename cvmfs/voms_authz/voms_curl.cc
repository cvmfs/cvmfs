
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
    int fd_proxy = fileno(fp);
    if (fp == NULL) {return false;}
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
    curl_easy_setopt(curl_handle, CURLOPT_SSLCERT, fname);
    curl_easy_setopt(curl_handle, CURLOPT_SSLKEY, fname);
    curl_easy_setopt(curl_handle, CURLOPT_SSL_VERIFYPEER, 1L);
    const char *cadir = getenv("X509_CERT_DIR");
    if (!cadir) {cadir = "/etc/grid-security/certificates";}
    curl_easy_setopt(curl_handle, CURLOPT_CAPATH, cadir);
    return true;
}

