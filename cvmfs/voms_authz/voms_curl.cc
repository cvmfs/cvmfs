/**
 * This file is part of the CernVM File System.
 *
 * This configures a given CURL handle to authenticate using a user's X509
 * credential.
 */


#include "voms_authz.h"

#include <openssl/err.h>
#include <openssl/ssl.h>

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>

#include <cstring>

#include "../duplex_curl.h"
#include "../logging_internal.h"
#include "../util_concurrency.h"

// TODO(jblomer): more documentation

// TODO(jblomer): add unit tests for functions, perhaps all these functions can
// be encapuslated in a class so that the global vanish, too.
pthread_mutex_t g_ssl_mutex = PTHREAD_MUTEX_INITIALIZER;
bool loaded_ssl_strings = false;

struct sslctx_info {
  sslctx_info() : chain(NULL), pkey(NULL) {}

  STACK_OF(X509) *chain;
  EVP_PKEY *pkey;
};


static void
LogOpenSSLErrors(const char *top_message) {
  {
    MutexLockGuard guard(g_ssl_mutex);
    if (!loaded_ssl_strings) {
      SSL_load_error_strings();
      loaded_ssl_strings = true;
    }
  }
  char error_buf[1024];
  LogCvmfs(kLogVoms, kLogWarning, "%s", top_message);
  unsigned long next_err;  // NOLINT; this is the type expected by OpenSSL
  while ((next_err = ERR_get_error())) {
    ERR_error_string_n(next_err, error_buf, 1024);
    LogCvmfs(kLogVoms, kLogStderr, "%s", error_buf);
  }
}

static CURLcode sslctx_config_function(CURL *curl, void *sslctx, void *parm) {
  sslctx_info * p = reinterpret_cast<sslctx_info *>(parm);
  SSL_CTX * ctx = reinterpret_cast<SSL_CTX *>(sslctx);

  if (parm == NULL) {return CURLE_OK;}

  STACK_OF(X509) *chain = p->chain;
  EVP_PKEY *pkey = p->pkey;

  LogCvmfs(kLogVoms, kLogDebug, "Customizing OpenSSL context.");

  int cert_count = sk_X509_num(chain);
  if (cert_count == 0) {
    LogOpenSSLErrors("No certificate found in chain.");
  }
  X509 *cert = sk_X509_value(chain, 0);

  // NOTE: SSL_CTX_use_certificate and _user_PrivateKey increase the ref count.
  if (!SSL_CTX_use_certificate(ctx, cert)) {
    LogOpenSSLErrors("Failed to set the user certificate in the SSL "
                     "connection");
    return CURLE_SSL_CERTPROBLEM;
  }

  if (!SSL_CTX_use_PrivateKey(ctx, pkey)) {
    LogOpenSSLErrors("Failed to set the private key in the SSL connection");
    return CURLE_SSL_CERTPROBLEM;
  }

  if (!SSL_CTX_check_private_key(ctx)) {
    LogOpenSSLErrors("Provided certificate and key do not match");
    return CURLE_SSL_CERTPROBLEM;
  } else {
    LogCvmfs(kLogVoms, kLogDebug, "Client certificate and key match.");
  }

  // NOTE: SSL_CTX_add_extra_chain_cert DOES NOT increase the ref count
  // Instead, it now owns the pointer.  THIS IS DIFFERENT FROM ABOVE.
  for (int idx = 1; idx < cert_count; idx++) {
    cert = sk_X509_value(chain, idx);
    if (!SSL_CTX_add_extra_chain_cert(ctx, X509_dup(cert))) {
      LogOpenSSLErrors("Failed to add client cert to chain");
    }
  }

  return CURLE_OK;
}


void
ReleaseCurlHandle(CURL *curl_handle, void *info_data) {
  sslctx_info * p = reinterpret_cast<sslctx_info *>(info_data);
  STACK_OF(X509) *chain = p->chain;
  EVP_PKEY *pkey = p->pkey;
  p->chain = NULL;
  p->pkey = NULL;
  delete p;

  // Calls X509_free on each element, then frees the stack itself
  sk_X509_pop_free(chain, X509_free);
  EVP_PKEY_free(pkey);

  // Make sure that if CVMFS reuses this curl handle, curl doesn't try
  // to reuse cert chain we just freed.
  curl_easy_setopt(curl_handle, CURLOPT_SSL_CTX_DATA, 0);
}


bool
ConfigureCurlHandle(CURL *curl_handle, pid_t pid, uid_t uid, gid_t gid,
                    char **info_fname, void **info_data)
{
    // We cannot rely on libcurl to pipeline (yet), as cvmfs may
    // bounce between different auth handles.
    curl_easy_setopt(curl_handle, CURLOPT_FRESH_CONNECT, 1);
    curl_easy_setopt(curl_handle, CURLOPT_FORBID_REUSE, 1);
    curl_easy_setopt(curl_handle, CURLOPT_SSL_SESSIONID_CACHE, 0);
    curl_easy_setopt(curl_handle, CURLOPT_SSL_CTX_DATA, NULL);

    // The calling layer is reusing data;
    if (info_data && *info_data) {
      curl_easy_setopt(curl_handle, CURLOPT_SSL_CTX_DATA, *info_data);
      return true;
    }

    if (info_fname && *info_fname) {delete *info_fname; *info_fname = NULL;}

    int fd = -1;
    FILE *fp = GetProxyFile(pid, uid, gid);
    if (fp == NULL) {return false;}

    // Prefer to load credentials into memory and not bother with temporary
    // files.
    if (info_data && curl_easy_setopt(curl_handle, CURLOPT_SSL_CTX_FUNCTION,
                         sslctx_config_function) == CURLE_OK)
    {
      LogCvmfs(kLogVoms, kLogDebug, "Configuring in-memory OpenSSL callback");

      sslctx_info *parm = new sslctx_info;

      STACK_OF(X509_INFO) *sk = NULL;
      STACK_OF(X509) *certstack = sk_X509_new_null();
      parm->chain = certstack;
      if (certstack == NULL) {
        LogCvmfs(kLogVoms, kLogStderr, "Failed to allocate new X509 chain.");
        fclose(fp);
        return false;
      }

      if (!(sk=PEM_X509_INFO_read(fp, NULL, NULL, NULL))) {
        LogOpenSSLErrors("Failed to load credential file.");
        sk_X509_INFO_free(sk);
        sk_X509_free(certstack);
        fclose(fp);
        return false;
      }

      while (sk_X509_INFO_num(sk)) {
        X509_INFO *xi = sk_X509_INFO_shift(sk);
        if (xi == NULL) {continue;}
        if (xi->x509 != NULL) {
          CRYPTO_add(&xi->x509->references, 1, CRYPTO_LOCK_X509);
          sk_X509_push(certstack, xi->x509);
        }
        if ((xi->x_pkey != NULL) && (xi->x_pkey->dec_pkey != NULL)) {
          parm->pkey = xi->x_pkey->dec_pkey;
          CRYPTO_add(&parm->pkey->references, 1, CRYPTO_LOCK_EVP_PKEY);
        }
        X509_INFO_free(xi);
      }
      sk_X509_INFO_free(sk);

      if (parm->pkey == NULL) {
        sk_X509_free(certstack);
        fclose(fp);
        LogCvmfs(kLogVoms, kLogStderr, "Credential did not contain a decrypted"
                 " private key.");
        return false;
      }

      if (!sk_X509_num(certstack)) {
        EVP_PKEY_free(parm->pkey);
        sk_X509_free(certstack);
        fclose(fp);
        LogCvmfs(kLogVoms, kLogStderr, "Credential file did not contain any "
                 "actual credentials.");
        return false;
      } else {
        LogCvmfs(kLogVoms, kLogDebug, "Certificate stack contains %d entries.",
                 sk_X509_num(certstack));
      }

      curl_easy_setopt(curl_handle, CURLOPT_SSL_CTX_DATA, parm);
      *info_data = parm;

    } else if (info_fname) {
      int fd_proxy = fileno(fp);
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

      curl_easy_setopt(curl_handle, CURLOPT_SSLCERT, fname);
      curl_easy_setopt(curl_handle, CURLOPT_SSLKEY, fname);
      *info_fname = strdup(fname);
    } else {
       fclose(fp);
       close(fd);
       return false;
    }
    fclose(fp);
    close(fd);
    return true;
}
