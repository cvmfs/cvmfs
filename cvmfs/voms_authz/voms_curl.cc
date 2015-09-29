
#include "voms_authz.h"

#include <curl/curl.h>
#include <openssl/err.h>
#include <openssl/ssl.h>

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

#include <cstring>

#include "../logging_internal.h"
#include "../util_concurrency.h"

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
  unsigned long next_err;
  while ((next_err = ERR_get_error())) {
    ERR_error_string_n(next_err, error_buf, 1024);
    LogCvmfs(kLogVoms, kLogStderr, "%s", error_buf);
  }
}

static CURLcode sslctx_config_function(CURL *curl, void *sslctx, void *parm) {
  sslctx_info * p = (sslctx_info *) parm;
  SSL_CTX * ctx = (SSL_CTX *) sslctx ;

  if (parm == NULL) {return CURLE_OK;}

  STACK_OF(X509) *chain = p->chain;
  EVP_PKEY *pkey = p->pkey;
  delete p;
  p = NULL;

  // The context will now own the memory - do not reuse.
  curl_easy_setopt(curl, CURLOPT_SSL_CTX_DATA, NULL);

  LogCvmfs(kLogVoms, kLogDebug, "Customizing OpenSSL context.");

  X509 *cert = sk_X509_shift(chain);
  if (cert == NULL) {
    LogOpenSSLErrors("No certificate found in chain.");
    EVP_PKEY_free(pkey);
    sk_X509_free(chain);
  }

  // NOTE: SSL_CTX_use_certificate and _user_PrivateKey increase the ref count.
  // Hence, we must call free afterward.
  if (!SSL_CTX_use_certificate(ctx, cert)) {
    LogOpenSSLErrors("Failed to set the user certificate in the SSL connection");
    X509_free(cert);
    EVP_PKEY_free(pkey);
    sk_X509_free(chain);
    return CURLE_SSL_CERTPROBLEM;
  }
  X509_free(cert);
  if (!SSL_CTX_use_PrivateKey(ctx, pkey)) {
    LogOpenSSLErrors("Failed to set the private key in the SSL connection");
    EVP_PKEY_free(pkey);
    sk_X509_free(chain);
    return CURLE_SSL_CERTPROBLEM;
  }
  EVP_PKEY_free(pkey);
 
  if (!SSL_CTX_check_private_key(ctx)) {
    LogOpenSSLErrors("Provided certificate and key do not match");
    sk_X509_free(chain);
    return CURLE_SSL_CERTPROBLEM;
  } else {
    LogCvmfs(kLogVoms, kLogDebug, "Client certificate and key match.");
  }

  // NOTE: SSL_CTX_add_extra_chain_cert DOES NOT increase the ref count
  // Instead, it now owns the pointer.  THIS IS DIFFERENT FROM ABOVE.
  while ((cert = sk_X509_shift(chain))) {
    if (!SSL_CTX_add_extra_chain_cert(ctx, cert)) {
      LogOpenSSLErrors("Failed to add client cert to chain");
      X509_free(cert);
    }
  }
  sk_X509_free(chain);

  return CURLE_OK;
}

bool
ConfigureCurlHandle(CURL *curl_handle, pid_t pid, uid_t uid, gid_t gid,
                    char *&info_fname)
{
    if (info_fname) {delete info_fname; info_fname = NULL;}

    int fd = -1;
    FILE *fp = GetProxyFile(pid, uid, gid);
    if (fp == NULL) {return false;}

    // We cannot rely on libcurl to pipeline, as cvmfs may
    // bounce between different auth handles.
    curl_easy_setopt(curl_handle, CURLOPT_FRESH_CONNECT, 1);
    curl_easy_setopt(curl_handle, CURLOPT_FORBID_REUSE, 1);
    curl_easy_setopt(curl_handle, CURLOPT_SSL_SESSIONID_CACHE, 0);

    // Prefer to load credentials into memory and not bother with temporary files.
    if (curl_easy_setopt(curl_handle, CURLOPT_SSL_CTX_FUNCTION, sslctx_config_function) == CURLE_OK) {

      LogCvmfs(kLogVoms, kLogDebug, "Configuring in-memory OpenSSL callback");

      sslctx_info *parm = new sslctx_info;

      STACK_OF(X509_INFO) *sk=NULL;
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
        LogCvmfs(kLogVoms, kLogStderr, "Credential did not contain a decrypted private key.");
        return false;
      }

      if (!sk_X509_num(certstack)) {
        EVP_PKEY_free(parm->pkey);
        sk_X509_free(certstack);
        fclose(fp);
        LogCvmfs(kLogVoms, kLogStderr, "Credential file did not contain any actual credentials.");
        return false;
      } else {
        LogCvmfs(kLogVoms, kLogDebug, "Certificate stack contains %d entries.", sk_X509_num(certstack));
      }

      curl_easy_setopt(curl_handle, CURLOPT_SSL_CTX_DATA, parm);

    } else {

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
      info_fname = strdup(fname);
    }
    fclose(fp);
    close(fd);
    return true;
}

