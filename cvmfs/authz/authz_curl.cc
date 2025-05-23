/**
 * This file is part of the CernVM File System.
 */
#define __STDC_FORMAT_MACROS
#include "authz_curl.h"

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <pthread.h>

#include <cassert>

#include "authz/authz_session.h"
#include "crypto/openssl_version.h"
#include "duplex_curl.h"
#include "util/concurrency.h"
#include "util/logging.h"
#include "util/pointer.h"

using namespace std;  // NOLINT


namespace {

struct sslctx_info {
  sslctx_info() : chain(NULL), pkey(NULL) { }

  STACK_OF(X509) * chain;
  EVP_PKEY *pkey;
};

struct bearer_info {
  /**
   * List of extra headers to put on the HTTP request.  This is required
   * in order to add the "Authorization: Bearer XXXXX" header.
   */
  struct curl_slist *list;

  /**
   * Actual text of the bearer token
   */
  char *token;
};
}  // anonymous namespace


bool AuthzAttachment::ssl_strings_loaded_ = false;


AuthzAttachment::AuthzAttachment(AuthzSessionManager *sm)
    : authz_session_manager_(sm) {
  // Required for logging OpenSSL errors
  SSL_load_error_strings();
  ssl_strings_loaded_ = true;
}


CURLcode AuthzAttachment::CallbackSslCtx(CURL *curl, void *sslctx, void *parm) {
  sslctx_info *p = reinterpret_cast<sslctx_info *>(parm);
  SSL_CTX *ctx = reinterpret_cast<SSL_CTX *>(sslctx);

  if (parm == NULL)
    return CURLE_OK;

  STACK_OF(X509) *chain = p->chain;
  EVP_PKEY *pkey = p->pkey;

  LogCvmfs(kLogAuthz, kLogDebug, "Customizing OpenSSL context.");

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
    LogCvmfs(kLogAuthz, kLogDebug, "Client certificate and key match.");
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


bool AuthzAttachment::ConfigureSciTokenCurl(CURL *curl_handle,
                                            const AuthzToken &token,
                                            void **info_data) {
  if (*info_data == NULL) {
    AuthzToken *saved_token = new AuthzToken();
    saved_token->type = kTokenBearer;
    saved_token->data = new bearer_info;
    bearer_info *bearer = static_cast<bearer_info *>(saved_token->data);
    bearer->list = NULL;
    bearer->token = static_cast<char *>(
        smalloc((sizeof(char) * token.size) + 1));
    memcpy(bearer->token, token.data, token.size);
    static_cast<char *>(bearer->token)[token.size] = 0;
    *info_data = saved_token;
  }

  AuthzToken *tmp_token = static_cast<AuthzToken *>(*info_data);
  bearer_info *bearer = static_cast<bearer_info *>(tmp_token->data);

  LogCvmfs(kLogAuthz, kLogDebug, "Setting OAUTH bearer token to: %s",
           static_cast<char *>(bearer->token));

  // Create the Bearer token
  // The CURLOPT_XOAUTH2_BEARER option only works "IMAP, POP3 and SMTP"
  // protocols. Not HTTPS
  std::string auth_preamble = "Authorization: Bearer ";
  std::string auth_header = auth_preamble + static_cast<char *>(bearer->token);
  bearer->list = curl_slist_append(bearer->list, auth_header.c_str());
  int retval = curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, bearer->list);

  if (retval != CURLE_OK) {
    LogCvmfs(kLogAuthz, kLogSyslogErr, "Failed to set Oauth2 Bearer Token");
    return false;
  }

  return true;
}


bool AuthzAttachment::ConfigureCurlHandle(CURL *curl_handle,
                                          pid_t pid,
                                          void **info_data) {
  assert(info_data);

  // File catalog has no membership requirement, no tokens to attach
  if (membership_.empty())
    return false;

  // We cannot rely on libcurl to pipeline (yet), as cvmfs may
  // bounce between different auth handles.
  curl_easy_setopt(curl_handle, CURLOPT_FRESH_CONNECT, 1);
  curl_easy_setopt(curl_handle, CURLOPT_FORBID_REUSE, 1);
  curl_easy_setopt(curl_handle, CURLOPT_SSL_SESSIONID_CACHE, 0);

  UniquePtr<AuthzToken> token(
      authz_session_manager_->GetTokenCopy(pid, membership_));
  if (!token.IsValid()) {
    LogCvmfs(kLogAuthz, kLogDebug, "failed to get authz token for pid %d", pid);
    return false;
  }

  switch (token->type) {
    case kTokenBearer:
      // If it's a scitoken, then just go to the private
      // ConfigureSciTokenCurl function
      return ConfigureSciTokenCurl(curl_handle, *token, info_data);

    case kTokenX509:
      // The x509 code is below, so just break and go.
      break;

    default:
      // Oh no, don't know the the token type, throw error and return
      LogCvmfs(kLogAuthz, kLogDebug, "unknown token type: %d", token->type);
      return false;
  }

  curl_easy_setopt(curl_handle, CURLOPT_SSL_CTX_DATA, NULL);

  // The calling layer is reusing data;
  if (*info_data) {
    curl_easy_setopt(curl_handle, CURLOPT_SSL_CTX_DATA,
                     static_cast<AuthzToken *>(*info_data)->data);
    return true;
  }


  int retval = curl_easy_setopt(
      curl_handle, CURLOPT_SSL_CTX_FUNCTION, CallbackSslCtx);
  if (retval != CURLE_OK) {
    LogCvmfs(kLogAuthz, kLogDebug, "cannot configure curl ssl callback");
    return false;
  }

  UniquePtr<sslctx_info> parm(new sslctx_info);

  STACK_OF(X509_INFO) *sk = NULL;
  STACK_OF(X509) *certstack = sk_X509_new_null();
  parm->chain = certstack;
  if (certstack == NULL) {
    LogCvmfs(kLogAuthz, kLogSyslogErr, "Failed to allocate new X509 chain.");
    return false;
  }

  BIO *bio_token = BIO_new_mem_buf(token->data, token->size);
  assert(bio_token != NULL);
  sk = PEM_X509_INFO_read_bio(bio_token, NULL, NULL, NULL);
  BIO_free(bio_token);
  if (!sk) {
    LogOpenSSLErrors("Failed to load credential file.");
    sk_X509_INFO_free(sk);
    sk_X509_free(certstack);
    return false;
  }

  while (sk_X509_INFO_num(sk)) {
    X509_INFO *xi = sk_X509_INFO_shift(sk);
    if (xi == NULL) {
      continue;
    }
    if (xi->x509 != NULL) {
#ifdef OPENSSL_API_INTERFACE_V11
      retval = X509_up_ref(xi->x509);
      assert(retval == 1);
#else
      CRYPTO_add(&xi->x509->references, 1, CRYPTO_LOCK_X509);
#endif
      sk_X509_push(certstack, xi->x509);
    }
    if ((xi->x_pkey != NULL) && (xi->x_pkey->dec_pkey != NULL)) {
      parm->pkey = xi->x_pkey->dec_pkey;
#ifdef OPENSSL_API_INTERFACE_V11
      retval = EVP_PKEY_up_ref(parm->pkey);
      assert(retval == 1);
#else
      CRYPTO_add(&parm->pkey->references, 1, CRYPTO_LOCK_EVP_PKEY);
#endif
    }
    X509_INFO_free(xi);
  }
  sk_X509_INFO_free(sk);

  if (parm->pkey == NULL) {
    // Sigh - PEM_X509_INFO_read doesn't understand old key encodings.
    // Try a more general-purpose function.
    BIO *bio_token = BIO_new_mem_buf(token->data, token->size);
    assert(bio_token != NULL);
    EVP_PKEY *old_pkey = PEM_read_bio_PrivateKey(bio_token, NULL, NULL, NULL);
    BIO_free(bio_token);
    if (old_pkey) {
      parm->pkey = old_pkey;
    } else {
      sk_X509_free(certstack);
      LogCvmfs(kLogAuthz, kLogSyslogErr,
               "credential did not contain a decrypted private key.");
      return false;
    }
  }

  if (!sk_X509_num(certstack)) {
    EVP_PKEY_free(parm->pkey);
    sk_X509_free(certstack);
    LogCvmfs(kLogAuthz, kLogSyslogErr,
             "Credential file did not contain any actual credentials.");
    return false;
  } else {
    LogCvmfs(kLogAuthz, kLogDebug, "Certificate stack contains %d entries.",
             sk_X509_num(certstack));
  }

  AuthzToken *to_return = new AuthzToken();
  to_return->type = kTokenX509;
  to_return->data = static_cast<void *>(parm.Release());
  curl_easy_setopt(curl_handle, CURLOPT_SSL_CTX_DATA,
                   static_cast<sslctx_info *>(to_return->data));
  *info_data = to_return;
  return true;
}


void AuthzAttachment::LogOpenSSLErrors(const char *top_message) {
  assert(ssl_strings_loaded_);
  char error_buf[1024];
  LogCvmfs(kLogAuthz, kLogSyslogWarn, "%s", top_message);
  unsigned long next_err;  // NOLINT; this is the type expected by OpenSSL
  while ((next_err = ERR_get_error())) {
    ERR_error_string_n(next_err, error_buf, 1024);
    LogCvmfs(kLogAuthz, kLogSyslogErr, "%s", error_buf);
  }
}


void AuthzAttachment::ReleaseCurlHandle(CURL *curl_handle, void *info_data) {
  assert(info_data);

  AuthzToken *token = static_cast<AuthzToken *>(info_data);
  if (token->type == kTokenBearer) {
    // Compiler complains if we delete a void*
    bearer_info *bearer = static_cast<bearer_info *>(token->data);
    delete static_cast<char *>(bearer->token);
    curl_slist_free_all(bearer->list);
    delete static_cast<bearer_info *>(token->data);
    token->data = NULL;
    delete token;

  } else if (token->type == kTokenX509) {
    sslctx_info *p = static_cast<sslctx_info *>(token->data);
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
}
