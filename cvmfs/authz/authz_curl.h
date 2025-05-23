/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_AUTHZ_AUTHZ_CURL_H_
#define CVMFS_AUTHZ_AUTHZ_CURL_H_

#include <string>

#include "authz.h"
#include "network/download.h"

class AuthzSessionManager;

class AuthzAttachment : public download::CredentialsAttachment {
 public:
  explicit AuthzAttachment(AuthzSessionManager *sm);
  virtual ~AuthzAttachment() { }

  virtual bool ConfigureCurlHandle(CURL *curl_handle,
                                   pid_t pid,
                                   void **info_data);
  virtual void ReleaseCurlHandle(CURL *curl_handle, void *info_data);

  void set_membership(const std::string &m) { membership_ = m; }

 private:
  static void LogOpenSSLErrors(const char *top_message);
  static CURLcode CallbackSslCtx(CURL *curl, void *sslctx, void *parm);
  bool ConfigureSciTokenCurl(CURL *curl_handle,
                             const AuthzToken &token,
                             void **info_data);

  static bool ssl_strings_loaded_;

  /**
   * Used to gather user's credentials.
   */
  AuthzSessionManager *authz_session_manager_;

  /**
   * The required user group needs to be set on mount and remount by the client.
   */
  std::string membership_;
};

#endif  // CVMFS_AUTHZ_AUTHZ_CURL_H_
