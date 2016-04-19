/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_AUTHZ_X509_HELPER_GLOBUS_H_
#define CVMFS_AUTHZ_X509_HELPER_GLOBUS_H_

#include "globus/globus_gsi_cert_utils.h"
#include "globus/globus_gsi_credential.h"
#include "globus/globus_module.h"

extern "C" {
// Globus API declarations

// For activating / deactivating modules.
extern int (*g_globus_module_activate)(
    globus_module_descriptor_t * module_descriptor);
extern int (*g_globus_module_deactivate)(
    globus_module_descriptor_t * module_descriptor);
extern int (*g_globus_thread_set_model)(
    const char *                        model);
extern globus_object_t * (*g_globus_error_get)(
    globus_result_t                     result);
extern char * (*g_globus_error_print_chain)(
    globus_object_t *                   error);
extern globus_object_t *g_GLOBUS_ERROR_BASE_STATIC_PROTOTYPE;
// Modules we'll want to use.
extern globus_module_descriptor_t *g_globus_i_gsi_cert_utils_module;
extern globus_module_descriptor_t *g_globus_i_gsi_credential_module;
extern globus_module_descriptor_t *g_globus_i_gsi_callback_module;
extern globus_module_descriptor_t *g_globus_i_gsi_sysconfig_module;

// Actual functions we need to invoke.
extern globus_result_t (*g_globus_gsi_cred_handle_init)(
    globus_gsi_cred_handle_t *          handle,
    globus_gsi_cred_handle_attrs_t      handle_attrs);
extern globus_result_t (*g_globus_gsi_cred_handle_destroy)(
    globus_gsi_cred_handle_t            handle);
extern globus_result_t (*g_globus_gsi_cred_read_proxy_bio)(
    globus_gsi_cred_handle_t            handle,
    BIO *                               bio);
extern globus_result_t (*g_globus_gsi_cred_get_cert)(
    globus_gsi_cred_handle_t            handle,
    X509 **                             cert);
extern globus_result_t (*g_globus_gsi_cred_get_key)(
    globus_gsi_cred_handle_t            handle,
    EVP_PKEY **                         key);
extern globus_result_t (*g_globus_gsi_cred_get_cert_chain)(
    globus_gsi_cred_handle_t            handle,
    STACK_OF(X509) **                   cert_chain);
extern globus_result_t (*g_globus_gsi_cred_get_subject_name)(
    globus_gsi_cred_handle_t            handle,
    char **                             subject_name);
extern globus_result_t (*g_globus_gsi_cred_verify_cert_chain)(
    globus_gsi_cred_handle_t            cred_handle,
    globus_gsi_callback_data_t          callback_data);
extern globus_result_t (*g_globus_gsi_cert_utils_get_cert_type)(
    X509 *                              cert,
    globus_gsi_cert_utils_cert_type_t * type);
extern globus_result_t (*g_globus_gsi_cert_utils_get_identity_cert)(
    STACK_OF(X509) *                    cert_chain,
    X509 **                             eec);
extern globus_result_t (*g_globus_gsi_callback_data_init)(
    globus_gsi_callback_data_t *        callback_data);
extern globus_result_t (*g_globus_gsi_callback_data_destroy)(
    globus_gsi_callback_data_t          callback_data);
extern globus_result_t (*g_globus_gsi_callback_set_cert_dir)(
    globus_gsi_callback_data_t          callback_data,
    char *                              cert_dir);
extern globus_result_t (*g_globus_gsi_sysconfig_get_cert_dir_unix)(
    char **                             cert_dir);
}

class GlobusLib {
 public:
  GlobusLib();
  ~GlobusLib();

  void PrintError(globus_result_t result);

  static GlobusLib *GetInstance() {
    if (!g_globus)
      g_globus = new GlobusLib();
    return g_globus;
  }

  bool IsValid() const {return !m_zombie;}

 private:
  GlobusLib(const GlobusLib&);
  void Close();
  void Load();

  bool m_zombie;

  // Various library handles.
  void *m_globus_module_handle;
  void *m_globus_gsi_cert_utils_handle;
  void *m_globus_gsi_credential_handle;
  void *m_globus_gsi_callback_handle;
  void *m_globus_gsi_sysconfig_handle;

  static GlobusLib *g_globus;
};

#endif  // CVMFS_AUTHZ_X509_HELPER_GLOBUS_H_
