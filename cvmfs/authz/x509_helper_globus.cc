/**
 * This file is part of the CernVM File System.
 */
#include "x509_helper_globus.h"

#include <dlfcn.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <time.h>

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "authz/x509_helper_dynlib.h"
#include "authz/x509_helper_log.h"


extern "C" {
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
globus_result_t (*g_globus_gsi_cert_utils_get_identity_cert)(
    STACK_OF(X509) *                    cert_chain,
    X509 **                             eec) = NULL;
globus_result_t (*g_globus_gsi_callback_data_init)(
    globus_gsi_callback_data_t *        callback_data) = NULL;
globus_result_t (*g_globus_gsi_callback_data_destroy)(
    globus_gsi_callback_data_t          callback_data) = NULL;
globus_result_t (*g_globus_gsi_callback_set_cert_dir)(
    globus_gsi_callback_data_t          callback_data,
    char *                              cert_dir) = NULL;
globus_result_t (*g_globus_gsi_sysconfig_get_cert_dir_unix)(
    char **                             cert_dir) = NULL;
}


GlobusLib::GlobusLib() :
  m_zombie(true),
  m_globus_module_handle(NULL),
  m_globus_gsi_cert_utils_handle(NULL),
  m_globus_gsi_credential_handle(NULL),
  m_globus_gsi_callback_handle(NULL),
  m_globus_gsi_sysconfig_handle(NULL)
{
  Load();
  LogAuthz(kLogAuthzDebug|kLogAuthzSyslog,
           "Support for Globus authz is %senabled.", m_zombie ? "NOT " : "");
}


GlobusLib::~GlobusLib() {
  Close();
}

void
GlobusLib::PrintError(globus_result_t result) {
  globus_object_t *error_obj = (*g_globus_error_get)(result);
  if (error_obj == g_GLOBUS_ERROR_BASE_STATIC_PROTOTYPE) {
    LogAuthz(kLogAuthzDebug,
             "Globus error occurred (no further information available)");
    return;
  }
  char *error_full = (*g_globus_error_print_chain)(error_obj);
  if (!error_full) {
    LogAuthz(kLogAuthzDebug, "Globus error occurred (error unprintable)");
    return;
  }
  LogAuthz(kLogAuthzDebug, "Globus error: %s", error_full);
  free(error_full);
}


void GlobusLib::Close() {
  if (!m_zombie) {
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
  g_globus_gsi_cert_utils_get_identity_cert = NULL;
  g_globus_gsi_callback_data_init = NULL;
  g_globus_gsi_callback_data_destroy = NULL;
  g_globus_gsi_callback_set_cert_dir = NULL;
  g_globus_gsi_sysconfig_get_cert_dir_unix = NULL;
  m_zombie = true;
}


void GlobusLib::Load() {
  if (!OpenDynLib(&m_globus_module_handle,
                  "libglobus_common.so.0",
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
                  &g_globus_gsi_cert_utils_get_identity_cert,
                  "globus_gsi_cert_utils_get_identity_cert") ||
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
    LogAuthz(kLogAuthzDebug, "Failed to enable Globus thread model.");
    return;
  }
  if (GLOBUS_SUCCESS !=
      (*g_globus_module_activate)(g_globus_i_gsi_cert_utils_module)) {
    LogAuthz(kLogAuthzDebug,
             "Failed to activate Globus GSI cert utils module.");
    return;
  }
  if (GLOBUS_SUCCESS !=
      (*g_globus_module_activate)(g_globus_i_gsi_credential_module)) {
    LogAuthz(kLogAuthzDebug,
             "Failed to activate Globus GSI credential module.");
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
  LogAuthz(kLogAuthzDebug, "Successfully loaded Globus library");
  m_zombie = false;
}

GlobusLib *GlobusLib::g_globus = NULL;
