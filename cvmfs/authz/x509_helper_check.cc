/**
 * This file is part of the CernVM File System.
 */
#include "x509_helper_check.h"

#include <alloca.h>
#include <sys/types.h>

#include <cassert>
#include <cstdio>
#include <cstring>
#include <vector>

#include "authz/x509_helper_globus.h"
#include "authz/x509_helper_log.h"
#include "authz/x509_helper_voms.h"

using namespace std;  // NOLINT


namespace {

/**
 * Create the VOMS data structure to the best of our abilities.
 *
 * Resulting memory is owned by caller and must be destroyed with VOMS_Destroy.
 */
struct authz_state {
  FILE *m_fp;
  struct vomsdata *m_voms;
  globus_gsi_cred_handle_t m_cred;
  BIO *m_bio;
  X509 *m_cert;
  EVP_PKEY *m_pkey;
  STACK_OF(X509) *m_chain;
  char *m_subject;
  globus_gsi_callback_data_t m_callback;

  authz_state() :
    m_fp(NULL),
    m_voms(NULL),
    m_cred(NULL),
    m_bio(NULL),
    m_cert(NULL),
    m_pkey(NULL),
    m_chain(NULL),
    m_subject(NULL),
    m_callback(NULL)
  {}

  ~authz_state() {
    if (m_fp) {fclose(m_fp);}
    if (m_voms) {(*g_VOMS_Destroy)(m_voms);}
    if (m_cred) {(*g_globus_gsi_cred_handle_destroy)(m_cred);}
    if (m_bio) {BIO_free(m_bio);}
    if (m_cert) {X509_free(m_cert);}
    if (m_pkey) {EVP_PKEY_free(m_pkey);}
    if (m_chain) {sk_X509_pop_free(m_chain, X509_free);}
    if (m_subject) {OPENSSL_free(m_subject);}
    if (m_callback) {(*g_globus_gsi_callback_data_destroy)(m_callback);}
  }
};

}  // anonymous namespace


static authz_data *GenerateVOMSData(FILE *fp_proxy) {
  authz_state state;
  state.m_fp = fp_proxy;

  // Start of Globus proxy parsing and verification...
  globus_result_t result =
      (*g_globus_gsi_cred_handle_init)(&state.m_cred, NULL);
  if (GLOBUS_SUCCESS != result) {
    GlobusLib::GetInstance()->PrintError(result);
    return NULL;
  }

  state.m_bio = BIO_new_fp(state.m_fp, 0);
  if (!state.m_bio) {
    LogAuthz(kLogAuthzDebug, "Unable to allocate new BIO object");
    return NULL;
  }

  result = (*g_globus_gsi_cred_read_proxy_bio)(state.m_cred, state.m_bio);
  if (GLOBUS_SUCCESS != result) {
    LogAuthz(kLogAuthzDebug, "Failed to parse credentials");
    GlobusLib::GetInstance()->PrintError(result);
    return NULL;
  }

  // Setup Globus callback object.
  result = (*g_globus_gsi_callback_data_init)(&state.m_callback);
  if (GLOBUS_SUCCESS != result) {
    GlobusLib::GetInstance()->PrintError(result);
    return NULL;
  }
  char *cert_dir;
  result = (*g_globus_gsi_sysconfig_get_cert_dir_unix)(&cert_dir);
  if (GLOBUS_SUCCESS != result) {
    LogAuthz(kLogAuthzDebug,
             "Failed to determine trusted certificates directory.");
    GlobusLib::GetInstance()->PrintError(result);
    return NULL;
  }
  result = (*g_globus_gsi_callback_set_cert_dir)(state.m_callback, cert_dir);
  free(cert_dir);
  if (GLOBUS_SUCCESS != result) {
    GlobusLib::GetInstance()->PrintError(result);
    return NULL;
  }

  // Verify credential chain.
  result = (*g_globus_gsi_cred_verify_cert_chain)(state.m_cred,
                                                  state.m_callback);
  if (GLOBUS_SUCCESS != result) {
    LogAuthz(kLogAuthzDebug, "Failed to validate credentials");
    GlobusLib::GetInstance()->PrintError(result);
    return NULL;
  }

  // Load key and certificate from Globus handle
  result = (*g_globus_gsi_cred_get_key)(state.m_cred, &state.m_pkey);
  if (GLOBUS_SUCCESS != result) {
    LogAuthz(kLogAuthzDebug, "Failed to get process private key.");
    GlobusLib::GetInstance()->PrintError(result);
    return NULL;
  }
  result = (*g_globus_gsi_cred_get_cert)(state.m_cred, &state.m_cert);
  if (GLOBUS_SUCCESS != result) {
    LogAuthz(kLogAuthzDebug, "Failed to get process certificate.");
    GlobusLib::GetInstance()->PrintError(result);
    return NULL;
  }

  // Check proxy public key and private key match.
  if (!X509_check_private_key(state.m_cert, state.m_pkey)) {
    LogAuthz(kLogAuthzDebug, "Process certificate and key do not match");
    return NULL;
  }

  // Load certificate chain
  result = (*g_globus_gsi_cred_get_cert_chain)(state.m_cred, &state.m_chain);
  if (GLOBUS_SUCCESS != result) {
    LogAuthz(kLogAuthzDebug, "Process does not have cert chain.");
    GlobusLib::GetInstance()->PrintError(result);
    return NULL;
  }

  // Look through certificates to find an EEC (which has the subject)
  globus_gsi_cert_utils_cert_type_t cert_type;
  X509 *eec_cert = state.m_cert;
  result = (*g_globus_gsi_cert_utils_get_cert_type)(state.m_cert, &cert_type);
  if (GLOBUS_SUCCESS != result) {
    GlobusLib::GetInstance()->PrintError(result);
    return NULL;
  }
  if (!(cert_type & GLOBUS_GSI_CERT_UTILS_TYPE_EEC)) {
    result = (*g_globus_gsi_cert_utils_get_identity_cert)(state.m_chain,
                                                          &eec_cert);
    if (GLOBUS_SUCCESS != result) {
      GlobusLib::GetInstance()->PrintError(result);
      return NULL;
    }
  }
  // From the EEC, use OpenSSL to determine the subject
  char *dn = X509_NAME_oneline(X509_get_subject_name(eec_cert), NULL, 0);
  if (!dn) {
    LogAuthz(kLogAuthzDebug, "Unable to determine certificate DN.");
    return NULL;
  }
  state.m_subject = strdup(dn);
  OPENSSL_free(dn);

  state.m_voms = (*g_VOMS_Init)(NULL, NULL);
  if (!state.m_voms) {
    return NULL;
  }

  int voms_error = 0;
  const int retval = (*g_VOMS_Retrieve)(state.m_cert, state.m_chain,
                                        RECURSE_CHAIN,
                                        state.m_voms, &voms_error);
  // If there is no VOMS extension (VERR_NOEXT), this shouldn't be fatal.
  if (!retval && (voms_error != VERR_NOEXT)) {
    char *err_str = (*g_VOMS_ErrorMessage)(state.m_voms, voms_error, NULL, 0);
    LogAuthz(kLogAuthzDebug, "Unable to parse VOMS file: %s\n", err_str);
    free(err_str);
    return NULL;
  }

  // Move pointers to returned authz_data structure
  authz_data *authz = new authz_data();
  authz->dn_ = state.m_subject;
  state.m_subject = NULL;
  if (voms_error != VERR_NOEXT) {
    authz->voms_ = state.m_voms;
    state.m_voms = NULL;
  }
  return authz;
}


static void
SplitGroupToPaths(const string &group, vector<string> *hierarchy) {
  size_t start = 0, end = 0;
  while ((end = group.find('/', start)) != std::string::npos) {
    if (end-start) {
      hierarchy->push_back(group.substr(start, end-start));
    }
    start = end + 1;
  }
  if (start != group.size()-1) {hierarchy->push_back(group.substr(start));}
}


static bool
IsSubgroupOf(const vector<string> &group1, const vector<string> &group2) {
  if (group1.size() < group2.size()) {
    return false;
  }
  vector<string>::const_iterator it1 = group1.begin();
  for (vector<string>::const_iterator it2 = group2.begin();
       it2 != group2.end();
       it1++, it2++)
  {
    if (*it1 != *it2) {return false;}
  }
  return true;
}


static bool
IsRoleMatching(const char *role1, const char *role2) {
  if ((role2 == NULL) || (strlen(role2) == 0) ||
      !strcmp(role2, "NULL"))
  {
    return true;
  }
  if ((role1 == NULL) || !strcmp(role1, "NULL")) {return false;}

  return !strcmp(role1, role2);
}


static bool
CheckSingleAuthz(const authz_data *authz_ptr, const string &authz) {
  // An empty entry should authorize nobody.
  if (authz.empty()) {return false;}

  // Break the authz into VOMS VO, groups, and roles.
  // We will compare the required auth against the cached session VOMS info.
  // Roles must match exactly; Sub-groups are authorized in their parent
  // group.
  std::string vo, role, group;
  bool is_dn = false;
  if (authz[0] != '/') {
    size_t delim = authz.find(':');
    if (delim != std::string::npos) {
      vo = authz.substr(0, delim);
      size_t delim2 = authz.find("/Role=", delim+1);
      if (delim2 != std::string::npos) {
        role = authz.substr(delim2 + 6);
        group = authz.substr(delim + 1, delim2 - delim - 1);
      } else {
        group = authz.substr(delim + 1);;
      }
    }
  } else {
    // No VOMS info in the authz; it is a DN.
    is_dn = true;
  }
  // Quick sanity check of group name.
  if (!group.empty() && group[0] != '/') {return false;}

  vector<string> group_hierarchy;
  SplitGroupToPaths(group, &group_hierarchy);

  // Now we have valid data, check authz.
  // First, check if it is a valid DN.
  if (is_dn) {
    return !strcmp(authz.c_str(), authz_ptr->dn_);
  }
  // If there is no VOMS info, return immediately..
  if (!authz_ptr->voms_) {return false;}
  // Iterator through the VOs
  for (int idx=0; authz_ptr->voms_->data[idx] != NULL; idx++) {
    struct voms *it = authz_ptr->voms_->data[idx];
    if (!it->voname) {continue;}
    if (strcmp(vo.c_str(), it->voname)) {continue;}

    // Iterate through the FQANs.
    for (int idx2=0; it->std[idx2] != NULL; idx2++) {
      struct data *it2 = it->std[idx2];
      if (!it2->group) {continue;}
      LogAuthz(kLogAuthzDebug, "Checking (%s Role=%s) against group"
               " %s, role %s.", group.c_str(), role.c_str(), it2->group,
               it2->role);
      vector<string> avail_hierarchy;
      SplitGroupToPaths(it2->group, &avail_hierarchy);
      if (IsSubgroupOf(avail_hierarchy, group_hierarchy) &&
          IsRoleMatching(it2->role, role.c_str()))
      {
        return true;
      }
    }
  }

  return false;
}


static bool
CheckMultipleAuthz(const authz_data *authz_ptr,
                   const string &authz_list)
{
  // Check all authorizations against our information
  size_t last_delim = 0;
  size_t delim = authz_list.find('\n');
  while (delim != std::string::npos) {
    std::string next_authz = authz_list.substr(last_delim, delim-last_delim);
    last_delim = delim + 1;
    delim = authz_list.find('\n', last_delim);

    if (CheckSingleAuthz(authz_ptr, next_authz)) {return true;}
  }
  std::string next_authz = authz_list.substr(last_delim);
  return CheckSingleAuthz(authz_ptr, next_authz);
}


bool CheckX509Proxy(const string &membership, FILE *fp_proxy) {
  authz_data *voms_data = GenerateVOMSData(fp_proxy);
  if (voms_data == NULL)
    return false;
  const bool result = CheckMultipleAuthz(voms_data, membership);
  delete voms_data;
  return result;
}
