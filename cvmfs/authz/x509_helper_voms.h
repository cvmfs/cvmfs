/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_AUTHZ_X509_HELPER_VOMS_H_
#define CVMFS_AUTHZ_X509_HELPER_VOMS_H_

#include "voms/voms_apic.h"

#include "authz/x509_helper_log.h"

extern "C" {
// VOMS API declarations
extern struct vomsdata * (*g_VOMS_Init)(char *voms, char *cert);
extern void (*g_VOMS_Destroy)(struct vomsdata *vd);
extern int (*g_VOMS_Retrieve)(X509 *cert, STACK_OF(X509) *chain, int how,
                         struct vomsdata *vd, int *error);
extern char * (*g_VOMS_ErrorMessage)(struct vomsdata *vd, int error,
                         char *buffer, int len);
extern int (*g_VOMS_Export)(char **buffer, int *buflen, struct vomsdata *vd,
                            int *error);
extern int (*g_VOMS_Import)(char *buffer, int buflen, struct vomsdata *vd,
                            int *error);
}


struct authz_data {
  struct vomsdata *voms_;
  char *dn_;

  authz_data() :
    voms_(NULL),
    dn_(NULL)
  {}

  ~authz_data() {
    if (voms_ && g_VOMS_Destroy) {(*g_VOMS_Destroy)(voms_);}
    if (dn_) {free(dn_);}
  }
};


class VomsLib {
 public:
  VomsLib()
    : m_zombie(true)
  {
    Load();
    LogAuthz(kLogAuthzDebug | kLogAuthzSyslog,
             "Support for VOMS authz is %senabled.", m_zombie ? "NOT " : "");
  }


  bool IsValid() const {return !m_zombie;}

  static VomsLib *GetInstance()
  {
    if (!g_voms)
      g_voms = new VomsLib();
    return g_voms;
  }

 private:
  VomsLib(const VomsLib&);
  void Load();
  void Close();

  bool m_zombie;
  void *m_libvoms_handle;
  static VomsLib *g_voms;
};

#endif  // CVMFS_AUTHZ_X509_HELPER_VOMS_H_
