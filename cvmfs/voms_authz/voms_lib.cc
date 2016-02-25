/**
 * This file is part of the CernVM File System.
 *
 * This contains stubs and loaders for the VOMS librariy;
 * it allows us to load and use the library  at runtime but not
 * link against them at compile time.
 */

#include "../logging.h"

#include "loader_helper.h"
#include "voms_lib.h"


extern "C" {
// VOMS API declarations
struct vomsdata * (*g_VOMS_Init)(char *voms, char *cert) = NULL;
void (*g_VOMS_Destroy)(struct vomsdata *vd) = NULL;
int (*g_VOMS_Retrieve)(X509 *cert, STACK_OF(X509) *chain, int how,
                         struct vomsdata *vd, int *error) = NULL;
char * (*g_VOMS_ErrorMessage)(struct vomsdata *vd, int error, char *buffer,
                              int len) = NULL;
int (*g_VOMS_Export)(char **buffer, int *buflen, struct vomsdata *vd,
                     int *error) = NULL;
int (*g_VOMS_Import)(char *buffer, int buflen, struct vomsdata *vd,
                     int *error) = NULL;

}


void VomsLib::Load() {
  if (!OpenDynLib(&m_libvoms_handle, "libvomsapi.so.1", "VOMS")) {return;}
  if (
      !LoadSymbol(m_libvoms_handle, &g_VOMS_Init, "VOMS_Init") ||
      !LoadSymbol(m_libvoms_handle, &g_VOMS_Destroy, "VOMS_Destroy") ||
      !LoadSymbol(m_libvoms_handle, &g_VOMS_Retrieve, "VOMS_Retrieve") ||
      !LoadSymbol(m_libvoms_handle, &g_VOMS_Destroy, "VOMS_Destroy") ||
      !LoadSymbol(m_libvoms_handle, &g_VOMS_ErrorMessage, "VOMS_ErrorMessage") ||
      !LoadSymbol(m_libvoms_handle, &g_VOMS_Export, "VOMS_Export") ||
      !LoadSymbol(m_libvoms_handle, &g_VOMS_Import, "VOMS_Import")
     ) {
        g_VOMS_Init = NULL;
        return;
  }
  LogCvmfs(kLogVoms, kLogDebug, "Successfully loaded VOMS library");
  m_zombie = false;
}


void VomsLib::Close() {
  CloseDynLib(&m_libvoms_handle, "VOMS");
  g_VOMS_Init = NULL;
  g_VOMS_Destroy = NULL;
  g_VOMS_Retrieve = NULL;
  g_VOMS_ErrorMessage = NULL;
  g_VOMS_Export = NULL;
  g_VOMS_Import = NULL;
}

VomsLib VomsLib::g_voms;

