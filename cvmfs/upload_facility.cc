/**
 * This file is part of the CernVM File System.
 */

#include "upload_facility.h"

#include "upload_local.h"

using namespace upload;

void AbstractUploader::RegisterPlugins() {
  RegisterPlugin<LocalUploader>();
}


AbstractUploader::AbstractUploader(const SpoolerDefinition& spooler_definition) :
  spooler_definition_(spooler_definition)
{}


bool AbstractUploader::Initialize() {
  return true;
}


void AbstractUploader::TearDown() {}


void AbstractUploader::WaitForUpload() const {}


void AbstractUploader::DisablePrecaching() {}


void AbstractUploader::EnablePrecaching() {}


void AbstractUploader::Respond(const callback_t  *callback,
                               const int          return_code,
                               const std::string  local_path) {
  if (callback == NULL) {
    return;
  }

  const UploaderResults result(return_code, local_path);
  (*callback)(result);
  delete callback;
}
