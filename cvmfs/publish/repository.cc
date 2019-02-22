/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "publish/repository.h"

#include <cstddef>

#include "logging.h"
#include "publish/settings.h"
#include "publish/except.h"
#include "upload.h"
#include "upload_spooler_definition.h"
#include "util/pointer.h"

namespace publish {

Publisher *Publisher::Create(const SettingsPublisher &settings) {
  upload::SpoolerDefinition sd(
    settings.storage().GetLocator(),
    settings.transaction().hash_algorithm(),
    settings.transaction().compression_algorithm());
  UniquePtr<upload::Spooler> spooler(upload::Spooler::Construct(sd));
  if (!spooler.IsValid()) throw EPublish("could not initialize spooler");

  if (!spooler->Create())
    throw EPublish("could not initialize repository storage area");

  return NULL;
  //return new Publisher(settings);
}

void Publisher::EditTags() {}
void Publisher::Ingest() {}
void Publisher::Migrate() {}
void Publisher::Resign() {}
void Publisher::Rollback() {}
void Publisher::UpdateMetaInfo() {}

}
