/**
 * This file is part of the CernVM File System.
 */

#include "ingestion/ingestion_source.h"
#include "ingestion/quit_beacon.h"

IngestionSource* IngestionSource::CreateQuitBeacon() {
  return new QuitBeacon();
}
