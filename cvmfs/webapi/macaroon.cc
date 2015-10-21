/**
 * This file is part of the CernVM File System
 */

#include "cvmfs_config.h"
#include "macaroon.h"

#include <ctime>

#include "../util.h"

using namespace std;  // NOLINT

Macaroon::Macaroon()
  : expiry_utc_(0)
{
}


void Macaroon::ToJson() {

}


string Macaroon::ComputeHmac() {

}
