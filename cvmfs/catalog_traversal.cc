/**
 * This file is part of the CernVM File System.
 */

#include <limits>

#include "catalog_traversal.h"

namespace swissknife {

  const unsigned int CatalogTraversalParams::kFullHistory =
    std::numeric_limits<unsigned int>::max();
  const unsigned int CatalogTraversalParams::kNoHistory = 0;

}
