/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "repository_util.h"

#include <cstdio>
#include <string>
#include <vector>

#include "hash.h"
#include "publish/except.h"
#include "util/posix.h"
#include "util/string.h"

namespace publish {

CheckoutMarker *CheckoutMarker::CreateFrom(const std::string &path) {
  if (!FileExists(path))
    return NULL;

  FILE *f = fopen(path.c_str(), "r");
  if (f == NULL)
    throw publish::EPublish("cannot open checkout marker");
  std::string line;
  bool retval = GetLineFile(f, &line);
  fclose(f);
  if (!retval)
    throw publish::EPublish("empty checkout marker");
  line = Trim(line, true /* trim_newline */);
  std::vector<std::string> tokens = SplitString(line, ' ');
  if (tokens.size() != 3)
    throw publish::EPublish("checkout marker not parsable: " + line);

  CheckoutMarker *marker = new CheckoutMarker(tokens[0], tokens[2],
    shash::MkFromHexPtr(shash::HexPtr(tokens[1]), shash::kSuffixCatalog));
  return marker;
}

void CheckoutMarker::SaveAs(const std::string &path) const {
  std::string marker =
    tag_ + " " + hash_.ToString(false /* with_suffix */) + " " + branch_ + "\n";
  SafeWriteToFile(marker, path, kDefaultFileMode);
}

}  // namespace publish
