#include "jobinfo.h"
#include "util/string.h"

namespace download {

bool JobInfo::IsFileNotFound() {
  if (HasPrefix(*url, "file://", true /* ignore_case */))
    return error_code == kFailHostConnection;

  return http_code == 404;
}

}  // namespace download
