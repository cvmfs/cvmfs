#include "testutil.h"

#include <fstream>
#include <sstream>

#ifdef __APPLE__
  #include <sys/sysctl.h>
#endif


void SkipWhitespace(std::istringstream &iss) {
  while (iss.good()) {
    const char next = iss.peek();
    if (next != ' ' && next != '\t') {
      break;
    }
    iss.get();
  }
}


pid_t GetParentPid(const pid_t pid) {
  pid_t parent_pid = 0;

#ifdef __APPLE__
  int mib[4];
  size_t len;
  struct kinfo_proc kp;

  len = 4;
  sysctlnametomib("kern.proc.pid", mib, &len);

  mib[3] = pid;
  len = sizeof(kp);
  if (sysctl(mib, 4, &kp, &len, NULL, 0) == 0) {
    parent_pid = kp.kp_eproc.e_ppid;
  }
#else
  static const std::string ppid_label = "PPid:";

  std::stringstream proc_status_path;
  proc_status_path << "/proc/" << pid << "/status";

  std::ifstream proc_status(proc_status_path.str().c_str());

  std::string line;
  while (std::getline(proc_status, line)) {
    if (line.compare(0, ppid_label.size(), ppid_label) == 0) {
      const std::string s_ppid = line.substr(ppid_label.size());
      std::istringstream iss_ppid(s_ppid);
      SkipWhitespace(iss_ppid);
      int i_ppid = 0; iss_ppid >> i_ppid;
      if (i_ppid > 0) {
        parent_pid = static_cast<pid_t>(i_ppid);
      }
      break;
    }
  }
#endif

  return parent_pid;
}

namespace catalog {

DirectoryEntry DirectoryEntryTestFactory::RegularFile() {
  DirectoryEntry dirent;
  dirent.mode_ = 33188;
  return dirent;
}


DirectoryEntry DirectoryEntryTestFactory::Directory() {
  DirectoryEntry dirent;
  dirent.mode_ = 16893;
  return dirent;
}


DirectoryEntry DirectoryEntryTestFactory::Symlink() {
  DirectoryEntry dirent;
  dirent.mode_ = 41471;
  return dirent;
}


DirectoryEntry DirectoryEntryTestFactory::ChunkedFile() {
  DirectoryEntry dirent;
  dirent.mode_ = 33188;
  dirent.is_chunked_file_ = true;
  return dirent;
}

} /* namespace catalog */
