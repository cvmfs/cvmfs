#include "testutil.h"

#include <fstream>
#include <sstream>


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
  static const std::string ppid_label = "PPid:";
  pid_t parent_pid = 0;

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

  return parent_pid;
}
