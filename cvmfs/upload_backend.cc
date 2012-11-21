#include "upload_backend.h"

#include <fcntl.h>
#include <errno.h>

#include "compression.h"
#include "logging.h"
#include "util.h"

using namespace upload;

AbstractSpoolerBackend::AbstractSpoolerBackend() :
  pipes_connected_(false),
  initialized_(false)
{}


AbstractSpoolerBackend::~AbstractSpoolerBackend() {
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "Spooler backend terminates");
  fclose(fpathes_);
  close(fd_digests_);
}


bool AbstractSpoolerBackend::Connect(const std::string &fifo_paths,
                                     const std::string &fifo_digests) {
  // connect to incoming pathes pipe
  fpathes_ = fopen(fifo_paths.c_str(), "r");
  if (!fpathes_) {
    LogCvmfs(kLogSpooler, kLogStderr,
             "Spooler Backend failed to connect to pathes pipe");
    return false;
  }
  LogCvmfs(kLogSpooler, kLogVerboseMsg,
           "Spooler Backend connected to pathes pipe");
  
  // open a pipe for outgoing digest hashes
  fd_digests_ = open(fifo_digests.c_str(), O_WRONLY);
  if (fd_digests_ < 0) {
    fclose(fpathes_);
    LogCvmfs(kLogSpooler, kLogStderr,
             "Spooler Backend failed to create digests pipe");
    return false;
  }
  LogCvmfs(kLogSpooler, kLogVerboseMsg,
           "Spooler Backend created digests pipe");

  // all set and ready to go...
  pipes_connected_ = true;
  return true;
}


bool AbstractSpoolerBackend::Initialize() {
  if (!pipes_connected_) {
    LogCvmfs(kLogSpooler, kLogWarning, "IO pipes are not setup properly");
    return false;
  }

  initialized_ = true;
  return true;
}


int AbstractSpoolerBackend::Run() {
  int retval;
  bool running = true;

  std::string local_path;
  std::string remote_path;
  std::string remote_dir;
  std::string file_suffix;
  std::string return_line = "";

  // reading from input pipe until it is gone
  while ((retval = getc_unlocked(fpathes_)) != EOF && running) {

    // figure out if file should be moved and what the actual command is
    bool move_file = false;
    if (retval & kCmdMoveFlag) {
      retval -= kCmdMoveFlag;
      move_file = true;
    }
    unsigned char command = retval;

    return_line.clear();
    switch (command) {
      case kCmdEndOfTransaction:
        LogCvmfs(kLogSpooler, kLogVerboseMsg,
                 "Spooler received 'end of transaction'");

        EndOfTransaction(return_line);
        running = false;
        break;

      case kCmdCopy:
        GetString(fpathes_, &local_path);
        GetString(fpathes_, &remote_path);
        LogCvmfs(kLogSpooler, kLogVerboseMsg,
                 "Spooler received 'copy': source %s, dest %s move %d",
                 local_path.c_str(), remote_path.c_str(), move_file);

        Copy(local_path, remote_path, move_file, return_line);
        break;

      case kCmdProcess:
        GetString(fpathes_, &local_path);
        GetString(fpathes_, &remote_dir);
        GetString(fpathes_, &file_suffix);
        LogCvmfs(kLogSpooler, kLogVerboseMsg,
                 "Spooler received 'process': source %s, dest %s, "
                 "postfix %s, move %d", local_path.c_str(),
                 remote_dir.c_str(), file_suffix.c_str(), move_file);

        Process(local_path, remote_dir, file_suffix, move_file, return_line);
        break;

      default:
        LogCvmfs(kLogSpooler, kLogWarning, "Spooler received 'unknown command': %d",
                 command);

        Unknown(return_line);
        break;
    }

    LogCvmfs(kLogSpooler, kLogVerboseMsg,
             "Spooler sends back result %s",
             return_line.c_str());

    WritePipe(fd_digests_, return_line.data(), return_line.length());
  }

  return 0;
}


void AbstractSpoolerBackend::CreateResponseMessage(
                                    std::string &response,
                                    const int error_code,
                                    const std::string &local_path,
                                    const std::string &compressed_hash) const {
  response = StringifyInt(error_code);
  response.push_back('\0');
  response.append(local_path);
  response.push_back('\0');
  response.append(compressed_hash);
  response.push_back('\n');
}


void AbstractSpoolerBackend::EndOfTransaction(std::string &response) {
  CreateResponseMessage(response, 0, "", "");
}


void AbstractSpoolerBackend::Unknown(std::string &response) {
  CreateResponseMessage(response, 1, "", "");
}


bool AbstractSpoolerBackend::IsReady() const {
  return 
    pipes_connected_ &&
    initialized_;
}


bool AbstractSpoolerBackend::GetString(FILE *f, std::string *str) const {
  str->clear();
  do {
    int retval = getc_unlocked(f);
    if (retval == EOF)
      return false;
    char c = retval;
    if (c == '\0')
      return true;
    str->push_back(c);
  } while (true);
}
