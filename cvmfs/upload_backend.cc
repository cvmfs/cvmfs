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

AbstractSpoolerBackend::~AbstractSpoolerBackend()
{}

bool AbstractSpoolerBackend::Connect(const std::string &fifo_paths,
                                     const std::string &fifo_digests)
{
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
  int fd_digests_ = open(fifo_digests.c_str(), O_WRONLY);
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

bool AbstractSpoolerBackend::Initialize()
{
  if (!pipes_connected_)
  {
    LogCvmfs(kLogSpooler, kLogWarning, "IO pipes are not setup properly");
    return false;
  }

  initialized_ = true;
  return true;
}

void AbstractSpoolerBackend::Run()
{
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
                 "Spooler sends transaction ack back");

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
        LogCvmfs(kLogSpooler, kLogVerboseMsg, "Spooler received 'unknown command': %d",
                 command);

        Unknown(return_line);
        break;
    }

    LogCvmfs(kLogSpooler, kLogVerboseMsg,
             "Spooler sends back result %s",
             return_line.c_str());
    WritePipe(fd_digests_, return_line.data(), return_line.length());
  }
}

void AbstractSpoolerBackend::Unknown(std::string &response)
{
  response = "1";
  response.push_back('\0');
  response.push_back('\0');
  response.push_back('\n');
}

bool AbstractSpoolerBackend::IsReady() const
{
  return 
    pipes_connected_ &&
    initialized_;
}

bool AbstractSpoolerBackend::Compress()
{

  return false;
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

// -----------------------------------------------------------------------------

LocalSpoolerBackend::LocalSpoolerBackend() :
  initialized_(false)
{}

LocalSpoolerBackend::~LocalSpoolerBackend()
{

}

void LocalSpoolerBackend::set_upstream_path(const std::string &upstream_path)
{
  if (IsReady())
    LogCvmfs(kLogSpooler, kLogWarning, "Setting upstream path in a running spooler backend might be harmful!");

  upstream_path_ = upstream_path;
}

bool LocalSpoolerBackend::Initialize()
{
  bool retval = AbstractSpoolerBackend::Initialize();
  if (!retval)
    return false;

  if (upstream_path_.empty())
  {
    LogCvmfs(kLogSpooler, kLogWarning, "upstream path not configured");
    return false;
  }

  initialized_ = true;
  return true;
}

void LocalSpoolerBackend::EndOfTransaction(std::string &response)
{
  response = "0";
  response.push_back('\0');
  response.push_back('\0');
  response.push_back('\n');
}

void LocalSpoolerBackend::Copy(const std::string &local_path,
                               const std::string &remote_path,
                               const bool move,
                               std::string &response)
{
  if (move) {
    int retval = rename(local_path.c_str(), remote_path.c_str());
    response = (retval == 0) ? "0" : StringifyInt(errno);
  } else {
    int retval = CopyPath2Path(local_path, remote_path);
    response = retval ? "0" : "100";
  }
  response.push_back('\0');
  response.append(local_path);
  response.push_back('\0');
  response.push_back('\n');
}

void LocalSpoolerBackend::Process(const std::string &local_path,
                                  const std::string &remote_dir,
                                  const std::string &file_suffix,
                                  const bool move,
                                  std::string &response)
{
  hash::Any compressed_hash(hash::kSha1);
  std::string remote_path = upstream_path_ + "/" + remote_dir;

  std::string tmp_path;
  FILE *fcas = CreateTempFile(remote_path + "/cvmfs", 0777, "w",
                              &tmp_path);
  if (fcas == NULL) {
    response = "103";
  } else {
    int retval = zlib::CompressPath2File(local_path, fcas,
                                         &compressed_hash);
    response = retval ? "0" : "103";
    fclose(fcas);
    if (retval) {
      const std::string cas_path = remote_path + compressed_hash.MakePath(1, 2)
                              + file_suffix;
      retval = rename(tmp_path.c_str(), cas_path.c_str());
      if (retval != 0) {
        unlink(tmp_path.c_str());
        response = "104";
      }
    }
  }
  if (move) {
    if (unlink(local_path.c_str()) != 0)
      response = "105";
  }
  response.push_back('\0');
  response.append(local_path);
  response.push_back('\0');
  response.append(compressed_hash.ToString());
  response.push_back('\n');
}

bool LocalSpoolerBackend::IsReady() const
{
  const bool ready = AbstractSpoolerBackend::IsReady();
  return ready && initialized_;
}

// -----------------------------------------------------------------------------

RiakSpoolerBackend::RiakSpoolerBackend() :
  initialized_(false)
{}

RiakSpoolerBackend::~RiakSpoolerBackend()
{

}

bool RiakSpoolerBackend::Initialize()
{
  bool retval = AbstractSpoolerBackend::Initialize();
  if (!retval)
    return false;

  return true;
}

void RiakSpoolerBackend::EndOfTransaction(std::string &response)
{

}

void RiakSpoolerBackend::Copy(const std::string &local_path,
                              const std::string &remote_path,
                              const bool move,
                              std::string &response)
{
  
}

void RiakSpoolerBackend::Process(const std::string &local_path,
                                 const std::string &remote_dir,
                                 const std::string &file_suffix,
                                 const bool move,
                                 std::string &response)
{

}

bool RiakSpoolerBackend::IsReady() const
{
  const bool ready = AbstractSpoolerBackend::IsReady();
  return ready && initialized_;
}
