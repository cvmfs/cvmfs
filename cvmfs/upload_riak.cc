#include "upload_riak.h"

using namespace upload;

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