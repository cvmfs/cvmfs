#ifndef CVMFS_UPLOAD_RIAK_H_
#define CVMFS_UPLOAD_RIAK_H_

#include "upload_backend.h"

namespace upload {
  class RiakSpoolerBackend : public AbstractSpoolerBackend {
   public:
    RiakSpoolerBackend(const std::string &config_file_path);
    virtual ~RiakSpoolerBackend();
    bool Initialize();

    bool IsReady() const;

    bool addRiakNode(const std::string& url);

   protected:
    void Copy(const std::string &local_path,
              const std::string &remote_path,
              const bool move);
    void Process(const std::string &local_path,
                 const std::string &remote_dir,
                 const std::string &file_suffix,
                 const bool move);

   private:
    const std::string config_file_path_;
    bool initialized_;
  };
}

#endif /* CVMFS_UPLOAD_RIAK_H_ */
