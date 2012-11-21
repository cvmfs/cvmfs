#ifndef CVMFS_UPLOAD_LOCAL_H_
#define CVMFS_UPLOAD_LOCAL_H_

#include "upload_backend.h"

namespace upload
{
  class LocalSpoolerBackend : public AbstractSpoolerBackend {
   public:
    LocalSpoolerBackend(const std::string &upstream_path);

    bool Initialize();
    bool IsReady() const;
   
   protected:
    void Copy(const std::string &local_path,
              const std::string &remote_path,
              const bool move,
              std::string &response);
    void Process(const std::string &local_path,
                 const std::string &remote_dir,
                 const std::string &file_suffix,
                 const bool move,
                 std::string &response);

   private:
    const std::string upstream_path_;
    bool initialized_;
  };
}

#endif /* CVMFS_UPLOAD_LOCAL_H_ */
