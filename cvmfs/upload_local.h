#ifndef CVMFS_UPLOAD_LOCAL_H_
#define CVMFS_UPLOAD_LOCAL_H_

#include "upload_backend.h"

namespace upload
{
  class LocalSpoolerBackend : public AbstractSpoolerBackend
  {
   public:
    LocalSpoolerBackend();
    virtual ~LocalSpoolerBackend();
    bool Initialize();

    bool IsReady() const;

    /**
     *  This method configures the upstream path for the local repository
     *  location. Call this once after you created this SpoolerBackend, then
     *  call initialize and you should be ready to go.
     *  @param upstream_path      the upstream path to be configured
     */
    void set_upstream_path(const std::string &upstream_path);
   
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
    std::string upstream_path_;
    bool initialized_;
  };
}

#endif /* CVMFS_UPLOAD_LOCAL_H_ */
