#ifndef CVMFS_UPLOAD_RIAK_H_
#define CVMFS_UPLOAD_RIAK_H_

#include "upload_backend.h"

#include <vector>

namespace upload {
  class RiakSpoolerBackend : public AbstractSpoolerBackend {
   protected:
    struct RiakConfiguration {
      RiakConfiguration(const std::string &config_file_path);

      std::string bucket;
    };

    class PushFinishedCallback {
     public:
      PushFinishedCallback(const RiakSpoolerBackend *delegate,
                           const std::string        &local_path = "",
                           const hash::Any          &content_hash = hash::Any()) :
        delegate_(delegate),
        local_path_(local_path),
        content_hash_(content_hash) {}

      void operator()(const int return_code) const {
        delegate_->SendResult(return_code, local_path_, content_hash_);
      }

     private:
      const RiakSpoolerBackend *delegate_;
      const std::string         local_path_;
      const hash::Any           content_hash_;
    };

   public:
    RiakSpoolerBackend(const std::string &config_file_path);
    virtual ~RiakSpoolerBackend();
    bool Initialize();

    bool IsReady() const;

   protected:
    void Copy(const std::string &local_path,
              const std::string &remote_path,
              const bool move);
    void Process(const std::string &local_path,
                 const std::string &remote_dir,
                 const std::string &file_suffix,
                 const bool move);

    std::string GenerateRiakKey(const hash::Any   &compressed_hash,
                                const std::string &remote_dir,
                                const std::string &file_suffix) const;
    std::string GenerateRiakKey(const std::string &remote_path) const;
    void PushFileToRiakAsync(const std::string          &key,
                             const std::string          &file_path,
                             const PushFinishedCallback &callback);

   private:
    RiakConfiguration config_;
    bool initialized_;
  };
}

#endif /* CVMFS_UPLOAD_RIAK_H_ */
