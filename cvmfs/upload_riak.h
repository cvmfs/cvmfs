#ifndef CVMFS_UPLOAD_RIAK_H_
#define CVMFS_UPLOAD_RIAK_H_

#include "upload_backend.h"

#include <vector>

typedef void CURL;
struct curl_slist;

namespace upload {
  class RiakSpoolerBackend : public AbstractSpoolerBackend {
   protected:
    struct RiakConfiguration {
      RiakConfiguration(const std::string &upstream_urls);

      std::string CreateRequestUrl(const std::string &key) const;

      std::string url;
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
    RiakSpoolerBackend(const std::string &upstream_urls);
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

    // static size_t CurlReadCallback(void   *ptr,
    //                                size_t  size,
    //                                size_t  nmemb,
    //                                void   *stream);

   private:
    RiakConfiguration config_;
    bool initialized_;

    CURL *curl_;
    struct curl_slist *http_headers_;
  };
}

#endif /* CVMFS_UPLOAD_RIAK_H_ */
