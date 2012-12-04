#ifndef CVMFS_UPLOAD_RIAK_H_
#define CVMFS_UPLOAD_RIAK_H_

#include "upload_pushworker.h"

#include <vector>

typedef void CURL;
struct curl_slist;

namespace upload {
  class RiakPushWorker : public AbstractPushWorker {
   protected:
    class PushFinishedCallback {
     public:
      PushFinishedCallback(const RiakPushWorker *delegate,
                           const std::string        &local_path = "",
                           const hash::Any          &content_hash = hash::Any()) :
        delegate_(delegate),
        local_path_(local_path),
        content_hash_(content_hash) {}

      void operator()(const int return_code) const {
        //delegate_->SendResult(return_code, local_path_, content_hash_);
      }

     private:
      const RiakPushWorker *delegate_;
      const std::string         local_path_;
      const hash::Any           content_hash_;
    };


   public:
    /**
     * See AbstractPushWorker for description
     */
    struct Context : public AbstractPushWorker::ContextBase<SpoolerBackend<RiakPushWorker> > {
      Context(SpoolerBackend<RiakPushWorker> *master,
              const std::vector<std::string> &upstream_urls) :
        AbstractPushWorker::ContextBase<SpoolerBackend<RiakPushWorker> >(master),
        upstream_urls(upstream_urls) {}

      std::vector<std::string> upstream_urls;
    };

    /**
     * See AbstractPushWorker for description
     */
    static Context* GenerateContext(SpoolerBackend<RiakPushWorker> *master,
                                    const std::string              &upstream_urls);
    
    /**
     * See AbstractPushWorker for description
     */
    static int GetNumberOfWorkers(const Context *context);

   public:
    RiakPushWorker(Context* context);
    virtual ~RiakPushWorker();

    bool Initialize();
    bool IsReady() const;

    bool ProcessJob(StorageJob *job);

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
    std::string CreateRequestUrl(const std::string &key) const;

    void PushFileToRiakAsync(const std::string          &key,
                             const std::string          &file_path,
                             const PushFinishedCallback &callback);

    // static size_t CurlReadCallback(void   *ptr,
    //                                size_t  size,
    //                                size_t  nmemb,
    //                                void   *stream);

   private:
    Context *context_;
    bool initialized_;
    std::string upstream_url_;

    CURL *curl_;
    struct curl_slist *http_headers_;
  };
}

#endif /* CVMFS_UPLOAD_RIAK_H_ */
