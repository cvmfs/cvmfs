#ifndef CVMFS_UPLOAD_RIAK_H_
#define CVMFS_UPLOAD_RIAK_H_

#include "upload_pushworker.h"

#include <vector>

typedef void CURL;
struct curl_slist;

namespace upload {
  class RiakPushWorker : public AbstractPushWorker {
   public:
    /**
     * See AbstractPushWorker for description
     */
    class Context : public AbstractPushWorker::ContextBase<SpoolerBackend<RiakPushWorker> > {
     public:
      Context(SpoolerBackend<RiakPushWorker> *master,
              const std::vector<std::string> &upstream_urls) :
        AbstractPushWorker::ContextBase<SpoolerBackend<RiakPushWorker> >(master),
        upstream_urls(upstream_urls),
        next_upstream_url_(0) {}

      const std::string& AcquireUpstreamUrl() const;

     public:
      const std::vector<std::string> upstream_urls;

     private:
      mutable int next_upstream_url_;
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

    /**
     * See AbstractPushWorker for description
     *
     * This method essentially calls curl_global_init()
     */
    static bool DoGlobalInitialization();

    /**
     * See AbstractPushWorker for description
     *
     * This method essentially calls curl_global_cleanup()
     */
    static void DoGlobalCleanup();

   public:
    RiakPushWorker(Context* context);
    virtual ~RiakPushWorker();

    bool Initialize();
    bool IsReady() const;

   protected:
    void ProcessCopyJob(StorageCopyJob *copy_job);
    void ProcessCompressionJob(StorageCompressionJob *compression_job);

    bool CompressToTempFile(const std::string &source_file_path,
                            const std::string &destination_dir,
                            std::string       *tmp_file_path,
                            hash::Any         *content_hash) const;

    std::string GenerateRiakKey(const StorageCompressionJob *compression_job) const;
    std::string GenerateRiakKey(const std::string &remote_path) const;
    std::string CreateRequestUrl(const std::string &key) const;

    int PushFileToRiak(const std::string &key, const std::string &file_path);

   private:
    Context *context_;
    bool initialized_;
    std::string upstream_url_;

    CURL *curl_;
    struct curl_slist *http_headers_;
  };
}

#endif /* CVMFS_UPLOAD_RIAK_H_ */
