// /**
//  * This file is part of the CernVM File System.
//  */

// #ifndef CVMFS_UPLOAD_RIAK_H_
// #define CVMFS_UPLOAD_RIAK_H_

// #include "upload_pushworker.h"

// #include <vector>

// typedef void CURL;
// struct curl_slist;

// struct json_value;
// typedef struct json_value JSON;

// namespace upload {
//   class RiakPushWorker : public AbstractPushWorker {
//    public:
//     /**
//      * See AbstractPushWorker for description
//      */
//     class Context : public AbstractPushWorker::ContextBase<SpoolerImpl<RiakPushWorker> > {
//      public:
//       Context(SpoolerImpl<RiakPushWorker> *master,
//               const std::vector<std::string> &upstream_urls,
//               const size_t compression_buffer_size) :
//         AbstractPushWorker::ContextBase<SpoolerImpl<RiakPushWorker> >(master),
//         upstream_urls(upstream_urls),
//         compression_buffer_size(compression_buffer_size),
//         next_upstream_url_(0) {}

//       const std::string& AcquireUpstreamUrl() const;

//      public:
//       const std::vector<std::string> upstream_urls;
//       const size_t                   compression_buffer_size;

//      private:
//       mutable int next_upstream_url_;
//     };

//     /**
//      * See AbstractPushWorker for description
//      */
//     static Context* GenerateContext(SpoolerImpl<RiakPushWorker>      *master,
//                                     const Spooler::SpoolerDefinition &spooler_definition);
    
//     /**
//      * See AbstractPushWorker for description
//      */
//     static int GetNumberOfWorkers(const Context *context);

//     /**
//      * See AbstractPushWorker for description
//      *
//      * This method essentially calls curl_global_init() and checks the Riak
//      * configuration
//      */
//     static bool DoGlobalInitialization(const Context *context);

//     /**
//      * See AbstractPushWorker for description
//      *
//      * This method essentially calls curl_global_cleanup()
//      */
//     static void DoGlobalCleanup();

//    public:
//     RiakPushWorker(Context* context);
//     virtual ~RiakPushWorker();

//     bool Initialize();
//     bool IsReady() const;

//    protected:
//     void ProcessCopyJob(StorageCopyJob *copy_job);
//     void ProcessCompressionJob(StorageCompressionJob *compression_job);

//     bool CompressToTempFile(const std::string &source_file_path,
//                             const std::string &destination_dir,
//                             std::string       *tmp_file_path,
//                             hash::Any         *content_hash) const;

//     bool InitUploadHandle();
//     bool InitDownloadHandle();

//     bool GetVectorClock(const std::string &key, std::string &vector_clock);

//     /**
//      * Pushes a file into the Riak data store under a given key. Furthermore
//      * uploads can be marked as 'critical' meaning that they are ensured to be
//      * consistent after the upload finished
//      * @param key          the key which should reference the data in the file
//      * @param file_path    the path to the file to be stored into Riak
//      * @param is_critical  a flag marking files as 'critical' (default = false)
//      * @return             0 on success, > 0 otherwise
//      */
//     int PushFileToRiak(const std::string &key,
//                        const std::string &file_path,
//                        const bool         is_critical = false);
//     int PushMemoryToRiak(const std::string   &key,
//                          const unsigned char *mem,
//                          const size_t         size,
//                          const bool           is_critical = false);

//     std::string GenerateRiakKey(const StorageCompressionJob *compression_job) const;
//     std::string GenerateRiakKey(const std::string &remote_path) const;

//     std::string GenerateRandomKey() const;

//     *
//      * Generates a request URL out of the known Riak base URL and the given key.
//      * Additionally it can set the W-value to 'all' if a consistent write must
//      * be ensured. (see http://docs.basho.com/riak/1.2.1/tutorials/
//      *                  fast-track/Tunable-CAP-Controls-in-Riak/ for details)
//      * @param key          the key where the request URL should point to
//      * @param is_critical  set to true if a consistent write is desired
//      *                     (sets Riak's w_val to 'all')
//      * @return             the final request URL string
     
//     std::string CreateRequestUrl(const std::string &key,
//                                  const bool         is_critical = false) const;

//     typedef size_t (*UploadCallback)(void*, size_t, size_t, void*);
//     bool ConfigureUpload(const std::string   &key,
//                          const std::string   &url,
//                          struct curl_slist   *headers,
//                          const size_t         data_size, 
//                          const UploadCallback callback,
//                          const void*          userdata);
//     bool CheckUploadSuccess(const int file_size);

//    protected:
//     /**
//      * Encapsulates an extendable memory buffer.
//      * consecutive calls to Copy() will copy the given memory into the buffer
//      * without overwriting the previously copied data. This is very handy for
//      * cURL-style data handling callbacks.
//      */
//     struct DataBuffer {
//       DataBuffer() : data(NULL), size_(0), offset_(0) {}
//       ~DataBuffer() { free(data); data = NULL; size_ = 0; offset_ = 0; }

//       bool           Reserve(const size_t bytes);
//       unsigned char* Position() const;
//       void           Copy(const unsigned char* ptr, const size_t bytes);

//       unsigned char* data;
//       size_t         size_;
//       unsigned int   offset_;
//     };

//    private:
//     static bool  CheckRiakConfiguration(const Context *context);
//     static bool  DownloadRiakConfiguration(const std::string &url,
//                                            DataBuffer& buffer);
//     static JSON* ParseJsonConfiguration(DataBuffer& buffer);
//     static bool  CheckJsonConfiguration(const JSON *json_root);

//     bool CollectUploadStatistics();
//     bool CollectVclockFetchStatistics();

//     // cURL callback methods
//     static size_t ObtainVclockCallback(void *ptr, 
//                                        size_t size,
//                                        size_t nmemb,
//                                        void *userdata);
//     static size_t WriteMemoryCallback(void *ptr,
//                                       size_t size,
//                                       size_t nmemb,
//                                       void *userdata);
//     static size_t ReceiveDataCallback(void *ptr,
//                                       size_t size,
//                                       size_t nmemb,
//                                       void *userdata);

//    private:
//     // state information
//     Context *context_;
//     bool initialized_;
//     std::string upstream_url_;

//     // CURL state
//     CURL *curl_upload_;
//     CURL *curl_download_;
//     struct curl_slist *http_headers_download_;

//     // in-memory compression buffer
//     unsigned char *compression_buffer_;
//     const size_t   compression_buffer_size_;

//     // instrumentation
//     StopWatch compression_stopwatch_;
//     StopWatch upload_stopwatch_;
//     double compression_time_aggregated_;
//     double upload_time_aggregated_;
//     double curl_upload_time_aggregated_;
//     double curl_get_vclock_time_aggregated_;
//     double curl_connection_time_aggregated_;
//     int    curl_connections_;
//     double curl_upload_speed_aggregated_;
//     int upload_jobs_count_;
//   };
// }

// #endif /* CVMFS_UPLOAD_RIAK_H_ */
