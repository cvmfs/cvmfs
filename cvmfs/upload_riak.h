/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_RIAK_H_
#define CVMFS_UPLOAD_RIAK_H_

#include <vector>

#include "upload.h"

#include "util_concurrency.h"

typedef void CURL;
struct curl_slist;

struct json_value;
typedef struct json_value JSON;

namespace upload {
  class RiakSpooler : public AbstractSpooler {
   public:
    /**
     * Encapsulates an extendable memory buffer.
     * consecutive calls to Copy() will copy the given memory into the buffer
     * without overwriting the previously copied data. This is very handy for
     * cURL-style data handling callbacks.
     */
    struct DataBuffer {
      DataBuffer() : data(NULL), size_(0), offset_(0) {}
      ~DataBuffer() { free(data); data = NULL; size_ = 0; offset_ = 0; }

      bool           Reserve(const size_t bytes);
      unsigned char* Position() const;
      void           Copy(const unsigned char* ptr, const size_t bytes);

      unsigned char* data;
      size_t         size_;
      unsigned int   offset_;
    };

   protected:
    struct upload_parameters : public SpoolerResult {
      enum JobType { kPlainUpload,  kCompressedUpload, kEmpty };

      upload_parameters(const std::string  &local_path,
                        const std::string  &remote_path,
                        const bool          move) :
        SpoolerResult(0, local_path),
        type(kPlainUpload),
        upload_source_path(local_path),
        remote_path(remote_path),
        move(move) {}

      upload_parameters(const int           return_code,
                        const std::string  &local_path) :
        SpoolerResult(return_code, local_path),
        type(kEmpty),
        move(false) {}

      upload_parameters(const int           return_code,
                        const std::string  &local_path,
                        const std::string  &compressed_path,
                        const hash::Any    &content_hash,
                        const std::string  &file_suffix,
                        const bool          move) :
        SpoolerResult(return_code, local_path, content_hash),
        type(kCompressedUpload),
        upload_source_path(compressed_path),
        file_suffix(file_suffix),
        move(move) {}

      upload_parameters() :
        SpoolerResult(),
        type(kEmpty),
        move(false) {}

      std::string GetRiakKey() const;

      const JobType     type;
      const std::string upload_source_path;
      const std::string remote_path;
      const std::string file_suffix;
      const bool        move;
    };


    class CompressionWorker : public ConcurrentWorker<CompressionWorker> {
     public:
      typedef compression_parameters expected_data;
      typedef upload_parameters      returned_data;

      struct worker_context {
        worker_context(const std::string &temp_directory) :
          temp_directory(temp_directory) {}

        const std::string temp_directory;
      };

     public:
      CompressionWorker(const worker_context *context);
      void operator()(const expected_data &input);

     protected:
      bool CompressToTempFile(const std::string &source_file_path,
                              const std::string &destination_dir,
                              std::string       *tmp_file_path,
                              hash::Any         *content_hash) const;

     private:
      StopWatch         compression_stopwatch_;
      double            compression_time_aggregated_;
      const std::string temp_directory_;
    };


    class UploadWorker : public ConcurrentWorker<UploadWorker> {
     public:
      typedef upload_parameters      expected_data;
      typedef SpoolerResult          returned_data;

      struct worker_context : public Lockable {
        worker_context(const std::vector<std::string> &upstream_urls) :
          upstream_urls(upstream_urls),
          next_upstream_url_(0) {}

        const std::string& AcquireUpstreamUrl() const;

        const std::vector<std::string> upstream_urls;
        mutable unsigned int next_upstream_url_;
      };

     public:
      UploadWorker(const worker_context *context);
      void operator()(const expected_data &input);

      bool Initialize();
      void TearDown();

     protected:
      bool InitUploadHandle();
      bool InitDownloadHandle();

      bool GetVectorClock(const std::string &key, std::string &vector_clock);

      /**
       * Pushes a file into the Riak data store under a given key. Furthermore
       * uploads can be marked as 'critical' meaning that they are ensured to be
       * consistent after the upload finished
       * @param key          the key which should reference the data in the file
       * @param file_path    the path to the file to be stored into Riak
       * @param is_critical  a flag marking files as 'critical' (default = false)
       * @return             0 on success, > 0 otherwise
       */
      int PushFileToRiak(const std::string &key,
                         const std::string &file_path,
                         const bool         is_critical = false);
      int PushMemoryToRiak(const std::string   &key,
                           const unsigned char *mem,
                           const size_t         size,
                           const bool           is_critical = false);

      std::string GenerateRandomKey() const;

      /*
       * Generates a request URL out of the known Riak base URL and the given key.
       * Additionally it can set the W-value to 'all' if a consistent write must
       * be ensured. (see http://docs.basho.com/riak/1.2.1/tutorials/
       *                  fast-track/Tunable-CAP-Controls-in-Riak/ for details)
       * @param key          the key where the request URL should point to
       * @param is_critical  set to true if a consistent write is desired
       *                     (sets Riak's w_val to 'all')
       * @return             the final request URL string
       */
      std::string CreateRequestUrl(const std::string &key,
                                   const bool         is_critical = false) const;

      typedef size_t (*UploadCallback)(void*, size_t, size_t, void*);
      bool ConfigureUpload(const std::string   &key,
                           const std::string   &url,
                           struct curl_slist   *headers,
                           const size_t         data_size, 
                           const UploadCallback callback,
                           const void*          userdata);
      bool CheckUploadSuccess(const int file_size);

      bool CollectUploadStatistics();
      bool CollectVclockFetchStatistics();

      static size_t ObtainVclockCallback(void *ptr, 
                                         size_t size,
                                         size_t nmemb,
                                         void *userdata);
      static size_t WriteMemoryCallback(void *ptr,
                                        size_t size,
                                        size_t nmemb,
                                        void *userdata);

     private:
      // general state information
      const std::string upstream_url_;

      // CURL state
      CURL              *curl_upload_;
      CURL              *curl_download_;
      struct curl_slist *http_headers_download_;

      // instrumentation
      StopWatch upload_stopwatch_;
      double    upload_time_aggregated_;
      double    curl_upload_time_aggregated_;
      double    curl_get_vclock_time_aggregated_;
      double    curl_connection_time_aggregated_;
      int       curl_connections_;
      double    curl_upload_speed_aggregated_;
    };


   public:
    RiakSpooler(const SpoolerDefinition &spooler_definition);
    virtual ~RiakSpooler();

    void Copy(const std::string &local_path,
              const std::string &remote_path);
    void Process(const std::string &local_path,
                 const std::string &remote_dir,
                 const std::string &file_suffix);

    void EndOfTransaction();
    void WaitForUpload() const;
    void WaitForTermination() const;

    unsigned int GetNumberOfErrors() const;

   protected:
    bool Initialize();
    void TearDown();

    void CompressionWorkerCallback(const CompressionWorker::returned_data &data);
    void UploadWorkerCallback(const UploadWorker::returned_data &data);

   private:
    static bool  CheckRiakConfiguration(const std::string &url);
    static bool  DownloadRiakConfiguration(const std::string &url,
                                           DataBuffer& buffer);
    static JSON* ParseJsonConfiguration(DataBuffer& buffer);
    static bool  CheckJsonConfiguration(const JSON *json_root);

   private:
    // concurrency objects
    UniquePtr<ConcurrentWorkers<CompressionWorker> > concurrent_compression_;
    UniquePtr<ConcurrentWorkers<UploadWorker> >      concurrent_upload_;

    UniquePtr<CompressionWorker::worker_context>     compression_context_;
    UniquePtr<UploadWorker::worker_context>          upload_context_;
  };
}

#endif /* CVMFS_UPLOAD_RIAK_H_ */
