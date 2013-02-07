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
  /**
   * The RiakSpooler implements an upstream backend adapter for a Riak key/value
   * storage (see http://basho.com/products/riak-overview/ for details).
   * It implements Upload() concurrently using the ConcurrentWorkers template.
   *
   * For a detailed interface description of this class please have a look at the
   * AbstractSpooler class which it is derived from.
   */
  class RiakSpooler : public AbstractSpooler {
   public:
    /**
     * Encapsulates an extendable memory buffer.
     * consecutive calls to Copy() will copy the given memory into the buffer
     * without overwriting the previously copied data. This is very handy for
     * cURL-style data handling callbacks.
     *
     * Note: this class might be useful for other parts of the code as well,
     *       and might end up as a Util somewhen.
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


    //
    // -------------------------------------------------------------------------
    //


    /**
     * Implementation of ConcurrentWorker that pushes files into a Riak storage.
     * Currently this worker is based on cURL and uses the HTTP interface of
     * Riak. Possibly this will be extended by a Protocol Buffer implementation.
     */
    class UploadWorker : public ConcurrentWorker<UploadWorker> {
     public:
      struct Parameters {
        static const int kBulkFileChunk = -1;

        Parameters(const std::string &local_path,
                   const std::string &riak_key) :
          local_path(local_path),
          temporary_path(local_path),
          riak_key(riak_key),
          file_chunk_id(kBulkFileChunk),
          delete_after_upload(false),
          is_critical(true) {}

        Parameters(const std::string  &local_path,
                   const std::string  &temporary_path,
                   const std::string  &riak_key,
                   const int           file_chunk_id = kBulkFileChunk) :
          local_path(local_path),
          temporary_path(temporary_path),
          riak_key(riak_key),
          file_chunk_id(file_chunk_id),
          delete_after_upload(true),
          is_critical(false) {}

        /**
         * This constructor produces an empty upload_parameters structure which
         * is required by the implementation of the ConcurrentWorkers template
         * See the documentation of the ConcurrentWorkers template for details.
         */
        Parameters() :
          file_chunk_id(kBulkFileChunk),
          delete_after_upload(false),
          is_critical(false) {}

        inline bool IsFileChunk() const {
          return file_chunk_id != kBulkFileChunk;
        }

        const std::string  local_path;          //!< local path of file to be uploaded (for identification only)
        const std::string  temporary_path;      //!< location of the file to be uploaded into Riak
        const std::string  riak_key;            //!< Riak conform key the file should be stored under
        const int          file_chunk_id;       //!< used internally to relate to file chunk upload jobs
        const bool         delete_after_upload; //!< might unlink a file after upload (temporary files)
        const bool         is_critical;         //!< should the upload be performed with special care?
      };

      struct Results {
        Results(const std::string &local_path,
                const int          return_code,
                const int          file_chunk_id) :
          local_path(local_path),
          return_code(return_code),
          file_chunk_id(file_chunk_id) {}

        bool IsBulkFile() const { return file_chunk_id == Parameters::kBulkFileChunk; }
        bool IsSuccessful() const { return return_code == 0; }

        const std::string local_path;    //!< local path of the uploaded file (might also be just a chunk of it)
        const int         return_code;   //!< 0 if job was successful
        const int         file_chunk_id; //!< used internally to relate to file chunk upload jobs
      };

     public:
      typedef Parameters expected_data;
      typedef Results    returned_data;

      /**
       * Concurrent UploadWorkers need to communicate through this context
       * object, therefore it implements the Lockable interface to be used as a
       * mutex object in conjuction with the LockGuard template.
       */
      struct worker_context : public Lockable {
        worker_context(const std::vector<std::string> &upstream_urls) :
          upstream_urls(upstream_urls),
          next_upstream_url_(0) {}

        /**
         * Provides each concurrent UploadWorker with an upstream URL to one of
         * the configured Riak cluster instances. Upstream URLs are handed to the
         * upload workers in a round robin scheme.
         *
         * @return  an URL to a Riak cluster instance
         */
        const std::string& AcquireUpstreamUrl() const;

        const std::vector<std::string> upstream_urls;      //!< list of available upstream URLs
        mutable unsigned int           next_upstream_url_; //!< state variable for the round robin allocation
      };

     public:
      UploadWorker(const worker_context *context);
      void operator()(const expected_data &input);

      bool Initialize();
      void TearDown();

     protected:
      bool InitUploadHandle();
      bool InitDownloadHandle();

      /**
       * Performs a read action to a Riak cluster and obtains the vector clock
       * for a already present key. If the key is not present no vector clock
       * is returned.
       *
       * @param key           the Riak key to be queried
       * @param vector_clock  output parameter: is set to the vector clock string
       *                                        after a successful read
       * @return  true if the key was found (and a vector clock was set), other-
       *          wise the vector clock is not set. False could also mean failure
       */
      bool GetVectorClock(const std::string &key, std::string &vector_clock);

      /**
       * Pushes a file into the Riak data store under a given key. Furthermore
       * uploads can be marked as 'critical' meaning that they are ensured to be
       * consistent after the upload finished (w=all, dw=all)
       *
       * @param key          the key which should reference the data in the file
       * @param file_path    the path to the file to be stored into Riak
       * @param is_critical  a flag marking files as 'critical' (default = false)
       * @return             0 on success, > 0 otherwise
       */
      int PushFileToRiak(const std::string &key,
                         const std::string &file_path,
                         const bool         is_critical = false);

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

      /**
       * Configures the cURL_easy_handle for upload a planned upload of a file
       * into the Riak storage. This might include a read operation to a Riak
       * node in order to obtain the current vector clock of the entry to be up-
       * loaded.
       *
       * @param key        the Riak key to be created (or updated)
       * @param url        the full-blown url to send the request to
       * @param headers    a cURL list of headers to be sent
       * @param data_size  the file size of the file to be uploaded
       * @param callback   a pointer to a callback function to obtain data from
       *                   (can be used to upload a memory buffer)
       *                   can be set to NULL to upload a file
       * @param userdata   a pointer to the file handle to be uploaded or to
       *                   some user data to be passed to the callback
       * @return  true on successful configuration
       */
      bool ConfigureUpload(const std::string   &key,
                           const std::string   &url,
                           struct curl_slist   *headers,
                           const size_t         data_size,
                           const UploadCallback callback,
                           const void*          userdata);
      bool CheckUploadSuccess(const int file_size);

      bool CollectUploadStatistics();
      bool CollectVclockFetchStatistics();

      /**
       * cURL callback to extract the vector clock header from a received list
       * of headers.
       */
      static size_t ObtainVclockCallback(void *ptr,
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


    //
    // -------------------------------------------------------------------------
    //


   public:
    RiakSpooler(const SpoolerDefinition &spooler_definition);
    virtual ~RiakSpooler();

    static bool WillHandle(const SpoolerDefinition &spooler_definition);
    inline std::string name() const { return "Riak HTTP"; }

    /**
     * Schedules an asynchronous upload to a Riak storage.
     *
     * @param local_path   path to the file to be directly uploaded into Riak
     * @param remote_path  used to determine the Riak key to make the file avail-
     *                     able under a certain remote_path in Riak
     */
    void Upload(const std::string &local_path,
                const std::string &remote_path);
    void Upload(const FileProcessor::Results &data);

    void WaitForUpload() const;
    void WaitForTermination() const;

    unsigned int GetNumberOfErrors() const;

   protected:
    bool Initialize();
    void TearDown();

    /**
     * Callback method for the concurrent UploadWorker
     * Will inform the user about the outcome of a scheduled job.
     */
    void UploadWorkerCallback(const UploadWorker::Results &result);

    std::string MakeRiakKey(const std::string &path) const;
    std::string MakeRiakKey(const hash::Any   &hash,
                            const std::string &suffix = "") const;

   private:
    /**
     * Checks if the configuration of the Riak cluster conforms to our require-
     * ments.
     *
     * @param url  the URL of one of the cluster nodes to be checked
     * @return     true if the configuration is sound
     */
    static bool  CheckRiakConfiguration(const std::string &url);

    /**
     * Downloads the configuration information from the Riak cluster.
     *
     * @param url     the URL to one of the Riak cluster nodes to be checked
     * @param buffer  a DataBuffer to store the results into
     * @return  true on successful download
     */
    static bool  DownloadRiakConfiguration(const std::string &url,
                                           DataBuffer& buffer);

    /**
     * Parses the obtained configuration information as a JSON string and pro-
     * vides a simple JSON structure for further investigation. We use the slim
     * JSON library vjson for parsing (see: http://code.google.com/p/vjson/)
     *
     * @param buffer  the DataBuffer object containing the JSON string
     * @return  a pointer to a vjson JSON structure
     */
    static JSON* ParseJsonConfiguration(DataBuffer& buffer);

    /**
     * Checks the JSON configuration obtained from Riak and makes sure that it
     * is configured as we expect it to be.
     *
     * @param json_root  the JSON structure obtained from ParseJsonConfiguration
     * @return  true if the configuration matches or requirements
     */
    static bool CheckJsonConfiguration(const JSON *json_root);

   protected:
    /**
     * Attaches additional information to the SpoolerResult structure that is
     * needed to keep track of currently ongoing upload jobs.
     *
     * This is created BEFORE the actual upload jobs for this SpoolerResult are
     * scheduled. Upload results will drop in asynchronously in will be added in
     * RiakSpooler::UploadWorkerCallback.
     */
    struct PendingSpoolerResult : public SpoolerResult {
      enum ItemUploadState {
        kItemUploadPending,
        kItemUploadSuccessful,
        kItemUploadFailed
      };
      typedef std::vector<ItemUploadState> ItemUploadStates;

      // TODO: with C++11 this might not be needed
      //       one could use std::map::emplace in PendingSpoolerResults::Insert
      PendingSpoolerResult() :
        SpoolerResult(-1, "", hash::Any(), FileChunks()) {}

      PendingSpoolerResult(const std::string &local_path,
                           const hash::Any   &content_hash = hash::Any(),
                           const FileChunks  &file_chunks = FileChunks()) :
        SpoolerResult(-1, local_path, content_hash, file_chunks),
        bulk_upload_state(kItemUploadPending),
        chunk_upload_states(file_chunks.size(), kItemUploadPending),
        uploads_finished(0),
        errors(0) {}

      /**
       * Checks if this PendingSpoolerResult is ready to be delivered
       *
       * @return  true if all items of this SpoolerResults reported back
       */
      inline bool AllUploadsFinished() const {
        return uploads_finished == chunk_upload_states.size() + 1;
      }

      inline bool IsSuccessful() const {
        return AllUploadsFinished() && errors == 0;
      }

      /**
       * Analyzes the result of the PendingSpoolerResult and brings it into its
       * final state. Afterwards it can be safely returned through
       * AbstractSpooler::JobDone()
       *
       * Note: this method assumes that all items related to this SpoolerResult
       *       already reported back (see AllUploadsFinished())
       */
      void Finalize();

      ItemUploadState   bulk_upload_state;   //!< state flag for the bulk file upload
      ItemUploadStates  chunk_upload_states; //!< state flags for all generated file chunks

      unsigned int      uploads_finished;    //!< counter of how many callbacks this PendingSpoolerResult already received
      unsigned int      errors;              //!< counter how many of the associated upload jobs failed
    };

    /**
     * Wraps the mapping from local_path to PendingSpoolerResults to keep track
     * of the progressing update procedure.
     *
     * Note: This will be used in RiakSpooler::UploadWorkerCallback meaning that
     *       it might suffer from threading issues. PendingSpoolerResults is not
     *       thread safe by itself but implements Lockable.
     *       You should always use LockGuard<PendingSpoolerResults> to sync your
     *       accesses into this map.
     */
    class PendingSpoolerResults : public std::map<std::string, PendingSpoolerResult>,
                                  public Lockable {
     public:
      /**
       * Inserts a simple upload job (user called AbstractSpooler::Upload() )
       *
       * @param local_path  the local path of the file that is being uploaded
       */
      void Insert(const std::string &local_path);

      /**
       * Inserts a (possibly) complex upload job that might contain multiple
       * chunks (invoked indirectly through AbstractSpooler::Process() )
       *
       * @param data  the results structure produced by the FileProcessor
       */
      void Insert(const FileProcessor::Results &data);

      /**
       * Retrieves the PendingSpoolerResult reference mapped to local_path
       * Note: this will CRASH if local_path is NOT found in the map.
       *
       * @param local_path  the local path to be retrieved from the map
       */
      PendingSpoolerResult& Get(const std::string &local_path) const;

      /**
       * Erases the PendingSpoolerResult mapped to local_path from the map
       * Note: This will CRASH if local_path is NOT found in the map.
       *       Additionally you might still hold a reference obtained by Get(),
       *       that might get invalidated by invoking Erase(), keep in that mind
       *
       * @param local_path  the local path to be erased from the map
       */
      void Erase(const std::string &local_path);
    };

   private:
    // concurrency objects
    UniquePtr<ConcurrentWorkers<UploadWorker> >  concurrent_upload_;
    UniquePtr<UploadWorker::worker_context>      upload_context_;

    PendingSpoolerResults                        pending_results_; //!< list of currently pending upload jobs
  };
}

#endif /* CVMFS_UPLOAD_RIAK_H_ */
