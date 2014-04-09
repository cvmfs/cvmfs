/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_S3_H_
#define CVMFS_UPLOAD_S3_H_

#include "upload_facility.h"

namespace s3fanout {
  class S3FanoutManager;
}

namespace upload
{

  struct S3StreamHandle : public UploadStreamHandle {
      S3StreamHandle(const callback_t   *commit_callback,
                     const int           tmp_fd,
                     const std::string  &tmp_path) :
      UploadStreamHandle(commit_callback),
      file_descriptor(tmp_fd),
      temporary_path(tmp_path) {}

    const int         file_descriptor;
    const std::string temporary_path;
  };


  /**
   * The S3Spooler implements the AbstractSpooler interface to push files
   * into a S3 CVMFS repository backend.
   * For a detailed description of the classes interface please have a look into
   * the AbstractSpooler base class.
   */
  class S3Uploader : public AbstractUploader {
   public:
    S3Uploader(const SpoolerDefinition &spooler_definition);
    static bool WillHandle(const SpoolerDefinition &spooler_definition);

    inline std::string name() const { return "S3"; }

    /**
     * Upload() is not done concurrently in the current implementation of the
     * S3Spooler, since it is a simple move or copy of a file without CPU
     * intensive operation
     * This method calls NotifyListeners and invokes a callback for all
     * registered listeners (see the Observable template for details).
     */
    void FileUpload(const std::string  &local_path,
		    const std::string  &remote_path,
		    const callback_t   *callback = NULL);

    UploadStreamHandle* InitStreamedUpload(const callback_t *callback = NULL);
    void Upload(UploadStreamHandle  *handle,
                CharBuffer          *buffer,
                const callback_t    *callback = NULL);
    void FinalizeStreamedUpload(UploadStreamHandle *handle,
                                const shash::Any    content_hash,
                                const std::string   hash_suffix);

    bool Remove(const std::string &file_to_delete);

    bool Peek(const std::string& path) const;

    /**
     * Determines the number of failed jobs in the S3CompressionWorker as
     * well as in the Upload() command.
     */
    unsigned int GetNumberOfErrors() const;

   protected:
    void WorkerThread();

    int Move(const std::string &local_path,
             const std::string &remote_path) const;

    int CreateAndOpenTemporaryChunkFile(std::string *path) const;

   private:
    s3fanout::S3FanoutManager *s3fanout_mgr;
    bool ParseSpoolerDefinition(const SpoolerDefinition &spooler_definition);
    int uploadFile(std::string       filename,
		   char              *buff,
		   unsigned long     size_of_file,
		   const callback_t  *callback,
		   MemoryMappedFile  *mmf);

    int getKeysAndBucket(const std::string filename,
			 std::string &access_key,
			 std::string &secret_key,
			 std::string &bucket_name);
    std::string getBucketName(unsigned int use_bucket);
    int select_bucket(std::string rem_filename);
    int getKeyIndex(unsigned int use_bucket);

    // state information
    std::string full_host_name_;
    std::string host_name_;
    std::string bucket_body_name_;
    int         number_of_buckets_;
    int         maximum_number_of_parallell_uploads_;
    std::vector< std::pair<std::string, std::string> > keys_;

    const std::string    upstream_path_;
    const std::string    temporary_path_;
    mutable atomic_int32 copy_errors_;   //!< counts the number of occured
                                         //!< errors in Upload()
  };
}

#endif /* CVMFS_UPLOAD_S3_H_ */
