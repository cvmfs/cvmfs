#ifndef CVMFS_UPLOAD_BACKEND_H_
#define CVMFS_UPLOAD_BACKEND_H_

#include <string>
#include <cstdio>

#include "hash.h"

namespace upload
{
  /**
   * Commands to the spooler
   */
  enum Commands {
    kCmdProcess           = 1,
    kCmdCopy,
    kCmdEndOfTransaction,
    kCmdMoveFlag          = 128,
  };

  class AbstractSpoolerBackend
  {
   public:
    virtual ~AbstractSpoolerBackend();
    bool Connect(const std::string &fifo_paths,
                 const std::string &fifo_digests);
    virtual bool Initialize();
    int Run();

    virtual bool IsReady() const;

   protected:
    AbstractSpoolerBackend();

    virtual void EndOfTransaction();
    virtual void Copy(const std::string &local_path,
                      const std::string &remote_path,
                      const bool move) = 0;
    virtual void Process(const std::string &local_path,
                         const std::string &remote_dir,
                         const std::string &file_suffix,
                         const bool move) = 0;
    virtual void Unknown();

    void SendResult(const int error_code,
                    const std::string &local_path = "",
                    const hash::Any &compressed_hash = hash::Any()) const;

    /**
     * Creates a temporary file at a given location and stores the compressed
     * source file in it. Additionally it computes the content hash of the
     * compression result.
     * Note: It is the user's responsibility to remove the temporary file if
     *       applicable.
     * @param source_file_path   path to the file to be compressed
     * @param destination_dir    the desired location of the temporary file
     *                           used to store the result of the compression
     * @param tmp_file_path      pointer to a string that will contain the path
     *                           to the resulting temporary file
     * @param content_hash       pointer to a hash structure, that will contain
     *                           the content_hash of tmp_file_path afterwards
     * @return   true on success, false otherwise
     *
     */
    bool CompressToTempFile(const std::string &source_file_path,
                            const std::string &destination_dir,
                            std::string       *tmp_file_path,
                            hash::Any         *content_hash) const;

   private:
    bool OpenPipes();
    bool GetString(FILE *f, std::string *str) const;

   private:
    FILE *fpathes_;
    int fd_digests_;
    bool pipes_connected_;
    bool initialized_;
  };
}

#endif /* CVMFS_UPLOAD_BACKEND_H_ */
