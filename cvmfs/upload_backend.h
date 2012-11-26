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

  template <class PushWorkerT>
  class SpoolerBackend {
   public:
    SpoolerBackend(const std::string &spooler_description);
    virtual ~SpoolerBackend();

    bool Connect(const std::string &fifo_paths,
                 const std::string &fifo_digests);
    bool Initialize();
    int Run();

    virtual bool IsReady() const;

   protected:
    void EndOfTransaction();
    void Copy(const std::string &local_path,
              const std::string &remote_path,
              const bool move);
    void Process(const std::string &local_path,
                 const std::string &remote_dir,
                 const std::string &file_suffix,
                 const bool move);
    void Unknown();

    void SendResult(const int error_code,
                    const std::string &local_path = "",
                    const hash::Any &compressed_hash = hash::Any()) const;

   private:
    bool OpenPipes();
    bool GetString(FILE *f, std::string *str) const;

   private:
    const std::string spooler_description_;

    FILE *fpathes_;
    int fd_digests_;
    bool pipes_connected_;
    bool initialized_;
  };

}

#include "upload_backend_impl.h"

#endif /* CVMFS_UPLOAD_BACKEND_H_ */
