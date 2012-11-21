#ifndef CVMFS_UPLOAD_BACKEND_H_
#define CVMFS_UPLOAD_BACKEND_H_

#include <string>
#include <cstdio>

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

    virtual void EndOfTransaction(std::string &response);
    virtual void Copy(const std::string &local_path,
                      const std::string &remote_path,
                      const bool move,
                      std::string &response) = 0;
    virtual void Process(const std::string &local_path,
                         const std::string &remote_dir,
                         const std::string &file_suffix,
                         const bool move,
                         std::string &response) = 0;
    virtual void Unknown(std::string &response);

    void CreateResponseMessage(std::string &response,
                               const int error_code,
                               const std::string &local_path,
                               const std::string &compressed_hash) const;

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
