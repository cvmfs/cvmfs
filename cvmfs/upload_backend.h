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
    void Run();

    virtual bool IsReady() const;

   protected:
    AbstractSpoolerBackend();

    virtual void EndOfTransaction(std::string &response) = 0;
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

    virtual bool Compress();

   private:
    bool OpenPipes();
    bool GetString(FILE *f, std::string *str) const;

   private:
    FILE *fpathes_;
    int fd_digests_;
    bool pipes_connected_;
    bool initialized_;
  };

  class LocalSpoolerBackend : AbstractSpoolerBackend
  {
   public:
    LocalSpoolerBackend();
    virtual ~LocalSpoolerBackend();
    bool Initialize();

    bool IsReady() const;

    /**
     *  This method configures the upstream path for the local repository
     *  location. Call this once after you created this SpoolerBackend, then
     *  call initialize and you should be ready to go.
     *  @param upstream_path      the upstream path to be configured
     */
    void set_upstream_path(const std::string &upstream_path);
   
   protected:
    void EndOfTransaction(std::string &response);
    void Copy(const std::string &local_path,
              const std::string &remote_path,
              const bool move,
              std::string &response);
    void Process(const std::string &local_path,
                 const std::string &remote_dir,
                 const std::string &file_suffix,
                 const bool move,
                 std::string &response);

   private:
    std::string upstream_path_;
    bool initialized_;
  };

  class RiakSpoolerBackend : AbstractSpoolerBackend
  {
   public:
    RiakSpoolerBackend();
    virtual ~RiakSpoolerBackend();
    bool Initialize();

    bool IsReady() const;

   protected:
    void EndOfTransaction(std::string &response);
    void Copy(const std::string &local_path,
              const std::string &remote_path,
              const bool move,
              std::string &response);
    void Process(const std::string &local_path,
                 const std::string &remote_dir,
                 const std::string &file_suffix,
                 const bool move,
                 std::string &response);

   private:
    bool initialized_;
  };
}