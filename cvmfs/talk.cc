/**
 * This file is part of the CernVM File System.
 *
 * Implements a socket interface to cvmfs.  This way commands can be send
 * to cvmfs.  When cvmfs is running, the socket
 * /var/cache/cvmfs2/$INSTANCE/cvmfs_io
 * is available for command input and reply messages, resp.
 *
 * Cvmfs comes with the cvmfs_talk script, that handles writing and reading the
 * socket.
 *
 * The talk module runs in a separate thread.
 */

#include "cvmfs_config.h"
#include "talk.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/uio.h>
#include <pthread.h>
#include <unistd.h>
#include <errno.h>

#include <cassert>
#include <cstdlib>

#include <string>
#include <vector>

#include "platform.h"
#include "tracer.h"
#include "quota.h"
#include "cvmfs.h"
#include "util.h"
#include "logging.h"
#include "download.h"
#include "duplex_sqlite3.h"
#include "shortstring.h"
#include "lru.h"
#include "nfs_maps.h"
#include "loader.h"
#include "options.h"
#include "cache.h"
#include "monitor.h"

using namespace std;  // NOLINT

namespace talk {

const unsigned kMaxCommandSize = 512;

string *cachedir_ = NULL;  /**< Stores the cache directory from cvmfs.
                                Pipe files will be created here. */
string *socket_path_ = NULL;  /**< $cache_dir/cvmfs_io */
int socket_fd_;
pthread_t thread_talk_;
bool spawned_;
bool initialized_ = false;


static void Answer(const int con_fd, const string &msg) {
  (void)send(con_fd, &msg[0], msg.length(), MSG_NOSIGNAL);
}


static void AnswerStringList(const int con_fd, const vector<string> &list) {
  string list_str;
  for (unsigned i = 0; i < list.size(); ++i) {
    list_str += list[i] + "\n";
  }
  Answer(con_fd, list_str);
}


static void *MainTalk(void *data __attribute__((unused))) {
  LogCvmfs(kLogTalk, kLogDebug, "talk thread started");

  struct sockaddr_un remote;
  socklen_t socket_size = sizeof(remote);
  int con_fd = -1;
  while (true) {
    if (con_fd > 0) {
      shutdown(con_fd, SHUT_RDWR);
      close(con_fd);
    }
    LogCvmfs(kLogTalk, kLogDebug, "accepting connections on socketfd %d",
             socket_fd_);
    if ((con_fd = accept(socket_fd_, (struct sockaddr *)&remote, &socket_size))
         < 0)
    {
      LogCvmfs(kLogTalk, kLogDebug, "terminating talk thread (fd %d, errno %d)",
               con_fd, errno);
      break;
    }

    char buf[kMaxCommandSize];
    if (recv(con_fd, buf, sizeof(buf), 0) > 0) {
      const string line = string(buf);
      LogCvmfs(kLogTalk, kLogDebug, "received %s", line.c_str());

      if (line == "tracebuffer flush") {
        tracer::Flush();
        Answer(con_fd, "OK\n");
      } else if (line == "cache size") {
        if (quota::GetCapacity() == 0) {
          Answer(con_fd, "Cache is unmanaged\n");
        } else {
          uint64_t size_unpinned = quota::GetSize();
          uint64_t size_pinned = quota::GetSizePinned();
          const string size_str = "Current cache size is " +
            StringifyInt(size_unpinned / (1024*1024)) + "MB (" +
            StringifyInt(size_unpinned) + " Bytes), pinned: " +
            StringifyInt(size_pinned / (1024*1024)) + "MB (" +
            StringifyInt(size_pinned) + " Bytes)\n";
          Answer(con_fd, size_str);
        }
      } else if (line == "cache list") {
        if (quota::GetCapacity() == 0) {
          Answer(con_fd, "Cache is unmanaged\n");
        } else {
          vector<string> ls = quota::List();
          AnswerStringList(con_fd, ls);
        }
      } else if (line == "cache list pinned") {
        if (quota::GetCapacity() == 0) {
          Answer(con_fd, "Cache is unmanaged\n");
        } else {
          vector<string> ls_pinned = quota::ListPinned();
          AnswerStringList(con_fd, ls_pinned);
        }
      } else if (line == "cache list catalogs") {
        if (quota::GetCapacity() == 0) {
          Answer(con_fd, "Cache is unmanaged\n");
        } else {
          vector<string> ls_catalogs = quota::ListCatalogs();
          AnswerStringList(con_fd, ls_catalogs);
        }
      } else if (line.substr(0, 7) == "cleanup") {
        if (quota::GetCapacity() == 0) {
          Answer(con_fd, "Cache is unmanaged\n");
        } else {
          if (line.length() < 9) {
            Answer(con_fd, "Usage: cleanup <MB>\n");
          } else {
            const uint64_t size = String2Uint64(line.substr(8))*1024*1024;
            if (quota::Cleanup(size)) {
              Answer(con_fd, "OK\n");
            } else {
              Answer(con_fd, "Not fully cleaned "
                     "(there might be pinned chunks)\n");
            }
          }
        }
      } else if (line == "mountpoint") {
        Answer(con_fd, *cvmfs::mountpoint_ + "\n");
      } else if (line == "remount") {
        catalog::LoadError result = cvmfs::RemountStart();
        switch (result) {
          case catalog::kLoadFail:
            Answer(con_fd, "Failed\n");
            break;
          case catalog::kLoadNoSpace:
            Answer(con_fd, "Failed (no space)\n");
            break;
          case catalog::kLoadUp2Date:
            Answer(con_fd, "Catalog up to date\n");
            break;
          case catalog::kLoadNew:
            Answer(con_fd, "New revision applied\n");
            break;
          default:
            Answer(con_fd, "internal error\n");
        }
      } else if (line == "revision") {
        Answer(con_fd, StringifyInt(cvmfs::GetRevision()) + "\n");
      } else if (line == "max ttl info") {
        const unsigned max_ttl = cvmfs::GetMaxTTL();
        if (max_ttl == 0) {
          Answer(con_fd, "unset\n");
        } else {
          const string max_ttl_str = StringifyInt(max_ttl) + " minutes\n";
          Answer(con_fd, max_ttl_str);
        }
      } else if (line.substr(0, 11) == "max ttl set") {
        if (line.length() < 13) {
          Answer(con_fd, "Usage: max ttl set <minutes>\n");
        } else {
          const unsigned max_ttl = String2Uint64(line.substr(12));
          cvmfs::SetMaxTTL(max_ttl);
          Answer(con_fd, "OK\n");
        }
      } else if (line == "host info") {
        vector<string> host_chain;
        vector<int> rtt;
        unsigned active_host;

        download::GetHostInfo(&host_chain, &rtt, &active_host);
        string host_str;
        for (unsigned i = 0; i < host_chain.size(); ++i) {
          host_str += "  [" + StringifyInt(i) + "] " + host_chain[i] + " (";
          if (rtt[i] == -1)
            host_str += "unprobed";
          else if (rtt[i] == -2)
            host_str += "host down";
          else
            host_str += StringifyInt(rtt[i]) + " ms";
          host_str += ")\n";
        }
        host_str += "Active host " + StringifyInt(active_host) + ": " +
                    host_chain[active_host] + "\n";
        Answer(con_fd, host_str);
      } else if (line == "host probe") {
        download::ProbeHosts();
        Answer(con_fd, "OK\n");
      } else if (line == "host switch") {
        download::SwitchHost();
        Answer(con_fd, "OK\n");
      } else if (line.substr(0, 8) == "host set") {
        if (line.length() < 10) {
          Answer(con_fd, "Usage: host set <host list>\n");
        } else {
          const string hosts = line.substr(9);
          download::SetHostChain(hosts);
          Answer(con_fd, "OK\n");
        }
      } else if (line == "proxy info") {
        vector< vector<string> > proxy_chain;
        unsigned active_group;
        download::GetProxyInfo(&proxy_chain, &active_group);

        string proxy_str;
        if (proxy_chain.size()) {
          proxy_str += "Load-balance groups:\n";
          for (unsigned i = 0; i < proxy_chain.size(); ++i) {
            proxy_str += "[" + StringifyInt(i) + "] " +
                         JoinStrings(proxy_chain[i], ", ") + "\n";
          }
          proxy_str += "Active proxy: [" + StringifyInt(active_group) + "] " +
                       proxy_chain[active_group][0] + "\n";
        } else {
          proxy_str = "No proxies defined\n";
        }

        Answer(con_fd, proxy_str);
      } else if (line == "proxy rebalance") {
        download::RebalanceProxies();
        Answer(con_fd, "OK\n");
      } else if (line == "proxy group switch") {
        download::SwitchProxyGroup();
        Answer(con_fd, "OK\n");
      } else if (line.substr(0, 9) == "proxy set") {
        if (line.length() < 11) {
          Answer(con_fd, "Usage: proxy set <proxy list>\n");
        } else {
          const string proxies = line.substr(10);
          download::SetProxyChain(proxies);
          Answer(con_fd, "OK\n");
        }
      } else if (line == "timeout info") {
        unsigned timeout;
        unsigned timeout_direct;
        download::GetTimeout(&timeout, &timeout_direct);
        string timeout_str =  "Timeout with proxy: ";
        if (timeout)
          timeout_str += StringifyInt(timeout) + "s\n";
        else
          timeout_str += "no timeout\n";
        timeout_str += "Timeout without proxy: ";
        if (timeout_direct)
          timeout_str += StringifyInt(timeout_direct) + "s\n";
        else
          timeout_str += "no timeout\n";
        Answer(con_fd, timeout_str);
      } else if (line.substr(0, 11) == "timeout set") {
        if (line.length() < 13) {
          Answer(con_fd, "Usage: timeout set <proxy> <direct>\n");
        } else {
          uint64_t timeout;
          uint64_t timeout_direct;
          String2Uint64Pair(line.substr(12), &timeout, &timeout_direct);
          download::SetTimeout(timeout, timeout_direct);
          Answer(con_fd, "OK\n");
        }
      } else if (line == "open catalogs") {
        Answer(con_fd, cvmfs::GetOpenCatalogs());
      } else if (line == "internal affairs") {
        int current;
        int highwater;
        lru::Statistics inode_stats;
        lru::Statistics path_stats;
        lru::Statistics md5path_stats;
        catalog::Statistics catalog_stats;
        string result;

        result += "Inode Generation:\n  " + cvmfs::PrintInodeGeneration();
        result += "File System Call Statistics:\n  " + cvmfs::GetFsStats();

        cvmfs::GetLruStatistics(&inode_stats, &path_stats, &md5path_stats);
        result += "File Catalog Memory Cache:\n" +
                  string("  inode cache:   ") + inode_stats.Print() +
                  string("  path cache:    ") + path_stats.Print() +
                  string("  md5path cache: ") + md5path_stats.Print();
        
        result += string("  glue buffer:   ") + 
                  cvmfs::PrintGlueBufferStatistics();
        result += string("  cwd buffer:    ") +  
          cvmfs::PrintCwdBufferStatistics();

        result += "File Catalogs:\n  " + cvmfs::GetCatalogStatistics().Print();
        result += "Certificate cache:\n  " + cvmfs::GetCertificateStats();

        result += "Path Strings:\n  instances: " +
          StringifyInt(PathString::num_instances()) + "  overflows: " +
          StringifyInt(PathString::num_overflows()) + "\n";
        result += "Name Strings:\n  instances: " +
          StringifyInt(NameString::num_instances()) + "  overflows: " +
          StringifyInt(NameString::num_overflows()) + "\n";
        result += "Symlink Strings:\n  instances: " +
          StringifyInt(LinkString::num_instances()) + "  overflows: " +
          StringifyInt(LinkString::num_overflows()) + "\n";

        result += "\nCache Mode: ";
        switch (cache::GetCacheMode()) {
          case cache::kCacheReadWrite:
            result += "read-write";
            break;
          case cache::kCacheReadOnly:
            result += "read-only";
            break;
          default:
            result += "unknown";
        }
        result += "\n";

        if (cvmfs::nfs_maps_) {
          result += "\nLEVELDB Statistics:\n";
          result += nfs_maps::GetStatistics();
        }

        result += "\nNetwork Statistics:\n";
        result += download::GetStatistics().Print();
        unsigned proxy_reset_delay;
        time_t proxy_timestamp_failover;
        download::GetProxyBackupInfo(&proxy_reset_delay,
                                     &proxy_timestamp_failover);
        result += "Backup proxy group: " + ((proxy_timestamp_failover > 0) ?
          ("Backup since " + StringifyTime(proxy_timestamp_failover, true)) :
          "Primary");
        result += "\n\n";

        result += "SQlite Statistics:\n";
        sqlite3_status(SQLITE_STATUS_MALLOC_COUNT, &current, &highwater, 0);
        result += "  Number of allocations " + StringifyInt(current) + "\n";

        sqlite3_status(SQLITE_STATUS_MEMORY_USED, &current, &highwater, 0);
        result += "  General purpose allocator " +StringifyInt(current/1024) +
                  " KB / " + StringifyInt(highwater/1024) + " KB\n";

        sqlite3_status(SQLITE_STATUS_MALLOC_SIZE, &current, &highwater, 0);
        result += "  Largest malloc " + StringifyInt(highwater) + " Bytes\n";

        sqlite3_status(SQLITE_STATUS_PAGECACHE_USED, &current, &highwater, 0);
        result += "  Page cache allocations " + StringifyInt(current) + " / " +
                  StringifyInt(highwater) + "\n";

        sqlite3_status(SQLITE_STATUS_PAGECACHE_OVERFLOW,
                       &current, &highwater, 0);
        result += "  Page cache overflows " + StringifyInt(current/1024) +
                  " KB / " + StringifyInt(highwater/1024) + " KB\n";

        sqlite3_status(SQLITE_STATUS_PAGECACHE_SIZE, &current, &highwater, 0);
        result += "  Largest page cache allocation " + StringifyInt(highwater) +
                  " Bytes\n";

        sqlite3_status(SQLITE_STATUS_SCRATCH_USED, &current, &highwater, 0);
        result += "  Scratch allocations " + StringifyInt(current) + " / " +
                  StringifyInt(highwater) + "\n";

        sqlite3_status(SQLITE_STATUS_SCRATCH_OVERFLOW, &current, &highwater, 0);
        result += "  Scratch overflows " + StringifyInt(current) + " / " +
                  StringifyInt(highwater) + "\n";

        sqlite3_status(SQLITE_STATUS_SCRATCH_SIZE, &current, &highwater, 0);
        result += "  Largest scratch allocation " + StringifyInt(highwater/1024)
                  + " KB\n";

        Answer(con_fd, result);
      } else if (line == "reset error counters") {
        cvmfs::ResetErrorCounters();
        Answer(con_fd, "OK\n");
      } else if (line == "pid") {
        const string pid_str = StringifyInt(cvmfs::pid_) + "\n";
        Answer(con_fd, pid_str);
      } else if (line == "pid cachemgr") {
        const string pid_str = StringifyInt(quota::GetPid()) + "\n";
        Answer(con_fd, pid_str);
      } else if (line == "pid watchdog") {
        const string pid_str = StringifyInt(monitor::GetPid()) + "\n";
        Answer(con_fd, pid_str);
      } else if (line == "parameters") {
        Answer(con_fd, options::Dump());
      } else if (line == "hotpatch history") {
        string history_str =
          StringifyTime(cvmfs::loader_exports_->boot_time, false) +
          "    (start of CernVM-FS loader " +
          cvmfs::loader_exports_->loader_version + ")\n";
        for (loader::EventList::const_iterator i =
             cvmfs::loader_exports_->history.begin(),
             iEnd = cvmfs::loader_exports_->history.end(); i != iEnd; ++i)
        {
          history_str += StringifyTime((*i)->timestamp, false) +
            "    (loaded CernVM-FS Fuse Module " +
            (*i)->so_version + ")\n";
        }
        Answer(con_fd, history_str);
      } else if (line == "version") {
        string version_str = string(VERSION) + " (CernVM-FS Fuse Module)\n" +
          cvmfs::loader_exports_->loader_version + " (Loader)\n";
        Answer(con_fd, version_str);
      } else if (line == "version patchlevel") {
        Answer(con_fd, string(CVMFS_PATCH_LEVEL) + "\n");
      } else if (line == "tear down to read-only") {
        cache::TearDown2ReadOnly();
        Answer(con_fd, "In read-only mode\n");
      } else {
        Answer(con_fd, "unknown command\n");
      }
    }
  }

  return NULL;
}


/**
 * Init the socket.
 */
bool Init(const string &cachedir) {
  if (initialized_) return true;
  spawned_ = false;
  cachedir_ = new string(cachedir);
  socket_path_ = new string(cachedir + "/cvmfs_io." + *cvmfs::repository_name_);

  socket_fd_ = MakeSocket(*socket_path_, 0660);
  if (socket_fd_ == -1)
    return false;

  if (listen(socket_fd_, 1) == -1)
    return false;

  LogCvmfs(kLogTalk, kLogDebug, "socket created at %s (fd %d)",
           socket_path_->c_str(), socket_fd_);

  initialized_ = true;
  return true;
}


/**
 * Spawns the socket listener
 */
void Spawn() {
  int result;
  result = pthread_create(&thread_talk_, NULL, MainTalk, NULL);
  assert(result == 0);
  spawned_ = true;
}


/**
 * Terminates command-listener thread.  Removes socket.
 */
void Fini() {
  if (!initialized_) return;
  int result;
  result = unlink(socket_path_->c_str());
  if (result != 0) {
    LogCvmfs(kLogTalk, kLogSyslog,
             "Could not remove cvmfs_io socket from cache directory.");
  }

  delete cachedir_;
  delete socket_path_;
  cachedir_ = NULL;
  socket_path_ = NULL;

  shutdown(socket_fd_, SHUT_RDWR);
  close(socket_fd_);
  if (spawned_) pthread_join(thread_talk_, NULL);
  LogCvmfs(kLogTalk, kLogDebug, "talk thread stopped");
  initialized_ = false;
}

}  // namespace talk
