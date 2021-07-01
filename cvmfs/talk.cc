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

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "cvmfs_config.h"
#include "talk.h"

#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdint.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <unistd.h>

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "cache.h"
#include "cache_posix.h"
#include "catalog_mgr_client.h"
#include "cvmfs.h"
#include "download.h"
#include "duplex_sqlite3.h"
#include "fuse_remount.h"
#include "glue_buffer.h"
#include "loader.h"
#include "logging.h"
#include "monitor.h"
#include "mountpoint.h"
#include "nfs_maps.h"
#include "options.h"
#include "platform.h"
#include "quota.h"
#include "shortstring.h"
#include "statistics.h"
#include "tracer.h"
#include "util/pointer.h"
#include "wpad.h"

using namespace std;  // NOLINT


void TalkManager::Answer(int con_fd, const string &msg) {
  (void)send(con_fd, &msg[0], msg.length(), MSG_NOSIGNAL);
}


void TalkManager::AnswerStringList(int con_fd, const vector<string> &list) {
  string list_str;
  for (unsigned i = 0; i < list.size(); ++i) {
    list_str += list[i] + "\n";
  }
  Answer(con_fd, list_str);
}


TalkManager *TalkManager::Create(
  const string &socket_path,
  MountPoint *mount_point,
  FuseRemounter *remounter)
{
  UniquePtr<TalkManager>
    talk_manager(new TalkManager(socket_path, mount_point, remounter));

  talk_manager->socket_fd_ = MakeSocket(socket_path, 0660);
  if (talk_manager->socket_fd_ == -1)
    return NULL;
  if (listen(talk_manager->socket_fd_, 1) == -1)
    return NULL;

  LogCvmfs(kLogTalk, kLogDebug, "socket created at %s (fd %d)",
           socket_path.c_str(), talk_manager->socket_fd_);

  return talk_manager.Release();
}


string TalkManager::FormatHostInfo(download::DownloadManager *download_mgr) {
  vector<string> host_chain;
  vector<int> rtt;
  unsigned active_host;

  download_mgr->GetHostInfo(&host_chain, &rtt, &active_host);
  string host_str;
  for (unsigned i = 0; i < host_chain.size(); ++i) {
    host_str += "  [" + StringifyInt(i) + "] " + host_chain[i] + " (";
    if (rtt[i] == download::DownloadManager::kProbeUnprobed)
      host_str += "unprobed";
    else if (rtt[i] == download::DownloadManager::kProbeDown)
      host_str += "host down";
    else if (rtt[i] == download::DownloadManager::kProbeGeo)
      host_str += "geographically ordered";
    else
      host_str += StringifyInt(rtt[i]) + " ms";
    host_str += ")\n";
  }
  host_str += "Active host " + StringifyInt(active_host) + ": " +
              host_chain[active_host] + "\n";
  return host_str;
}

string TalkManager::FormatProxyInfo(download::DownloadManager *download_mgr) {
  vector< vector<download::DownloadManager::ProxyInfo> > proxy_chain;
  unsigned active_group;
  unsigned fallback_group;

  download_mgr->GetProxyInfo(&proxy_chain, &active_group, &fallback_group);
  string proxy_str;
  if (proxy_chain.size()) {
    proxy_str += "Load-balance groups:\n";
    for (unsigned i = 0; i < proxy_chain.size(); ++i) {
      vector<string> urls;
      for (unsigned j = 0; j < proxy_chain[i].size(); ++j) {
        urls.push_back(proxy_chain[i][j].Print());
      }
      proxy_str +=
        "[" + StringifyInt(i) + "] " + JoinStrings(urls, ", ") + "\n";
    }
    proxy_str += "Active proxy: [" + StringifyInt(active_group) + "] " +
                 proxy_chain[active_group][0].url + "\n";
    if (fallback_group < proxy_chain.size())
      proxy_str += "First fallback group: [" +
                   StringifyInt(fallback_group) + "]\n";
  } else {
    proxy_str = "No proxies defined\n";
  }
  return proxy_str;
}


/**
 * Listener thread on the socket.
 * TODO(jblomer): create Format... helpers to shorten this method
 */
void *TalkManager::MainResponder(void *data) {
  TalkManager *talk_mgr = reinterpret_cast<TalkManager *>(data);
  MountPoint *mount_point = talk_mgr->mount_point_;
  FileSystem *file_system = mount_point->file_system();
  FuseRemounter *remounter = talk_mgr->remounter_;
  LogCvmfs(kLogTalk, kLogDebug, "talk thread started");

  struct sockaddr_un remote;
  socklen_t socket_size = sizeof(remote);
  int con_fd = -1;
  while (true) {
    if (con_fd >= 0) {
      shutdown(con_fd, SHUT_RDWR);
      close(con_fd);
    }
    LogCvmfs(kLogTalk, kLogDebug, "accepting connections on socketfd %d",
             talk_mgr->socket_fd_);
    if ((con_fd = accept(talk_mgr->socket_fd_,
                         (struct sockaddr *)&remote,
                         &socket_size)) < 0)
    {
      LogCvmfs(kLogTalk, kLogDebug, "terminating talk thread (fd %d, errno %d)",
               con_fd, errno);
      break;
    }

    char buf[kMaxCommandSize];
    int bytes_read;
    if ((bytes_read = recv(con_fd, buf, sizeof(buf), 0)) <= 0)
      continue;

    if (buf[bytes_read-1] == '\0')
      bytes_read--;
    const string line = string(buf, bytes_read);
    LogCvmfs(kLogTalk, kLogDebug, "received %s (length %u)",
             line.c_str(), line.length());

    if (line == "tracebuffer flush") {
      mount_point->tracer()->Flush();
      talk_mgr->Answer(con_fd, "OK\n");
    } else if (line == "cache size") {
      QuotaManager *quota_mgr = file_system->cache_mgr()->quota_mgr();
      if (!quota_mgr->HasCapability(QuotaManager::kCapIntrospectSize)) {
        talk_mgr->Answer(con_fd, "Cache cannot report its size\n");
      } else {
        uint64_t size_unpinned = quota_mgr->GetSize();
        uint64_t size_pinned = quota_mgr->GetSizePinned();
        const string size_str = "Current cache size is " +
          StringifyInt(size_unpinned / (1024*1024)) + "MB (" +
          StringifyInt(size_unpinned) + " Bytes), pinned: " +
          StringifyInt(size_pinned / (1024*1024)) + "MB (" +
          StringifyInt(size_pinned) + " Bytes)\n";
        talk_mgr->Answer(con_fd, size_str);
      }
    } else if (line == "cache instance") {
      talk_mgr->Answer(con_fd, file_system->cache_mgr()->Describe());
    } else if (line == "cache list") {
      QuotaManager *quota_mgr = file_system->cache_mgr()->quota_mgr();
      if (!quota_mgr->HasCapability(QuotaManager::kCapList)) {
        talk_mgr->Answer(con_fd, "Cache cannot list its entries\n");
      } else {
        vector<string> ls = quota_mgr->List();
        talk_mgr->AnswerStringList(con_fd, ls);
      }
    } else if (line == "cache list pinned") {
      QuotaManager *quota_mgr = file_system->cache_mgr()->quota_mgr();
      if (!quota_mgr->HasCapability(QuotaManager::kCapList)) {
        talk_mgr->Answer(con_fd, "Cache cannot list its entries\n");
      } else {
        vector<string> ls_pinned = quota_mgr->ListPinned();
        talk_mgr->AnswerStringList(con_fd, ls_pinned);
      }
    } else if (line == "cache list catalogs") {
      QuotaManager *quota_mgr = file_system->cache_mgr()->quota_mgr();
      if (!quota_mgr->HasCapability(QuotaManager::kCapList)) {
        talk_mgr->Answer(con_fd, "Cache cannot list its entries\n");
      } else {
        vector<string> ls_catalogs = quota_mgr->ListCatalogs();
        talk_mgr->AnswerStringList(con_fd, ls_catalogs);
      }
    } else if (line.substr(0, 12) == "cleanup rate") {
      QuotaManager *quota_mgr = file_system->cache_mgr()->quota_mgr();
      if (!quota_mgr->HasCapability(QuotaManager::kCapIntrospectCleanupRate)) {
        talk_mgr->Answer(con_fd, "Unsupported by this cache\n");
      } else {
        if (line.length() < 14) {
          talk_mgr->Answer(con_fd, "Usage: cleanup rate <period in mn>\n");
        } else {
          const uint64_t period_s = String2Uint64(line.substr(13)) * 60;
          const uint64_t rate = quota_mgr->GetCleanupRate(period_s);
          talk_mgr->Answer(con_fd, StringifyInt(rate) + "\n");
        }
      }
    } else if (line.substr(0, 7) == "cleanup") {
      QuotaManager *quota_mgr = file_system->cache_mgr()->quota_mgr();
      if (!quota_mgr->HasCapability(QuotaManager::kCapShrink)) {
        talk_mgr->Answer(con_fd, "Cache cannot trigger eviction\n");
      } else {
        if (line.length() < 9) {
          talk_mgr->Answer(con_fd, "Usage: cleanup <MB>\n");
        } else {
          const uint64_t size = String2Uint64(line.substr(8))*1024*1024;
          if (quota_mgr->Cleanup(size)) {
            talk_mgr->Answer(con_fd, "OK\n");
          } else {
            talk_mgr->Answer(con_fd, "Not fully cleaned "
                                     "(there might be pinned chunks)\n");
          }
        }
      }
    } else if (line.substr(0, 5) == "evict") {
      assert(mount_point->file_system()->type() == FileSystem::kFsFuse);
      if (line.length() < 7) {
        talk_mgr->Answer(con_fd, "Usage: evict <path>\n");
      } else {
        const string path = line.substr(6);
        const bool found_regular = cvmfs::Evict(path);
        if (found_regular)
          talk_mgr->Answer(con_fd, "OK\n");
        else
          talk_mgr->Answer(con_fd, "No such regular file\n");
      }
    } else if (line.substr(0, 3) == "pin") {
      assert(mount_point->file_system()->type() == FileSystem::kFsFuse);
      if (line.length() < 5) {
        talk_mgr->Answer(con_fd, "Usage: pin <path>\n");
      } else {
        const string path = line.substr(4);
        const bool found_regular = cvmfs::Pin(path);
        if (found_regular)
          talk_mgr->Answer(con_fd, "OK\n");
        else
          talk_mgr->Answer(con_fd, "No such regular file or pinning failed\n");
      }
    } else if (line == "mountpoint") {
      talk_mgr->Answer(con_fd, cvmfs::loader_exports_->mount_point + "\n");
    } else if (line == "device id") {
      if (cvmfs::loader_exports_->version >= 5)
        talk_mgr->Answer(con_fd, cvmfs::loader_exports_->device_id + "\n");
      else
        talk_mgr->Answer(con_fd, "0:0\n");
    } else if (line.substr(0, 7) == "remount") {
      FuseRemounter::Status status;
      if (line == "remount sync")
        status = remounter->CheckSynchronously();
      else
        status = remounter->Check();
      switch (status) {
        case FuseRemounter::kStatusFailGeneral:
          talk_mgr->Answer(con_fd, "Failed\n");
          break;
        case FuseRemounter::kStatusFailNoSpace:
          talk_mgr->Answer(con_fd, "Failed (no space)\n");
          break;
        case FuseRemounter::kStatusUp2Date:
          talk_mgr->Answer(con_fd, "Catalog up to date\n");
          break;
        case FuseRemounter::kStatusDraining:
          talk_mgr->Answer(con_fd, "New revision applied\n");
          break;
        case FuseRemounter::kStatusMaintenance:
          talk_mgr->Answer(con_fd, "In maintenance mode\n");
          break;
        default:
          talk_mgr->Answer(con_fd, "internal error\n");
      }
    } else if (line.substr(0, 6) == "chroot") {
      if (line.length() < 8) {
        talk_mgr->Answer(con_fd, "Usage: chroot <hash>\n");
      } else {
        std::string root_hash = Trim(line.substr(7), true /* trim_newline */);
        FuseRemounter::Status status = remounter->ChangeRoot(
          MkFromHexPtr(shash::HexPtr(root_hash), shash::kSuffixCatalog));
        switch (status) {
          case FuseRemounter::kStatusUp2Date:
            talk_mgr->Answer(con_fd, "OK\n");
            break;
          default:
            talk_mgr->Answer(con_fd, "Failed\n");
            break;
        }
      }
    } else if (line == "detach nested catalogs") {
      mount_point->catalog_mgr()->DetachNested();
      talk_mgr->Answer(con_fd, "OK\n");
    } else if (line == "revision") {
      string revision = StringifyInt(mount_point->catalog_mgr()->GetRevision());
      talk_mgr->Answer(con_fd, revision + "\n");
    } else if (line == "max ttl info") {
      const unsigned max_ttl = mount_point->GetMaxTtlMn();
      if (max_ttl == 0) {
        talk_mgr->Answer(con_fd, "unset\n");
      } else {
        const string max_ttl_str = StringifyInt(max_ttl) + " minutes\n";
        talk_mgr->Answer(con_fd, max_ttl_str);
      }
    } else if (line.substr(0, 11) == "max ttl set") {
      if (line.length() < 13) {
        talk_mgr->Answer(con_fd, "Usage: max ttl set <minutes>\n");
      } else {
        const unsigned max_ttl = String2Uint64(line.substr(12));
        mount_point->SetMaxTtlMn(max_ttl);
        talk_mgr->Answer(con_fd, "OK\n");
      }
    } else if (line.substr(0, 14) == "nameserver get") {
      const string dns_server = mount_point->download_mgr()->GetDnsServer();
      const string reply = !dns_server.empty() ?
        std::string("DNS server address: ") + dns_server + "\n":
        std::string("DNS server not set.\n");
      talk_mgr->Answer(con_fd, reply);
    } else if (line.substr(0, 14) == "nameserver set") {
      if (line.length() < 16) {
        talk_mgr->Answer(con_fd, "Usage: nameserver set <host>\n");
      } else {
        const string host = line.substr(15);
        mount_point->download_mgr()->SetDnsServer(host);
        talk_mgr->Answer(con_fd, "OK\n");
      }
    } else if (line == "external host info") {
      string external_host_info =
        talk_mgr->FormatHostInfo(mount_point->external_download_mgr());
      talk_mgr->Answer(con_fd, external_host_info);
    } else if (line == "host info") {
      string host_info = talk_mgr->FormatHostInfo(mount_point->download_mgr());
      talk_mgr->Answer(con_fd, host_info);
    } else if (line == "host probe") {
      mount_point->download_mgr()->ProbeHosts();
      talk_mgr->Answer(con_fd, "OK\n");
    } else if (line == "host probe geo") {
      bool retval = mount_point->download_mgr()->ProbeGeo();
      if (retval)
        talk_mgr->Answer(con_fd, "OK\n");
      else
        talk_mgr->Answer(con_fd, "Failed\n");
    } else if (line == "external host switch") {
      mount_point->external_download_mgr()->SwitchHost();
      talk_mgr->Answer(con_fd, "OK\n");
    } else if (line == "host switch") {
      mount_point->download_mgr()->SwitchHost();
      talk_mgr->Answer(con_fd, "OK\n");
    } else if (line.substr(0, 17) == "external host set") {
      if (line.length() < 19) {
        talk_mgr->Answer(con_fd, "Usage: external host set <URL>\n");
      } else {
        const std::string host = line.substr(18);
        mount_point->external_download_mgr()->SetHostChain(host);
        talk_mgr->Answer(con_fd, "OK\n");
      }
    } else if (line.substr(0, 8) == "host set") {
      if (line.length() < 10) {
        talk_mgr->Answer(con_fd, "Usage: host set <host list>\n");
      } else {
        const string hosts = line.substr(9);
        mount_point->download_mgr()->SetHostChain(hosts);
        talk_mgr->Answer(con_fd, "OK\n");
      }
    } else if (line == "external proxy info") {
      string external_proxy_info =
        talk_mgr->FormatProxyInfo(mount_point->external_download_mgr());
      talk_mgr->Answer(con_fd, external_proxy_info);
    } else if (line == "proxy info") {
      string proxy_info =
        talk_mgr->FormatProxyInfo(mount_point->download_mgr());
      talk_mgr->Answer(con_fd, proxy_info);
    } else if (line == "proxy rebalance") {
      mount_point->download_mgr()->RebalanceProxies();
      talk_mgr->Answer(con_fd, "OK\n");
    } else if (line == "proxy group switch") {
      mount_point->download_mgr()->SwitchProxyGroup();
      talk_mgr->Answer(con_fd, "OK\n");
    } else if (line.substr(0, 18) == "external proxy set") {
      if (line.length() < 20) {
        talk_mgr->Answer(con_fd, "Usage: external proxy set <proxy list>\n");
      } else {
        string external_proxies = line.substr(19);
        mount_point->external_download_mgr()->SetProxyChain(
          external_proxies, "", download::DownloadManager::kSetProxyRegular);
        talk_mgr->Answer(con_fd, "OK\n");
      }
    } else if (line.substr(0, 9) == "proxy set") {
      if (line.length() < 11) {
        talk_mgr->Answer(con_fd, "Usage: proxy set <proxy list>\n");
      } else {
        string proxies = line.substr(10);
        proxies =
          download::ResolveProxyDescription(proxies, "",
                                            mount_point->download_mgr());
        if (proxies == "") {
            talk_mgr->Answer(con_fd, "Failed, no valid proxies\n");
        } else {
          mount_point->download_mgr()->SetProxyChain(
            proxies, "", download::DownloadManager::kSetProxyRegular);
          talk_mgr->Answer(con_fd, "OK\n");
        }
      }
    } else if (line.substr(0, 14) == "proxy fallback") {
      if (line.length() < 15) {
        talk_mgr->Answer(con_fd, "Usage: proxy fallback <proxy list>\n");
      } else {
        string fallback_proxies = line.substr(15);
        mount_point->download_mgr()->SetProxyChain(
          "", fallback_proxies, download::DownloadManager::kSetProxyFallback);
        talk_mgr->Answer(con_fd, "OK\n");
      }
    } else if (line == "timeout info") {
      unsigned timeout;
      unsigned timeout_direct;
      mount_point->download_mgr()->GetTimeout(&timeout, &timeout_direct);
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
      talk_mgr->Answer(con_fd, timeout_str);
    } else if (line.substr(0, 11) == "timeout set") {
      if (line.length() < 13) {
        talk_mgr->Answer(con_fd, "Usage: timeout set <proxy> <direct>\n");
      } else {
        uint64_t timeout;
        uint64_t timeout_direct;
        String2Uint64Pair(line.substr(12), &timeout, &timeout_direct);
        mount_point->download_mgr()->SetTimeout(timeout, timeout_direct);
        talk_mgr->Answer(con_fd, "OK\n");
      }
    } else if (line == "open catalogs") {
      talk_mgr->Answer(con_fd, mount_point->catalog_mgr()->PrintHierarchy());
    } else if (line == "internal affairs") {
      int current;
      int highwater;
      string result;

      result += "Inode Generation:\n  " + cvmfs::PrintInodeGeneration();

      // Manually setting the values of the ShortString counters
      mount_point->statistics()->Lookup("pathstring.n_instances")->
          Set(PathString::num_instances());
      mount_point->statistics()->Lookup("pathstring.n_overflows")->
          Set(PathString::num_overflows());
      mount_point->statistics()->Lookup("namestring.n_instances")->
          Set(NameString::num_instances());
      mount_point->statistics()->Lookup("namestring.n_overflows")->
          Set(NameString::num_overflows());
      mount_point->statistics()->Lookup("linkstring.n_instances")->
          Set(LinkString::num_instances());
      mount_point->statistics()->Lookup("linkstring.n_overflows")->
          Set(LinkString::num_overflows());

      // Manually setting the inode tracker numbers
      glue::InodeTracker::Statistics inode_stats =
        mount_point->inode_tracker()->GetStatistics();
      glue::NentryTracker::Statistics nentry_stats =
        mount_point->nentry_tracker()->GetStatistics();
      mount_point->statistics()->Lookup("inode_tracker.n_insert")->Set(
        atomic_read64(&inode_stats.num_inserts));
      mount_point->statistics()->Lookup("inode_tracker.n_remove")->Set(
        atomic_read64(&inode_stats.num_removes));
      mount_point->statistics()->Lookup("inode_tracker.no_reference")->Set(
        atomic_read64(&inode_stats.num_references));
      mount_point->statistics()->Lookup("inode_tracker.n_hit_inode")->Set(
        atomic_read64(&inode_stats.num_hits_inode));
      mount_point->statistics()->Lookup("inode_tracker.n_hit_path")->Set(
        atomic_read64(&inode_stats.num_hits_path));
      mount_point->statistics()->Lookup("inode_tracker.n_miss_path")->Set(
        atomic_read64(&inode_stats.num_misses_path));
      mount_point->statistics()->Lookup("nentry_tracker.n_insert")->Set(
        nentry_stats.num_insert);
      mount_point->statistics()->Lookup("nentry_tracker.n_remove")->Set(
        nentry_stats.num_remove);
      mount_point->statistics()->Lookup("nentry_tracker.n_prune")->Set(
        nentry_stats.num_prune);

      if (file_system->cache_mgr()->id() == kPosixCacheManager) {
        PosixCacheManager *cache_mgr =
          reinterpret_cast<PosixCacheManager *>(
            file_system->cache_mgr());
        result += "\nCache Mode: ";
        switch (cache_mgr->cache_mode()) {
          case PosixCacheManager::kCacheReadWrite:
            result += "read-write";
            break;
          case PosixCacheManager::kCacheReadOnly:
            result += "read-only";
            break;
          default:
            result += "unknown";
        }
      }
      bool drainout_mode;
      bool maintenance_mode;
      cvmfs::GetReloadStatus(&drainout_mode, &maintenance_mode);
      result += "\nDrainout Mode: " + StringifyBool(drainout_mode) + "\n";
      result += "Maintenance Mode: " + StringifyBool(maintenance_mode) + "\n";

      if (file_system->IsNfsSource()) {
        result += "\nNFS Map Statistics:\n";
        result += file_system->nfs_maps()->GetStatistics();
      }

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

      result += "\nPer-Connection Memory Statistics:\n" +
                mount_point->catalog_mgr()->PrintAllMemStatistics();

      result += "\nLatency distribution of system calls:\n";

      result += "Lookup\n" + file_system->hist_fs_lookup()->ToString();
      result += "Forget\n" + file_system->hist_fs_forget()->ToString();
      result += "Multi-Forget\n"
                + file_system->hist_fs_forget_multi()->ToString();
      result += "Getattr\n" + file_system->hist_fs_getattr()->ToString();
      result += "Readlink\n" + file_system->hist_fs_readlink()->ToString();
      result += "Opendir\n" + file_system->hist_fs_opendir()->ToString();
      result += "Releasedir\n" + file_system->hist_fs_releasedir()->ToString();
      result += "Readdir\n" + file_system->hist_fs_readdir()->ToString();
      result += "Open\n" + file_system->hist_fs_open()->ToString();
      result += "Read\n" + file_system->hist_fs_read()->ToString();
      result += "Release\n" + file_system->hist_fs_release()->ToString();

      result += "\nRaw Counters:\n" +
        mount_point->statistics()->PrintList(perf::Statistics::kPrintHeader);

      talk_mgr->Answer(con_fd, result);
    } else if (line == "reset error counters") {
      file_system->ResetErrorCounters();
      talk_mgr->Answer(con_fd, "OK\n");
    } else if (line == "pid") {
      const string pid_str = StringifyInt(cvmfs::pid_) + "\n";
      talk_mgr->Answer(con_fd, pid_str);
    } else if (line == "pid cachemgr") {
      const string pid_str =
        StringifyInt(file_system->cache_mgr()->quota_mgr()->GetPid()) + "\n";
      talk_mgr->Answer(con_fd, pid_str);
    } else if (line == "pid watchdog") {
      const string pid_str = StringifyInt(Watchdog::GetPid()) + "\n";
      talk_mgr->Answer(con_fd, pid_str);
    } else if (line == "parameters") {
      talk_mgr->Answer(con_fd, file_system->options_mgr()->Dump());
    } else if (line == "hotpatch history") {
      string history_str =
        StringifyTime(cvmfs::loader_exports_->boot_time, true) +
        "    (start of CernVM-FS loader " +
        cvmfs::loader_exports_->loader_version + ")\n";
      for (loader::EventList::const_iterator i =
           cvmfs::loader_exports_->history.begin(),
           iEnd = cvmfs::loader_exports_->history.end(); i != iEnd; ++i)
      {
        history_str += StringifyTime((*i)->timestamp, true) +
          "    (loaded CernVM-FS Fuse Module " +
          (*i)->so_version + ")\n";
      }
      talk_mgr->Answer(con_fd, history_str);
    } else if (line == "vfs inodes") {
      string result;
      glue::InodeTracker::Cursor cursor(
        mount_point->inode_tracker()->BeginEnumerate());
      uint64_t inode;
      while (mount_point->inode_tracker()->NextInode(&cursor, &inode)) {
        result += StringifyInt(inode) + "\n";
      }
      mount_point->inode_tracker()->EndEnumerate(&cursor);
      talk_mgr->Answer(con_fd, result);
    } else if (line == "vfs entries") {
      string result;
      glue::InodeTracker::Cursor cursor(
        mount_point->inode_tracker()->BeginEnumerate());
      uint64_t inode_parent;
      NameString name;
      while (mount_point->inode_tracker()->NextEntry(
        &cursor, &inode_parent, &name))
      {
        result += "<" + StringifyInt(inode_parent) + ">/" + name.ToString() +
                  "\n";
      }
      mount_point->inode_tracker()->EndEnumerate(&cursor);
      talk_mgr->Answer(con_fd, result);
    } else if (line == "version") {
      string version_str = string(VERSION) + " (CernVM-FS Fuse Module)\n" +
        cvmfs::loader_exports_->loader_version + " (Loader)\n";
      talk_mgr->Answer(con_fd, version_str);
    } else if (line == "version patchlevel") {
      talk_mgr->Answer(con_fd, string(CVMFS_PATCH_LEVEL) + "\n");
    } else if (line == "tear down to read-only") {
      if (file_system->cache_mgr()->id() != kPosixCacheManager) {
        talk_mgr->Answer(con_fd, "not supported\n");
      } else {
        // hack
        cvmfs::UnregisterQuotaListener();
        file_system->TearDown2ReadOnly();
        talk_mgr->Answer(con_fd, "In read-only mode\n");
      }
    } else if (line == "latency") {
      string result = talk_mgr->FormatLatencies(*mount_point, file_system);
      talk_mgr->Answer(con_fd, result);
    } else {
      talk_mgr->Answer(con_fd, "unknown command\n");
    }
  }

  return NULL;
}  // NOLINT(readability/fn_size)

string TalkManager::FormatLatencies(const MountPoint &mount_point,
                                    FileSystem *file_system) {
  string result;
  const unsigned int bufSize = 300;
  char buffer[bufSize];

  vector<float> qs;
  qs.push_back(.1);
  qs.push_back(.2);
  qs.push_back(.25);
  qs.push_back(.3);
  qs.push_back(.4);
  qs.push_back(.5);
  qs.push_back(.6);
  qs.push_back(.7);
  qs.push_back(.75);
  qs.push_back(.8);
  qs.push_back(.9);
  qs.push_back(.95);
  qs.push_back(.99);
  qs.push_back(.999);
  qs.push_back(.9999);

  string repo(mount_point.fqrn());

  unsigned int format_index =
      snprintf(buffer, bufSize, "\"%s\",\"%s\",\"%s\",\"%s\"", "repository",
               "action", "total_count", "time_unit");
  for (unsigned int i = 0; i < qs.size(); i++) {
    format_index += snprintf(buffer + format_index, bufSize - format_index,
                             ",%0.5f", qs[i]);
  }
  format_index += snprintf(buffer + format_index, bufSize - format_index, "\n");
  assert(format_index < bufSize);

  result += buffer;
  memset(buffer, 0, sizeof(buffer));
  format_index = 0;

  vector<Log2Histogram *> hist;
  vector<string> names;
  hist.push_back(file_system->hist_fs_lookup());
  names.push_back("lookup");
  hist.push_back(file_system->hist_fs_forget());
  names.push_back("forget");
  hist.push_back(file_system->hist_fs_forget_multi());
  names.push_back("forget_multi");
  hist.push_back(file_system->hist_fs_getattr());
  names.push_back("getattr");
  hist.push_back(file_system->hist_fs_readlink());
  names.push_back("readlink");
  hist.push_back(file_system->hist_fs_opendir());
  names.push_back("opendir");
  hist.push_back(file_system->hist_fs_releasedir());
  names.push_back("releasedir");
  hist.push_back(file_system->hist_fs_readdir());
  names.push_back("readdir");
  hist.push_back(file_system->hist_fs_open());
  names.push_back("open");
  hist.push_back(file_system->hist_fs_read());
  names.push_back("read");
  hist.push_back(file_system->hist_fs_release());
  names.push_back("release");

  for (unsigned int j = 0; j < hist.size(); j++) {
    Log2Histogram *h = hist[j];
    unsigned int format_index =
      snprintf(buffer, bufSize, "\"%s\",\"%s\",%" PRIu64 ",\"nanoseconds\"",
               repo.c_str(), names[j].c_str(), h->N());
    for (unsigned int i = 0; i < qs.size(); i++) {
      format_index += snprintf(buffer + format_index, bufSize - format_index,
                               ",%u", h->GetQuantile(qs[i]));
    }
    format_index +=
        snprintf(buffer + format_index, bufSize - format_index, "\n");
    assert(format_index < bufSize);

    result += buffer;
    memset(buffer, 0, sizeof(buffer));
    format_index = 0;
  }
  return result;
}

TalkManager::TalkManager(
  const string &socket_path,
  MountPoint *mount_point,
  FuseRemounter *remounter)
  : socket_path_(socket_path)
  , socket_fd_(-1)
  , mount_point_(mount_point)
  , remounter_(remounter)
  , spawned_(false)
{
  memset(&thread_talk_, 0, sizeof(thread_talk_));
}


TalkManager::~TalkManager() {
  if (!socket_path_.empty()) {
    int retval = unlink(socket_path_.c_str());
    if ((retval != 0) && (errno != ENOENT)) {
      LogCvmfs(kLogTalk, kLogSyslogWarn,
               "Could not remove cvmfs_io socket from cache directory (%d)",
               errno);
    }
  }

  if (socket_fd_ >= 0) {
    shutdown(socket_fd_, SHUT_RDWR);
    close(socket_fd_);
  }

  if (spawned_) {
    pthread_join(thread_talk_, NULL);
    LogCvmfs(kLogTalk, kLogDebug, "talk thread stopped");
  }
}


void TalkManager::Spawn() {
  int retval = pthread_create(&thread_talk_, NULL, MainResponder, this);
  assert(retval == 0);
  spawned_ = true;
}
