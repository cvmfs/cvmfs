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
#include "duplex_sqlite3.h"
#include "fuse_remount.h"
#include "glue_buffer.h"
#include "loader.h"
#include "lru_md.h"
#include "monitor.h"
#include "mountpoint.h"
#include "network/download.h"
#include "nfs_maps.h"
#include "options.h"
#include "quota.h"
#include "shortstring.h"
#include "statistics.h"
#include "tracer.h"
#include "util/logging.h"
#include "util/platform.h"
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


TalkManager *TalkManager::Create(const string &socket_path,
                                 MountPoint *mount_point,
                                 FuseRemounter *remounter) {
  UniquePtr<TalkManager> talk_manager(
      new TalkManager(socket_path, mount_point, remounter));

  talk_manager->socket_fd_ = MakeSocket(socket_path, 0660);
  if (talk_manager->socket_fd_ == -1)
    return NULL;
  if (listen(talk_manager->socket_fd_, 1) == -1)
    return NULL;

  LogCvmfs(kLogTalk, kLogDebug, "socket created at %s (fd %d)",
           socket_path.c_str(), talk_manager->socket_fd_);

  return talk_manager.Release();
}


string TalkManager::FormatMetalinkInfo(
    download::DownloadManager *download_mgr) {
  vector<string> metalink_chain;
  unsigned active_metalink;

  download_mgr->GetMetalinkInfo(&metalink_chain, &active_metalink);
  if (metalink_chain.size() == 0)
    return "No metalinks defined\n";

  string metalink_str;
  for (unsigned i = 0; i < metalink_chain.size(); ++i) {
    metalink_str += "  [" + StringifyInt(i) + "] " + metalink_chain[i] + "\n";
  }
  metalink_str += "Active metalink " + StringifyInt(active_metalink) + ": "
                  + metalink_chain[active_metalink] + "\n";
  return metalink_str;
}

string TalkManager::FormatHostInfo(download::DownloadManager *download_mgr) {
  vector<string> host_chain;
  vector<int> rtt;
  unsigned active_host;

  download_mgr->GetHostInfo(&host_chain, &rtt, &active_host);
  if (host_chain.size() == 0)
    return "No hosts defined\n";

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
  host_str += "Active host " + StringifyInt(active_host) + ": "
              + host_chain[active_host] + "\n";
  return host_str;
}

string TalkManager::FormatProxyInfo(download::DownloadManager *download_mgr) {
  vector<vector<download::DownloadManager::ProxyInfo> > proxy_chain;
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
      proxy_str += "[" + StringifyInt(i) + "] " + JoinStrings(urls, ", ")
                   + "\n";
    }
    proxy_str += "Active proxy: [" + StringifyInt(active_group) + "] "
                 + proxy_chain[active_group][0].url + "\n";
    if (fallback_group < proxy_chain.size())
      proxy_str += "First fallback group: [" + StringifyInt(fallback_group)
                   + "]\n";
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
    if ((con_fd = accept(talk_mgr->socket_fd_, (struct sockaddr *)&remote,
                         &socket_size))
        < 0) {
      LogCvmfs(kLogTalk, kLogDebug, "terminating talk thread (fd %d, errno %d)",
               con_fd, errno);
      break;
    }

    char buf[kMaxCommandSize];
    int bytes_read;
    if ((bytes_read = recv(con_fd, buf, sizeof(buf), 0)) <= 0)
      continue;

    if (buf[bytes_read - 1] == '\0')
      bytes_read--;
    const string line = string(buf, bytes_read);
    LogCvmfs(kLogTalk, kLogDebug, "received %s (length %lu)", line.c_str(),
             line.length());

    if (line == "tracebuffer flush") {
      mount_point->tracer()->Flush();
      talk_mgr->Answer(con_fd, "OK\n");
    } else if (line == "cache size") {
      QuotaManager *quota_mgr = file_system->cache_mgr()->quota_mgr();
      if (!quota_mgr->HasCapability(QuotaManager::kCapIntrospectSize)) {
        talk_mgr->Answer(con_fd, "Cache cannot report its size\n");
      } else {
        const uint64_t size_unpinned = quota_mgr->GetSize();
        const uint64_t size_pinned = quota_mgr->GetSizePinned();
        const string size_str = "Current cache size is "
                                + StringifyInt(size_unpinned / (1024 * 1024))
                                + "MB (" + StringifyInt(size_unpinned)
                                + " Bytes), pinned: "
                                + StringifyInt(size_pinned / (1024 * 1024))
                                + "MB (" + StringifyInt(size_pinned)
                                + " Bytes)\n";
        talk_mgr->Answer(con_fd, size_str);
      }
    } else if (line == "cache instance") {
      talk_mgr->Answer(con_fd, file_system->cache_mgr()->Describe());
    } else if (line == "cache list") {
      QuotaManager *quota_mgr = file_system->cache_mgr()->quota_mgr();
      if (!quota_mgr->HasCapability(QuotaManager::kCapList)) {
        talk_mgr->Answer(con_fd, "Cache cannot list its entries\n");
      } else {
        const vector<string> ls = quota_mgr->List();
        talk_mgr->AnswerStringList(con_fd, ls);
      }
    } else if (line == "cache list pinned") {
      QuotaManager *quota_mgr = file_system->cache_mgr()->quota_mgr();
      if (!quota_mgr->HasCapability(QuotaManager::kCapList)) {
        talk_mgr->Answer(con_fd, "Cache cannot list its entries\n");
      } else {
        const vector<string> ls_pinned = quota_mgr->ListPinned();
        talk_mgr->AnswerStringList(con_fd, ls_pinned);
      }
    } else if (line == "cache list catalogs") {
      QuotaManager *quota_mgr = file_system->cache_mgr()->quota_mgr();
      if (!quota_mgr->HasCapability(QuotaManager::kCapList)) {
        talk_mgr->Answer(con_fd, "Cache cannot list its entries\n");
      } else {
        const vector<string> ls_catalogs = quota_mgr->ListCatalogs();
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
    } else if (line.substr(0, 15) == "cache limit set") {
      if (line.length() < 16) {
        talk_mgr->Answer(con_fd, "Usage: cache limit set <MB>\n");
      } else {
        QuotaManager *quota_mgr = file_system->cache_mgr()->quota_mgr();
        const uint64_t size = String2Uint64(line.substr(16));
        if (size < 1000) {
          talk_mgr->Answer(con_fd, "New limit too low (minimum 1000)\n");
        } else {
          if (quota_mgr->SetLimit(size * 1024 * 1024)) {
            file_system->options_mgr()->SetValueFromTalk("CVMFS_QUOTA_LIMIT",
                                                         StringifyUint(size));
            talk_mgr->Answer(con_fd, "OK\n");
          } else {
            talk_mgr->Answer(con_fd, "Limit not reset\n");
          }
        }
      }
    } else if (line == "cache limit get") {
      std::string limit_from_options;
      file_system->options_mgr()->GetValue("CVMFS_QUOTA_LIMIT",
                                           &limit_from_options);
      talk_mgr->Answer(con_fd, limit_from_options + "\n");
    } else if (line.substr(0, 7) == "cleanup") {
      QuotaManager *quota_mgr = file_system->cache_mgr()->quota_mgr();
      if (!quota_mgr->HasCapability(QuotaManager::kCapShrink)) {
        talk_mgr->Answer(con_fd, "Cache cannot trigger eviction\n");
      } else {
        if (line.length() < 9) {
          talk_mgr->Answer(con_fd, "Usage: cleanup <MB>\n");
        } else {
          const uint64_t size = String2Uint64(line.substr(8)) * 1024 * 1024;
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
    } else if (line.substr(0, 13) == "send mount fd") {
      // Hidden command intended to be used only by the cvmfs mount helper
      if (line.length() < 15) {
        talk_mgr->Answer(con_fd, "EINVAL\n");
      } else {
        const std::string socket_path = line.substr(14);
        const bool retval = cvmfs::SendFuseFd(socket_path);
        talk_mgr->Answer(con_fd, retval ? "OK\n" : "Failed\n");
        LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog,
                 "Attempt to send fuse connection info to new mount (via %s)%s",
                 socket_path.c_str(), retval ? "" : " -- failed!");
      }
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
        const std::string root_hash = Trim(line.substr(7),
                                           true /* trim_newline */);
        const FuseRemounter::Status status = remounter->ChangeRoot(
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
      const string revision = StringifyInt(
          mount_point->catalog_mgr()->GetRevision());
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
      const string reply = !dns_server.empty()
                               ? std::string("DNS server address: ")
                                     + dns_server + "\n"
                               : std::string("DNS server not set.\n");
      talk_mgr->Answer(con_fd, reply);
    } else if (line.substr(0, 14) == "nameserver set") {
      if (line.length() < 16) {
        talk_mgr->Answer(con_fd, "Usage: nameserver set <host>\n");
      } else {
        const string host = line.substr(15);
        mount_point->download_mgr()->SetDnsServer(host);
        talk_mgr->Answer(con_fd, "OK\n");
      }
    } else if (line.substr(0, 22) == "__testing_freeze_cvmfs") {
      const std::string fs_dir = line.substr(23) + "/dir";
      mkdir(fs_dir.c_str(), 0700);
    } else if (line == "external metalink info") {
      const string external_metalink_info = talk_mgr->FormatMetalinkInfo(
          mount_point->external_download_mgr());
      talk_mgr->Answer(con_fd, external_metalink_info);
    } else if (line == "metalink info") {
      const string metalink_info = talk_mgr->FormatMetalinkInfo(
          mount_point->download_mgr());
      talk_mgr->Answer(con_fd, metalink_info);
    } else if (line == "external host info") {
      const string external_host_info = talk_mgr->FormatHostInfo(
          mount_point->external_download_mgr());
      talk_mgr->Answer(con_fd, external_host_info);
    } else if (line == "host info") {
      const string host_info = talk_mgr->FormatHostInfo(
          mount_point->download_mgr());
      talk_mgr->Answer(con_fd, host_info);
    } else if (line == "host probe") {
      mount_point->download_mgr()->ProbeHosts();
      talk_mgr->Answer(con_fd, "OK\n");
    } else if (line == "host probe geo") {
      const bool retval = mount_point->download_mgr()->ProbeGeo();
      if (retval)
        talk_mgr->Answer(con_fd, "OK\n");
      else
        talk_mgr->Answer(con_fd, "Failed\n");
    } else if (line == "external metalink switch") {
      mount_point->external_download_mgr()->SwitchMetalink();
      talk_mgr->Answer(con_fd, "OK\n");
    } else if (line == "metalink switch") {
      mount_point->download_mgr()->SwitchMetalink();
      talk_mgr->Answer(con_fd, "OK\n");
    } else if (line == "external host switch") {
      mount_point->external_download_mgr()->SwitchHost();
      talk_mgr->Answer(con_fd, "OK\n");
    } else if (line == "host switch") {
      mount_point->download_mgr()->SwitchHost();
      talk_mgr->Answer(con_fd, "OK\n");
    } else if (line.substr(0, 21) == "external metalink set") {
      if (line.length() < 23) {
        talk_mgr->Answer(con_fd, "Usage: external metalink set <URL>\n");
      } else {
        const std::string host = line.substr(22);
        mount_point->external_download_mgr()->SetMetalinkChain(host);
        talk_mgr->Answer(con_fd, "OK\n");
      }
    } else if (line.substr(0, 12) == "metalink set") {
      if (line.length() < 14) {
        talk_mgr->Answer(con_fd, "Usage: metalink set <URL>\n");
      } else {
        const std::string host = line.substr(13);
        mount_point->download_mgr()->SetMetalinkChain(host);
        talk_mgr->Answer(con_fd, "OK\n");
      }
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
      const string external_proxy_info = talk_mgr->FormatProxyInfo(
          mount_point->external_download_mgr());
      talk_mgr->Answer(con_fd, external_proxy_info);
    } else if (line == "proxy info") {
      const string proxy_info = talk_mgr->FormatProxyInfo(
          mount_point->download_mgr());
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
        const string external_proxies = line.substr(19);
        mount_point->external_download_mgr()->SetProxyChain(
            external_proxies, "", download::DownloadManager::kSetProxyRegular);
        talk_mgr->Answer(con_fd, "OK\n");
      }
    } else if (line.substr(0, 9) == "proxy set") {
      if (line.length() < 11) {
        talk_mgr->Answer(con_fd, "Usage: proxy set <proxy list>\n");
      } else {
        string proxies = line.substr(10);
        proxies = download::ResolveProxyDescription(
            proxies, "", mount_point->download_mgr());
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
        const string fallback_proxies = line.substr(15);
        mount_point->download_mgr()->SetProxyChain(
            "", fallback_proxies, download::DownloadManager::kSetProxyFallback);
        talk_mgr->Answer(con_fd, "OK\n");
      }
    } else if (line == "timeout info") {
      unsigned timeout;
      unsigned timeout_direct;
      mount_point->download_mgr()->GetTimeout(&timeout, &timeout_direct);
      string timeout_str = "Timeout with proxy: ";
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
    } else if (line == "drop metadata caches") {
      // For testing
      mount_point->inode_cache()->Pause();
      mount_point->path_cache()->Pause();
      mount_point->md5path_cache()->Pause();
      mount_point->inode_cache()->Drop();
      mount_point->path_cache()->Drop();
      mount_point->md5path_cache()->Drop();
      mount_point->inode_cache()->Resume();
      mount_point->path_cache()->Resume();
      mount_point->md5path_cache()->Resume();
      talk_mgr->Answer(con_fd, "OK\n");
    } else if (line == "internal affairs") {
      int current;
      int highwater;
      string result;

      result += "Inode Generation:\n  " + cvmfs::PrintInodeGeneration();

      // Manually setting the values of the ShortString counters
      mount_point->statistics()
          ->Lookup("pathstring.n_instances")
          ->Set(PathString::num_instances());
      mount_point->statistics()
          ->Lookup("pathstring.n_overflows")
          ->Set(PathString::num_overflows());
      mount_point->statistics()
          ->Lookup("namestring.n_instances")
          ->Set(NameString::num_instances());
      mount_point->statistics()
          ->Lookup("namestring.n_overflows")
          ->Set(NameString::num_overflows());
      mount_point->statistics()
          ->Lookup("linkstring.n_instances")
          ->Set(LinkString::num_instances());
      mount_point->statistics()
          ->Lookup("linkstring.n_overflows")
          ->Set(LinkString::num_overflows());

      // Manually setting the inode tracker numbers
      glue::InodeTracker::Statistics inode_stats = mount_point->inode_tracker()
                                                       ->GetStatistics();
      const glue::DentryTracker::Statistics
          dentry_stats = mount_point->dentry_tracker()->GetStatistics();
      const glue::PageCacheTracker::Statistics
          page_cache_stats = mount_point->page_cache_tracker()->GetStatistics();
      mount_point->statistics()
          ->Lookup("inode_tracker.n_insert")
          ->Set(atomic_read64(&inode_stats.num_inserts));
      mount_point->statistics()
          ->Lookup("inode_tracker.n_remove")
          ->Set(atomic_read64(&inode_stats.num_removes));
      mount_point->statistics()
          ->Lookup("inode_tracker.no_reference")
          ->Set(atomic_read64(&inode_stats.num_references));
      mount_point->statistics()
          ->Lookup("inode_tracker.n_hit_inode")
          ->Set(atomic_read64(&inode_stats.num_hits_inode));
      mount_point->statistics()
          ->Lookup("inode_tracker.n_hit_path")
          ->Set(atomic_read64(&inode_stats.num_hits_path));
      mount_point->statistics()
          ->Lookup("inode_tracker.n_miss_path")
          ->Set(atomic_read64(&inode_stats.num_misses_path));
      mount_point->statistics()
          ->Lookup("dentry_tracker.n_insert")
          ->Set(dentry_stats.num_insert);
      mount_point->statistics()
          ->Lookup("dentry_tracker.n_remove")
          ->Set(dentry_stats.num_remove);
      mount_point->statistics()
          ->Lookup("dentry_tracker.n_prune")
          ->Set(dentry_stats.num_prune);
      mount_point->statistics()
          ->Lookup("page_cache_tracker.n_insert")
          ->Set(page_cache_stats.n_insert);
      mount_point->statistics()
          ->Lookup("page_cache_tracker.n_remove")
          ->Set(page_cache_stats.n_remove);
      mount_point->statistics()
          ->Lookup("page_cache_tracker.n_open_direct")
          ->Set(page_cache_stats.n_open_direct);
      mount_point->statistics()
          ->Lookup("page_cache_tracker.n_open_flush")
          ->Set(page_cache_stats.n_open_flush);
      mount_point->statistics()
          ->Lookup("page_cache_tracker.n_open_cached")
          ->Set(page_cache_stats.n_open_cached);

      if (file_system->cache_mgr()->id() == kPosixCacheManager) {
        PosixCacheManager *cache_mgr = reinterpret_cast<PosixCacheManager *>(
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
      result += "  General purpose allocator " + StringifyInt(current / 1024)
                + " KB / " + StringifyInt(highwater / 1024) + " KB\n";

      sqlite3_status(SQLITE_STATUS_MALLOC_SIZE, &current, &highwater, 0);
      result += "  Largest malloc " + StringifyInt(highwater) + " Bytes\n";

      sqlite3_status(SQLITE_STATUS_PAGECACHE_USED, &current, &highwater, 0);
      result += "  Page cache allocations " + StringifyInt(current) + " / "
                + StringifyInt(highwater) + "\n";

      sqlite3_status(SQLITE_STATUS_PAGECACHE_OVERFLOW, &current, &highwater, 0);
      result += "  Page cache overflows " + StringifyInt(current / 1024)
                + " KB / " + StringifyInt(highwater / 1024) + " KB\n";

      sqlite3_status(SQLITE_STATUS_PAGECACHE_SIZE, &current, &highwater, 0);
      result += "  Largest page cache allocation " + StringifyInt(highwater)
                + " Bytes\n";

      sqlite3_status(SQLITE_STATUS_SCRATCH_USED, &current, &highwater, 0);
      result += "  Scratch allocations " + StringifyInt(current) + " / "
                + StringifyInt(highwater) + "\n";

      sqlite3_status(SQLITE_STATUS_SCRATCH_OVERFLOW, &current, &highwater, 0);
      result += "  Scratch overflows " + StringifyInt(current) + " / "
                + StringifyInt(highwater) + "\n";

      sqlite3_status(SQLITE_STATUS_SCRATCH_SIZE, &current, &highwater, 0);
      result += "  Largest scratch allocation " + StringifyInt(highwater / 1024)
                + " KB\n";

      result += "\nPer-Connection Memory Statistics:\n"
                + mount_point->catalog_mgr()->PrintAllMemStatistics();

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

      result += "\nRaw Counters:\n"
                + mount_point->statistics()->PrintList(
                    perf::Statistics::kPrintHeader);

      talk_mgr->Answer(con_fd, result);
    } else if (line == "reset error counters") {
      file_system->ResetErrorCounters();
      talk_mgr->Answer(con_fd, "OK\n");
    } else if (line == "pid") {
      const string pid_str = StringifyInt(cvmfs::pid_) + "\n";
      talk_mgr->Answer(con_fd, pid_str);
    } else if (line == "pid cachemgr") {
      const string pid_str = StringifyInt(file_system->cache_mgr()
                                              ->quota_mgr()
                                              ->GetPid())
                             + "\n";
      talk_mgr->Answer(con_fd, pid_str);
    } else if (line == "pid watchdog") {
      const string pid_str = StringifyInt(Watchdog::GetPid()) + "\n";
      talk_mgr->Answer(con_fd, pid_str);
    } else if (line == "parameters") {
      talk_mgr->Answer(con_fd, file_system->options_mgr()->Dump());
    } else if (line == "hotpatch history") {
      string history_str = StringifyTime(cvmfs::loader_exports_->boot_time,
                                         true)
                           + "    (start of CernVM-FS loader "
                           + cvmfs::loader_exports_->loader_version + ")\n";
      for (loader::EventList::const_iterator
               i = cvmfs::loader_exports_->history.begin(),
               iEnd = cvmfs::loader_exports_->history.end();
           i != iEnd;
           ++i) {
        history_str += StringifyTime((*i)->timestamp, true)
                       + "    (loaded CernVM-FS Fuse Module " + (*i)->so_version
                       + ")\n";
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
      while (mount_point->inode_tracker()->NextEntry(&cursor, &inode_parent,
                                                     &name)) {
        result += "<" + StringifyInt(inode_parent) + ">/" + name.ToString()
                  + "\n";
      }
      mount_point->inode_tracker()->EndEnumerate(&cursor);
      talk_mgr->Answer(con_fd, result);
    } else if (line == "version") {
      const string version_str = string(CVMFS_VERSION)
                                 + " (CernVM-FS Fuse Module)\n"
                                 + cvmfs::loader_exports_->loader_version
                                 + " (Loader)\n";
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
      const string result = talk_mgr->FormatLatencies(*mount_point,
                                                      file_system);
      talk_mgr->Answer(con_fd, result);
    } else if (line == "metrics prometheus") {
      const string result = talk_mgr->FormatPrometheusMetrics(*mount_point,
                                                              file_system);
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

  const string repo(mount_point.fqrn());

  unsigned int format_index = snprintf(
      buffer, bufSize, "\"%s\",\"%s\",\"%s\",\"%s\"", "repository", "action",
      "total_count", "time_unit");
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
    unsigned int format_index = snprintf(
        buffer, bufSize, "\"%s\",\"%s\",%" PRIu64 ",\"nanoseconds\"",
        repo.c_str(), names[j].c_str(), h->N());
    for (unsigned int i = 0; i < qs.size(); i++) {
      format_index += snprintf(buffer + format_index, bufSize - format_index,
                               ",%u", h->GetQuantile(qs[i]));
    }
    format_index += snprintf(buffer + format_index, bufSize - format_index,
                             "\n");
    assert(format_index < bufSize);

    result += buffer;
    memset(buffer, 0, sizeof(buffer));
    format_index = 0;
  }
  return result;
}

string TalkManager::FormatPrometheusMetrics(MountPoint &mount_point,
                                            FileSystem *file_system) {
  string result;
  const string fqrn = mount_point.fqrn();
  const string mountpoint = cvmfs::loader_exports_->mount_point;

  // Helper function to format a prometheus metric
  class MetricFormatter {
   public:
    explicit MetricFormatter(string &result_ref) : result_(result_ref) {}
    void operator()(const string &name, const string &type,
                    const string &help, const string &labels,
                    const string &value) {
      result_ += "# HELP " + name + " " + help + "\n";
      result_ += "# TYPE " + name + " " + type + "\n";
      result_ += name + "{" + labels + "} " + value + "\n";
    }
   private:
    string &result_;
  };
  MetricFormatter format_metric(result);

  // Get cache information
  QuotaManager *quota_mgr = file_system->cache_mgr()->quota_mgr();
  if (quota_mgr->HasCapability(QuotaManager::kCapIntrospectSize)) {
    const uint64_t size_unpinned = quota_mgr->GetSize();
    const uint64_t size_pinned = quota_mgr->GetSizePinned();

    format_metric("cvmfs_cached_bytes", "gauge",
                  "CVMFS currently cached bytes.",
                  "repo=\"" + fqrn + "\"", StringifyUint(size_unpinned));
    format_metric("cvmfs_pinned_bytes", "gauge",
                  "CVMFS currently pinned bytes.",
                  "repo=\"" + fqrn + "\"", StringifyUint(size_pinned));
  }

  // Get cache limit from parameters
  string cache_limit_str;
  if (file_system->options_mgr()->GetValue("CVMFS_QUOTA_LIMIT", &cache_limit_str)) {
    const uint64_t cache_limit_mb = String2Uint64(cache_limit_str);
    const uint64_t cache_limit_bytes = cache_limit_mb * 1024 * 1024;
    format_metric("cvmfs_total_cache_size_bytes", "gauge",
                  "CVMFS configured cache size via CVMFS_QUOTA_LIMIT.",
                  "repo=\"" + fqrn + "\"", StringifyUint(cache_limit_bytes));
  }

  // Get cache base directory for df information
  string cache_base;
  if (file_system->options_mgr()->GetValue("CVMFS_CACHE_BASE", &cache_base)) {
    struct statvfs stat_info;
    if (statvfs(cache_base.c_str(), &stat_info) == 0) {
      const uint64_t total_size = static_cast<uint64_t>(stat_info.f_blocks) * stat_info.f_frsize;
      const uint64_t avail_size = static_cast<uint64_t>(stat_info.f_bavail) * stat_info.f_frsize;

      format_metric("cvmfs_physical_cache_size_bytes", "gauge",
                    "CVMFS cache volume physical size.",
                    "repo=\"" + fqrn + "\"", StringifyUint(total_size));
      format_metric("cvmfs_physical_cache_avail_bytes", "gauge",
                    "CVMFS cache volume physical free space available.",
                    "repo=\"" + fqrn + "\"", StringifyUint(avail_size));
    }
  }

  // Version and revision information
  const string version = string(CVMFS_VERSION) + "." + string(CVMFS_PATCH_LEVEL);
  const uint64_t revision = mount_point.catalog_mgr()->GetRevision();
  format_metric("cvmfs_repo", "gauge",
                "Shows the version of CVMFS used by this repository.",
                "repo=\"" + fqrn + "\",mountpoint=\"" + mountpoint +
                "\",version=\"" + version + "\",revision=\"" + StringifyUint(revision) + "\"",
                "1");

  // Statistics-based metrics
  perf::Statistics *statistics = mount_point.statistics();

  // Download statistics
  const int64_t rx_bytes = statistics->Lookup("download.sz_transferred_bytes")->Get();
  format_metric("cvmfs_rx_total", "counter",
                "Shows the overall amount of downloaded bytes since mounting.",
                "repo=\"" + fqrn + "\"", StringifyInt(rx_bytes));

  const int64_t n_downloads = statistics->Lookup("fetch.n_downloads")->Get();
  format_metric("cvmfs_ndownload_total", "counter",
                "Shows the overall number of downloaded files since mounting.",
                "repo=\"" + fqrn + "\"", StringifyInt(n_downloads));

  // Hit rate calculation
  const int64_t n_invocations = statistics->Lookup("fetch.n_invocations")->Get();
  if (n_invocations > 0) {
    const float hitrate = 100.0 * (1.0 - (static_cast<float>(n_downloads) /
                                          static_cast<float>(n_invocations)));
    format_metric("cvmfs_hitrate", "gauge",
                  "CVMFS cache hit rate (%)",
                  "repo=\"" + fqrn + "\"", StringifyDouble(hitrate));
  } else {
    format_metric("cvmfs_hitrate", "gauge",
                  "CVMFS cache hit rate (%)",
                  "repo=\"" + fqrn + "\"", "0");
  }

  // Speed calculation
  const int64_t transfer_time = statistics->Lookup("download.sz_transfer_time")->Get();
  if (transfer_time > 0) {
    const int64_t speed = (1000 * (rx_bytes / 1024)) / transfer_time;
    format_metric("cvmfs_speed", "gauge",
                  "Shows the average download speed.",
                  "repo=\"" + fqrn + "\"", StringifyInt(speed));
  } else {
    format_metric("cvmfs_speed", "gauge",
                  "Shows the average download speed.",
                  "repo=\"" + fqrn + "\"", "0");
  }

  // Uptime calculation
  const time_t now = time(NULL);
  const uint64_t uptime_seconds = now - cvmfs::loader_exports_->boot_time;
  const uint64_t mount_epoch_time = now - uptime_seconds;
  format_metric("cvmfs_uptime_seconds", "counter",
                "Shows the time since the repo was mounted.",
                "repo=\"" + fqrn + "\"", StringifyUint(uptime_seconds));
  format_metric("cvmfs_mount_epoch_timestamp", "counter",
                "Shows the epoch time the repo was mounted.",
                "repo=\"" + fqrn + "\"", StringifyUint(mount_epoch_time));

  // Catalog expiry - access through the TalkManager's remounter member
  const time_t catalogs_valid_until = remounter_->catalogs_valid_until();
  if (catalogs_valid_until != MountPoint::kIndefiniteDeadline) {
    const int64_t expires_seconds = (catalogs_valid_until - now);
    format_metric("cvmfs_repo_expires_seconds", "gauge",
                  "Shows the remaining life time of the mounted root file catalog in seconds.",
                  "repo=\"" + fqrn + "\"", StringifyInt(expires_seconds));
  }

  // I/O error count
  const uint64_t nioerr = file_system->io_error_info()->count();
  format_metric("cvmfs_nioerr_total", "counter",
                "Shows the total number of I/O errors encountered since mounting.",
                "repo=\"" + fqrn + "\"", StringifyUint(nioerr));

  // Timeout information
  unsigned timeout_proxy, timeout_direct;
  mount_point.download_mgr()->GetTimeout(&timeout_proxy, &timeout_direct);
  format_metric("cvmfs_timeout", "gauge",
                "Shows the timeout for proxied connections in seconds.",
                "repo=\"" + fqrn + "\"", StringifyUint(timeout_proxy));
  format_metric("cvmfs_timeout_direct", "gauge",
                "Shows the timeout for direct connections in seconds.",
                "repo=\"" + fqrn + "\"", StringifyUint(timeout_direct));

  // Last I/O error timestamp
  const int64_t timestamp_last_ioerr = file_system->io_error_info()->timestamp_last();
  format_metric("cvmfs_timestamp_last_ioerr", "counter",
                "Shows the timestamp of the last ioerror.",
                "repo=\"" + fqrn + "\"", StringifyInt(timestamp_last_ioerr));

  // CPU usage from /proc/pid/stat
  const pid_t pid = cvmfs::pid_;
  const string proc_stat_path = "/proc/" + StringifyInt(pid) + "/stat";
  FILE *stat_file = fopen(proc_stat_path.c_str(), "r");
  if (stat_file) {
    char stat_line[1024];
    if (fgets(stat_line, sizeof(stat_line), stat_file)) {
      vector<string> stat_fields = SplitString(string(stat_line), ' ');
      if (stat_fields.size() > 15) {
        const uint64_t utime = String2Uint64(stat_fields[13]);
        const uint64_t stime = String2Uint64(stat_fields[14]);
        const long clock_tick = sysconf(_SC_CLK_TCK);
        if (clock_tick > 0) {
          const double user_seconds = static_cast<double>(utime) / clock_tick;
          const double system_seconds = static_cast<double>(stime) / clock_tick;
          format_metric("cvmfs_cpu_user_total", "counter",
                        "CPU time used in userspace by CVMFS mount in seconds.",
                        "repo=\"" + fqrn + "\"", StringifyDouble(user_seconds));
          format_metric("cvmfs_cpu_system_total", "counter",
                        "CPU time used in the kernel system calls by CVMFS mount in seconds.",
                        "repo=\"" + fqrn + "\"", StringifyDouble(system_seconds));
        }
      }
    }
    fclose(stat_file);
  }

  // File descriptor and directory counts
  format_metric("cvmfs_usedfd", "gauge",
                "Shows the number of open directories currently used by file system clients.",
                "repo=\"" + fqrn + "\"",
                file_system->no_open_files()->ToString());
  format_metric("cvmfs_useddirp", "gauge",
                "Shows the number of file descriptors currently issued to file system clients.",
                "repo=\"" + fqrn + "\"",
                file_system->no_open_dirs()->ToString());
  format_metric("cvmfs_ndiropen", "gauge",
                "Shows the overall number of opened directories.",
                "repo=\"" + fqrn + "\"",
                file_system->n_fs_dir_open()->ToString());

  // Inode max
  format_metric("cvmfs_inode_max", "gauge",
                "Shows the highest possible inode with the current set of loaded catalogs.",
                "repo=\"" + fqrn + "\"",
                StringifyInt(mount_point.inode_annotation()->GetGeneration() +
                           mount_point.catalog_mgr()->inode_gauge()));

  // Process ID
  format_metric("cvmfs_pid", "gauge",
                "Shows the process id of the CernVM-FS Fuse process.",
                "repo=\"" + fqrn + "\"", StringifyInt(pid));

  // Catalog count
  const int n_catalogs = mount_point.catalog_mgr()->GetNumCatalogs();
  format_metric("cvmfs_nclg", "gauge",
                "Shows the number of currently loaded nested catalogs.",
                "repo=\"" + fqrn + "\"", StringifyInt(n_catalogs));

  // Cleanup rate (24 hours)
  if (quota_mgr->HasCapability(QuotaManager::kCapIntrospectCleanupRate)) {
    const uint64_t period_s = 24 * 60 * 60;
    const uint64_t cleanup_rate = quota_mgr->GetCleanupRate(period_s);
    format_metric("cvmfs_ncleanup24", "gauge",
                  "Shows the number of cache cleanups in the last 24 hours.",
                  "repo=\"" + fqrn + "\"", StringifyUint(cleanup_rate));
  } else {
    format_metric("cvmfs_ncleanup24", "gauge",
                  "Shows the number of cache cleanups in the last 24 hours.",
                  "repo=\"" + fqrn + "\"", "-1");
  }

  // Active proxy
  vector<vector<download::DownloadManager::ProxyInfo> > proxy_chain;
  unsigned current_group;
  mount_point.download_mgr()->GetProxyInfo(&proxy_chain, &current_group, NULL);
  string active_proxy = "DIRECT";
  if (proxy_chain.size() > 0 && current_group < proxy_chain.size() &&
      proxy_chain[current_group].size() > 0) {
    active_proxy = proxy_chain[current_group][0].url;
  }
  format_metric("cvmfs_active_proxy", "gauge",
                "Shows the active proxy in use for this mount.",
                "repo=\"" + fqrn + "\",proxy=\"" + active_proxy + "\"", "1");

  // Proxy list metrics
  for (unsigned int i = 0; i < proxy_chain.size(); i++) {
    for (unsigned int j = 0; j < proxy_chain[i].size(); j++) {
      format_metric("cvmfs_proxy", "gauge",
                    "Shows all registered proxies for this repository.",
                    "repo=\"" + fqrn + "\",group=\"" + StringifyInt(i) +
                    "\",url=\"" + proxy_chain[i][j].url + "\"", "1");
    }
  }

  // Internal affairs metrics (excluding histograms)

  // Update string counters manually (same as internal affairs does)
  mount_point.statistics()->Lookup("pathstring.n_instances")->Set(PathString::num_instances());
  mount_point.statistics()->Lookup("pathstring.n_overflows")->Set(PathString::num_overflows());
  mount_point.statistics()->Lookup("namestring.n_instances")->Set(NameString::num_instances());
  mount_point.statistics()->Lookup("namestring.n_overflows")->Set(NameString::num_overflows());
  mount_point.statistics()->Lookup("linkstring.n_instances")->Set(LinkString::num_instances());
  mount_point.statistics()->Lookup("linkstring.n_overflows")->Set(LinkString::num_overflows());

  // String statistics
  const int64_t pathstring_instances = mount_point.statistics()->Lookup("pathstring.n_instances")->Get();
  const int64_t pathstring_overflows = mount_point.statistics()->Lookup("pathstring.n_overflows")->Get();
  const int64_t namestring_instances = mount_point.statistics()->Lookup("namestring.n_instances")->Get();
  const int64_t namestring_overflows = mount_point.statistics()->Lookup("namestring.n_overflows")->Get();
  const int64_t linkstring_instances = mount_point.statistics()->Lookup("linkstring.n_instances")->Get();
  const int64_t linkstring_overflows = mount_point.statistics()->Lookup("linkstring.n_overflows")->Get();

  format_metric("cvmfs_pathstring_instances", "gauge",
                "Number of PathString instances.",
                "repo=\"" + fqrn + "\"", StringifyInt(pathstring_instances));
  format_metric("cvmfs_pathstring_overflows", "counter",
                "Number of PathString overflows.",
                "repo=\"" + fqrn + "\"", StringifyInt(pathstring_overflows));
  format_metric("cvmfs_namestring_instances", "gauge",
                "Number of NameString instances.",
                "repo=\"" + fqrn + "\"", StringifyInt(namestring_instances));
  format_metric("cvmfs_namestring_overflows", "counter",
                "Number of NameString overflows.",
                "repo=\"" + fqrn + "\"", StringifyInt(namestring_overflows));
  format_metric("cvmfs_linkstring_instances", "gauge",
                "Number of LinkString instances.",
                "repo=\"" + fqrn + "\"", StringifyInt(linkstring_instances));
  format_metric("cvmfs_linkstring_overflows", "counter",
                "Number of LinkString overflows.",
                "repo=\"" + fqrn + "\"", StringifyInt(linkstring_overflows));

  // Tracker statistics (same as internal affairs does)
  glue::InodeTracker::Statistics inode_stats = mount_point.inode_tracker()->GetStatistics();
  const glue::DentryTracker::Statistics dentry_stats = mount_point.dentry_tracker()->GetStatistics();
  const glue::PageCacheTracker::Statistics page_cache_stats = mount_point.page_cache_tracker()->GetStatistics();

  // Update statistics manually
  mount_point.statistics()->Lookup("inode_tracker.n_insert")->Set(atomic_read64(&inode_stats.num_inserts));
  mount_point.statistics()->Lookup("inode_tracker.n_remove")->Set(atomic_read64(&inode_stats.num_removes));
  mount_point.statistics()->Lookup("inode_tracker.no_reference")->Set(atomic_read64(&inode_stats.num_references));
  mount_point.statistics()->Lookup("inode_tracker.n_hit_inode")->Set(atomic_read64(&inode_stats.num_hits_inode));
  mount_point.statistics()->Lookup("inode_tracker.n_hit_path")->Set(atomic_read64(&inode_stats.num_hits_path));
  mount_point.statistics()->Lookup("inode_tracker.n_miss_path")->Set(atomic_read64(&inode_stats.num_misses_path));
  mount_point.statistics()->Lookup("dentry_tracker.n_insert")->Set(dentry_stats.num_insert);
  mount_point.statistics()->Lookup("dentry_tracker.n_remove")->Set(dentry_stats.num_remove);
  mount_point.statistics()->Lookup("dentry_tracker.n_prune")->Set(dentry_stats.num_prune);
  mount_point.statistics()->Lookup("page_cache_tracker.n_insert")->Set(page_cache_stats.n_insert);
  mount_point.statistics()->Lookup("page_cache_tracker.n_remove")->Set(page_cache_stats.n_remove);
  mount_point.statistics()->Lookup("page_cache_tracker.n_open_direct")->Set(page_cache_stats.n_open_direct);
  mount_point.statistics()->Lookup("page_cache_tracker.n_open_flush")->Set(page_cache_stats.n_open_flush);
  mount_point.statistics()->Lookup("page_cache_tracker.n_open_cached")->Set(page_cache_stats.n_open_cached);

  // Inode tracker metrics
  format_metric("cvmfs_inode_tracker_inserts_total", "counter",
                "Number of inode tracker insertions.",
                "repo=\"" + fqrn + "\"", StringifyInt(atomic_read64(&inode_stats.num_inserts)));
  format_metric("cvmfs_inode_tracker_removes_total", "counter",
                "Number of inode tracker removals.",
                "repo=\"" + fqrn + "\"", StringifyInt(atomic_read64(&inode_stats.num_removes)));
  format_metric("cvmfs_inode_tracker_references", "gauge",
                "Number of inode tracker references.",
                "repo=\"" + fqrn + "\"", StringifyInt(atomic_read64(&inode_stats.num_references)));
  format_metric("cvmfs_inode_tracker_hits_inode_total", "counter",
                "Number of inode tracker inode hits.",
                "repo=\"" + fqrn + "\"", StringifyInt(atomic_read64(&inode_stats.num_hits_inode)));
  format_metric("cvmfs_inode_tracker_hits_path_total", "counter",
                "Number of inode tracker path hits.",
                "repo=\"" + fqrn + "\"", StringifyInt(atomic_read64(&inode_stats.num_hits_path)));
  format_metric("cvmfs_inode_tracker_misses_path_total", "counter",
                "Number of inode tracker path misses.",
                "repo=\"" + fqrn + "\"", StringifyInt(atomic_read64(&inode_stats.num_misses_path)));

  // Dentry tracker metrics
  format_metric("cvmfs_dentry_tracker_inserts_total", "counter",
                "Number of dentry tracker insertions.",
                "repo=\"" + fqrn + "\"", StringifyInt(dentry_stats.num_insert));
  format_metric("cvmfs_dentry_tracker_removes_total", "counter",
                "Number of dentry tracker removals.",
                "repo=\"" + fqrn + "\"", StringifyInt(dentry_stats.num_remove));
  format_metric("cvmfs_dentry_tracker_prunes_total", "counter",
                "Number of dentry tracker prunes.",
                "repo=\"" + fqrn + "\"", StringifyInt(dentry_stats.num_prune));

  // Page cache tracker metrics
  format_metric("cvmfs_page_cache_tracker_inserts_total", "counter",
                "Number of page cache tracker insertions.",
                "repo=\"" + fqrn + "\"", StringifyInt(page_cache_stats.n_insert));
  format_metric("cvmfs_page_cache_tracker_removes_total", "counter",
                "Number of page cache tracker removals.",
                "repo=\"" + fqrn + "\"", StringifyInt(page_cache_stats.n_remove));
  format_metric("cvmfs_page_cache_tracker_opens_direct_total", "counter",
                "Number of page cache tracker direct opens.",
                "repo=\"" + fqrn + "\"", StringifyInt(page_cache_stats.n_open_direct));
  format_metric("cvmfs_page_cache_tracker_opens_flush_total", "counter",
                "Number of page cache tracker flush opens.",
                "repo=\"" + fqrn + "\"", StringifyInt(page_cache_stats.n_open_flush));
  format_metric("cvmfs_page_cache_tracker_opens_cached_total", "counter",
                "Number of page cache tracker cached opens.",
                "repo=\"" + fqrn + "\"", StringifyInt(page_cache_stats.n_open_cached));

  // Cache mode information
  if (file_system->cache_mgr()->id() == kPosixCacheManager) {
    PosixCacheManager *cache_mgr = reinterpret_cast<PosixCacheManager *>(file_system->cache_mgr());
    int cache_mode_value = 0;
    switch (cache_mgr->cache_mode()) {
      case PosixCacheManager::kCacheReadWrite:
        cache_mode_value = 1;
        break;
      case PosixCacheManager::kCacheReadOnly:
        cache_mode_value = 2;
        break;
      default:
        cache_mode_value = 0;
    }
    format_metric("cvmfs_cache_mode", "gauge",
                  "Cache mode (0=unknown, 1=read-write, 2=read-only).",
                  "repo=\"" + fqrn + "\"", StringifyInt(cache_mode_value));
  }

  // Drainout and maintenance mode
  bool drainout_mode;
  bool maintenance_mode;
  cvmfs::GetReloadStatus(&drainout_mode, &maintenance_mode);
  format_metric("cvmfs_drainout_mode", "gauge",
                "Drainout mode status (0=false, 1=true).",
                "repo=\"" + fqrn + "\"", StringifyInt(drainout_mode ? 1 : 0));
  format_metric("cvmfs_maintenance_mode", "gauge",
                "Maintenance mode status (0=false, 1=true).",
                "repo=\"" + fqrn + "\"", StringifyInt(maintenance_mode ? 1 : 0));

  // SQLite statistics
  int current, highwater;

  sqlite3_status(SQLITE_STATUS_MALLOC_COUNT, &current, &highwater, 0);
  format_metric("cvmfs_sqlite_malloc_count", "gauge",
                "Number of SQLite allocations.",
                "repo=\"" + fqrn + "\"", StringifyInt(current));

  sqlite3_status(SQLITE_STATUS_MEMORY_USED, &current, &highwater, 0);
  format_metric("cvmfs_sqlite_memory_used_bytes", "gauge",
                "SQLite general purpose allocator memory used.",
                "repo=\"" + fqrn + "\"", StringifyInt(current));
  format_metric("cvmfs_sqlite_memory_used_highwater_bytes", "gauge",
                "SQLite general purpose allocator memory used high water mark.",
                "repo=\"" + fqrn + "\"", StringifyInt(highwater));

  sqlite3_status(SQLITE_STATUS_MALLOC_SIZE, &current, &highwater, 0);
  format_metric("cvmfs_sqlite_largest_malloc_bytes", "gauge",
                "SQLite largest malloc size.",
                "repo=\"" + fqrn + "\"", StringifyInt(highwater));

  sqlite3_status(SQLITE_STATUS_PAGECACHE_USED, &current, &highwater, 0);
  format_metric("cvmfs_sqlite_pagecache_used", "gauge",
                "SQLite page cache allocations used.",
                "repo=\"" + fqrn + "\"", StringifyInt(current));
  format_metric("cvmfs_sqlite_pagecache_used_highwater", "gauge",
                "SQLite page cache allocations used high water mark.",
                "repo=\"" + fqrn + "\"", StringifyInt(highwater));

  sqlite3_status(SQLITE_STATUS_PAGECACHE_OVERFLOW, &current, &highwater, 0);
  format_metric("cvmfs_sqlite_pagecache_overflow_bytes", "gauge",
                "SQLite page cache overflow bytes.",
                "repo=\"" + fqrn + "\"", StringifyInt(current));
  format_metric("cvmfs_sqlite_pagecache_overflow_highwater_bytes", "gauge",
                "SQLite page cache overflow bytes high water mark.",
                "repo=\"" + fqrn + "\"", StringifyInt(highwater));

  sqlite3_status(SQLITE_STATUS_PAGECACHE_SIZE, &current, &highwater, 0);
  format_metric("cvmfs_sqlite_largest_pagecache_bytes", "gauge",
                "SQLite largest page cache allocation size.",
                "repo=\"" + fqrn + "\"", StringifyInt(highwater));

  sqlite3_status(SQLITE_STATUS_SCRATCH_USED, &current, &highwater, 0);
  format_metric("cvmfs_sqlite_scratch_used", "gauge",
                "SQLite scratch allocations used.",
                "repo=\"" + fqrn + "\"", StringifyInt(current));
  format_metric("cvmfs_sqlite_scratch_used_highwater", "gauge",
                "SQLite scratch allocations used high water mark.",
                "repo=\"" + fqrn + "\"", StringifyInt(highwater));

  sqlite3_status(SQLITE_STATUS_SCRATCH_OVERFLOW, &current, &highwater, 0);
  format_metric("cvmfs_sqlite_scratch_overflow", "gauge",
                "SQLite scratch overflows.",
                "repo=\"" + fqrn + "\"", StringifyInt(current));
  format_metric("cvmfs_sqlite_scratch_overflow_highwater", "gauge",
                "SQLite scratch overflows high water mark.",
                "repo=\"" + fqrn + "\"", StringifyInt(highwater));

  sqlite3_status(SQLITE_STATUS_SCRATCH_SIZE, &current, &highwater, 0);
  format_metric("cvmfs_sqlite_largest_scratch_bytes", "gauge",
                "SQLite largest scratch allocation size.",
                "repo=\"" + fqrn + "\"", StringifyInt(highwater));

  // NFS statistics (if applicable)
  if (file_system->IsNfsSource()) {
    format_metric("cvmfs_nfs_mode", "gauge",
                  "NFS mode enabled (1=true, 0=false).",
                  "repo=\"" + fqrn + "\"", "1");
    // Note: NFS map statistics are complex strings, skipping detailed parsing for now
  } else {
    format_metric("cvmfs_nfs_mode", "gauge",
                  "NFS mode enabled (1=true, 0=false).",
                  "repo=\"" + fqrn + "\"", "0");
  }

  return result;
}

TalkManager::TalkManager(const string &socket_path,
                         MountPoint *mount_point,
                         FuseRemounter *remounter)
    : socket_path_(socket_path)
    , socket_fd_(-1)
    , mount_point_(mount_point)
    , remounter_(remounter)
    , spawned_(false) {
  memset(&thread_talk_, 0, sizeof(thread_talk_));
}


TalkManager::~TalkManager() {
  if (!socket_path_.empty()) {
    const int retval = unlink(socket_path_.c_str());
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
  const int retval = pthread_create(&thread_talk_, NULL, MainResponder, this);
  assert(retval == 0);
  spawned_ = true;
}
