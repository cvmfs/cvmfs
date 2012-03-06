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
#include <sstream>
#include <vector>

#include "platform.h"
#include "tracer.h"
#include "lru.h"
#include "cvmfs.h"
#include "util.h"
#include "logging.h"
#include "download.h"
#include "sqlite3-duplex.h"

using namespace std;  // NOLINT

namespace talk {

string cachedir;  /**< Stores the cache directory from cvmfs.
                       Pipe files will be created here. */
string socket_path; /**< $cache_dir/cvmfs_io */
int socket_fd;
pthread_t thread_cvmfs_talk;
bool spawned = false;

static void answer(const int con_fd, const string &msg) {
  LogCvmfs(kLogTalk, kLogDebug, "message length: %d", msg.length());

  (void)send(con_fd, &msg[0], msg.length(), MSG_NOSIGNAL);
}

static void *tf_talk(void *data __attribute__((unused))) {
  LogCvmfs(kLogTalk, kLogDebug, "talk thread started");

  struct sockaddr_un remote;
  socklen_t s = sizeof(remote);
  int con_fd = -1;
  while (true) {
    if (con_fd > 0) {
      shutdown(con_fd, SHUT_RDWR);
      close(con_fd);
    }
    if ((con_fd = accept(socket_fd, (struct sockaddr *)&remote, &s)) < 0) {
      break;
    }

    char buf[256];
    if (recv(con_fd, buf, 256, 0) > 0) {
      const string line = string(buf);
      //pmesg(D_TALK, "received command %s", line.c_str());

      if (line == "flush") {
        tracer::Flush();
        answer(con_fd, "OK\n");
      }
      else if (line == "cache size") {
        if (lru::GetCapacity() == 0)
          answer(con_fd, "Cache is unmanaged\n");
        else {
          ostringstream size;
          uint64_t size_unpinned = lru::GetSize();
          uint64_t size_pinned = lru::GetSizePinned();
          size << "Current cache size is " << size_unpinned / (1024*1024) << "MB ("
          << size_unpinned << " Bytes), "
          << "pinned: " << size_pinned / (1024*1024) << "MB ("
          << size_pinned << " Bytes)" <<  endl;
          answer(con_fd, size.str());
        }
      }
      else if (line == "cache list") {
        if (lru::GetCapacity() == 0)
          answer(con_fd, "Cache is unmanaged\n");
        else {
          vector<string> ls = lru::List();
          string ls_result;
          for (unsigned i = 0; i < ls.size(); ++i) {
            ls_result += ls[i] + "\n";
          }
          answer(con_fd, ls_result);
        }
      }
      else if (line == "cache list pinned") {
        if (lru::GetCapacity() == 0)
          answer(con_fd, "Cache is unmanaged\n");
        else {
          vector<string> ls = lru::ListPinned();
          string ls_result;
          for (unsigned i = 0; i < ls.size(); ++i) {
            ls_result += ls[i] + "\n";
          }
          answer(con_fd, ls_result);
        }
      }
      else if (line == "cache list catalogs") {
        if (lru::GetCapacity() == 0)
          answer(con_fd, "Cache is unmanaged\n");
        else {
          vector<string> ls = lru::ListCatalogs();
          string ls_result;
          for (unsigned i = 0; i < ls.size(); ++i) {
            ls_result += ls[i] + "\n";
          }
          answer(con_fd, ls_result);
        }
      }
      else if (line.substr(0, 7) == "cleanup") {
        if (lru::GetCapacity() == 0) {
          answer(con_fd, "Cache is unmanaged\n");
        } else {
          if (line.length() < 9) {
            answer(con_fd, "Usage: cleanup <MB>\n");
          } else {
            istringstream ssize(line.substr(8));
            uint64_t size;
            ssize >> size;
            size *= 1024*1024;
            if (lru::Cleanup(size)) {
              answer(con_fd, "OK\n");
            } else {
              answer(con_fd, "Not fully cleaned (there might be pinned chunks)\n");
            }
          }
        }
      }
      else if (line.substr(0, 10) == "clear file") {
        if (lru::GetCapacity() == 0) {
          answer(con_fd, "Cache is unmanaged\n");
        } else {
          if (line.length() < 12) {
            answer(con_fd, "Usage: clear file <path>\n");
          } else {
            istringstream spath(line.substr(11));
            string path;
            spath >> path;
            int result = cvmfs::clear_file(path);
            switch (result) {
              case 0:
                answer(con_fd, "OK\n");
                break;
              case -ENOENT:
                answer(con_fd, "No such file\n");
                break;
              case -EINVAL:
                answer(con_fd, "Not a regular file\n");
                break;
              default:
                ostringstream err_unknown;
                err_unknown << "Unknown error (" << result << ")" << endl;
                answer(con_fd, err_unknown.str());
                break;
            }
          }
        }
      }
      else if (line == "mountpoint") {
        answer(con_fd, cvmfs::mountpoint + "\n");
      }
      else if (line == "remount") {
        // TODO: implement this!!
        int result = -1;
        //               int result = cvmfs::remount()
        if (result < 0) {
          answer(con_fd, "Failed\n");
        } else if (result == 0) {
          answer(con_fd, "Catalog up to date\n");
        } else if (result == 2) {
          answer(con_fd, "Already draining out caches\n");
        } else {
          ostringstream str_max_cache_timeout;
          str_max_cache_timeout << "Remounting, draining out kernel caches for "
          << cvmfs::max_cache_timeout
          << " seconds..." << endl;
          answer(con_fd, str_max_cache_timeout.str());
          sleep(cvmfs::max_cache_timeout);
        }
      }
      // TODO
      /*else if (line == "revision") {
       catalog::lock();
       const uint64_t revision = catalog::get_revision();
       catalog::unlock();
       ostringstream revision_str;
       revision_str << revision;
       answer(con_fd, revision_str.str() + "\n");
       }*/
      else if (line == "max ttl info") {
        const unsigned max_ttl = cvmfs::get_max_ttl();
        if (max_ttl == 0) {
          answer(con_fd, "unset\n");
        } else {
          ostringstream max_ttl_str;
          max_ttl_str << cvmfs::get_max_ttl() << " minutes";
          answer(con_fd, max_ttl_str.str() + "\n");
        }
      }
      else if (line.substr(0, 11) == "max ttl set") {
        if (line.length() < 13) {
          answer(con_fd, "Usage: max ttl set <minutes>\n");
        } else {
          istringstream smaxttl(line.substr(12));
          unsigned max_ttl;
          smaxttl >> max_ttl;
          cvmfs::set_max_ttl(max_ttl);
          answer(con_fd, "OK\n");
        }
      }
      else if (line == "host info") {
        vector<string> host_chain;
        vector<int> rtt;
        unsigned active_host;

        download::GetHostInfo(&host_chain, &rtt, &active_host);
        ostringstream info;
        for (unsigned i = 0; i < host_chain.size(); ++i) {
          info << "  [" << i << "]" << " " << host_chain[i] << " (";
          if (rtt[i] == -1) info << "unprobed";
          else if (rtt[i] == -2) info << "host down";
          else info << rtt[i] << " ms";
          info << ")" << endl;
        }
        info << "Active host " << active_host << ": "
        << host_chain[active_host] << endl;
        answer(con_fd, info.str());
      }
      else if (line == "host probe") {
        download::ProbeHosts();
        answer(con_fd, "OK\n");
      }
      else if (line == "host switch") {
        download::SwitchHost();
        answer(con_fd, "OK\n");
      }
      else if (line.substr(0, 8) == "host set") {
        if (line.length() < 10) {
          answer(con_fd, "Usage: host set <host list>\n");
        } else {
          istringstream shosts(line.substr(9));
          string hosts;
          shosts >> hosts;
          download::SetHostChain(hosts);
          answer(con_fd, "OK\n");
        }
      }
      else if (line == "proxy info") {
        vector< vector<string> > proxy_chain;
        unsigned active_group;
        download::GetProxyInfo(&proxy_chain, &active_group);

        ostringstream info;
        if (proxy_chain.size()) {
          info << "Load-balance groups:" << endl;
          for (unsigned i = 0; i < proxy_chain.size(); ++i) {
            info << "[" << i << "] " << JoinStrings(proxy_chain[i], ", ")
            << endl;
          }
          info << "Active proxy: [" << active_group << "] "
          << proxy_chain[active_group][0] << endl;
        } else {
          info << "No proxies defined\n";
        }

        answer(con_fd, info.str());
      }
      else if (line == "proxy rebalance") {
        download::RebalanceProxies();
        answer(con_fd, "OK\n");
      }
      else if (line == "proxy group switch") {
        download::SwitchProxyGroup();
        answer(con_fd, "OK\n");
      }
      else if (line.substr(0, 9) == "proxy set") {
        if (line.length() < 11) {
          answer(con_fd, "Usage: proxy set <proxy list>\n");
        } else {
          istringstream sproxies(line.substr(10));
          string proxies;
          sproxies >> proxies;
          download::SetProxyChain(proxies);
          answer(con_fd, "OK\n");
        }
      }
      else if (line == "timeout info") {
        unsigned timeout;
        unsigned timeout_direct;
        download::GetTimeout(&timeout, &timeout_direct);
        ostringstream info;
        info << "Timeout with proxy: ";
        if (timeout) info << timeout << "s\n";
        else info << "no timeout\n";
        info << "Timeout without proxy: ";
        if (timeout_direct) info << timeout_direct << "s\n";
        else info << "no timeout\n";
        answer(con_fd, info.str());
      }
      else if (line.substr(0, 11) == "timeout set") {
        if (line.length() < 13) {
          answer(con_fd, "Usage: timeout set <proxy> <direct>\n");
        } else {
          istringstream stimeouts(line.substr(12));
          unsigned timeout;
          stimeouts >> timeout;
          unsigned timeout_direct = timeout;
          stimeouts >> timeout_direct;
          download::SetTimeout(timeout, timeout_direct);
          answer(con_fd, "OK\n");
        }
      }
      /*  TODO  else if (line == "open catalogs") {
       vector<string> prefix;
       vector<time_t> last_modified, expires;
       vector<unsigned int> inode_offsets;
       cvmfs::info_loaded_catalogs(&prefix, &last_modified, &expires, &inode_offsets);
       string result = "Prefix | Last Modified | Expires | inode offset\n";
       for (unsigned i = 0; i < prefix.size(); ++i) {
       result += ((prefix[i] == "") ? "/" : prefix[i]) + " | ";
       result += ((last_modified[i] == 0) ? "n/a" : localtime_ascii(last_modified[i], true)) + " | ";
       result += (expires[i] == 0) ? "n/a" : localtime_ascii(expires[i], true) + " | ";
       result += " " + inode_offsets[i];
       result += "\n";
       }

       answer(con_fd, result);
       TODO
       } else if (line == "sqlite memory") {
       ostringstream result;
       int current = 0;
       int highwater = 0;
       int ncache = 0;
       int pcache = 0;
       int acache = 0;
       int cache_inserts = 0;
       int cache_replaces = 0;
       int cache_cleans = 0;
       int cache_hits = 0;
       int cache_misses = 0;
       int cert_hits = 0;
       int cert_misses = 0;

       catalog::lock();
       lru::lock();

       result << "File catalog memcache " << cvmfs::catalog_cache_memusage_bytes()/1024 << " KB" << endl;
       cvmfs::catalog_cache_memusage_slots(&pcache, &ncache, &acache,
       &cache_inserts, &cache_replaces, &cache_cleans, &cache_hits, &cache_misses,
       &cert_hits, &cert_misses);
       result << "File catalog memcache slots "
       << pcache << " positive, " << ncache << " negative / " << acache << " slots, "
       << cache_inserts << " inserts, " << cache_replaces << " replaces (not measured), " << cache_cleans << " cleans, "
       << cache_hits << " hits, " << cache_misses << " misses" << endl
       << "certificate disk cache hits/misses " << cert_hits << "/" << cert_misses << endl;


       sqlite3_status(SQLITE_STATUS_MALLOC_COUNT, &current, &highwater, 0);
       result << "Number of allocations " << current << endl;

       sqlite3_status(SQLITE_STATUS_MEMORY_USED, &current, &highwater, 0);
       result << "General purpose allocator " << current/1024 << " KB / "
       << highwater/1024 << " KB" << endl;

       sqlite3_status(SQLITE_STATUS_MALLOC_SIZE, &current, &highwater, 0);
       result << "Largest malloc " << highwater << " Bytes" << endl;

       sqlite3_status(SQLITE_STATUS_PAGECACHE_USED, &current, &highwater, 0);
       result << "Page cache allocations " << current << " / " << highwater << endl;

       sqlite3_status(SQLITE_STATUS_PAGECACHE_OVERFLOW, &current, &highwater, 0);
       result << "Page cache overflows " << current/1024 << " KB / " << highwater/1024 << " KB" << endl;

       sqlite3_status(SQLITE_STATUS_PAGECACHE_SIZE, &current, &highwater, 0);
       result << "Largest page cache allocation " << highwater << " Bytes" << endl;

       sqlite3_status(SQLITE_STATUS_SCRATCH_USED, &current, &highwater, 0);
       result << "Scratch allocations " << current << " / " << highwater << endl;

       sqlite3_status(SQLITE_STATUS_SCRATCH_OVERFLOW, &current, &highwater, 0);
       result << "Scratch overflows " << current << " / " << highwater << endl;

       sqlite3_status(SQLITE_STATUS_SCRATCH_SIZE, &current, &highwater, 0);
       result << "Largest scratch allocation " << highwater/1024 << " KB" << endl;

       result << catalog::get_db_memory_usage();
       result << lru::get_memory_usage();

       lru::unlock();
       catalog::unlock();

       answer(con_fd, result.str());
       }
       } else if (line == "catalog tree") {
       answer(con_fd, catalog_tree::show_tree());
       }*/
      else if (line == "pid") {
        ostringstream spid;
        spid << cvmfs::pid << endl;
        answer(con_fd, spid.str());
      }
      else if (line == "version") {
        answer(con_fd, string(VERSION) + "\n");
      }
      else if (line == "version patchlevel") {
        answer(con_fd, string(CVMFS_PATCH_LEVEL) + "\n");
      } else {
        answer(con_fd, "What?\n");
      }
    }
  }

  return NULL;
}


/**
 * Init the socket.
 */
bool Init(const string &cachedir) {
  talk::cachedir = cachedir;

  struct sockaddr_un sock_addr;
  socket_path = cachedir + "/cvmfs_io";
  if (socket_path.length() >= sizeof(sock_addr.sun_path))
    return false;

  if ((socket_fd = socket(AF_UNIX, SOCK_STREAM, 0)) == -1)
    return false;

#ifndef __APPLE__
  // fchmod on a socket is not allowed under Mac OS X
  // using default here... (0770 in this case)
  if (fchmod(socket_fd, 0660) != 0)
    return false;
#endif

  sock_addr.sun_family = AF_UNIX;
  strncpy(sock_addr.sun_path, socket_path.c_str(), sizeof(sock_addr.sun_path));

  if (bind(socket_fd, (struct sockaddr *)&sock_addr,
           sizeof(sock_addr.sun_family) + sizeof(sock_addr.sun_path)) < 0)
  {
    if ((errno == EADDRINUSE) && (unlink(socket_path.c_str()) == 0)) {
      /* second try, perhaps the file was left over */
      if (bind(socket_fd, (struct sockaddr *)&sock_addr,
               sizeof(sock_addr.sun_family) + sizeof(sock_addr.sun_path)) < 0)
      {
        return false;
      }
      LogCvmfs(kLogTalk, kLogSyslog,
               "There was already a cvmfs_io file in cache directory.  "
               "Did we have a crash shutdown?");
    } else {
      return false;
    }
  }

  if (listen(socket_fd, 1) < -1)
    return false;

  LogCvmfs(kLogTalk, kLogDebug, "socket created at %s", socket_path.c_str());

  return true;
}


/**
 * Spawns the socket-dealing thread
 */
void Spawn() {
  int result;
  result = pthread_create(&thread_cvmfs_talk, NULL, tf_talk, NULL);
  assert((result == 0) && "talk thread creation failed");
  spawned = true;
}


/**
 * Terminates command-listener thread.  Removes socket.
 */
void Fini() {
  int result;
  result = unlink(socket_path.c_str());
  if (result != 0) {
    LogCvmfs(kLogTalk, kLogSyslog,
             "Could not remove cvmfs_io socket from cache directory.");
  }

  shutdown(socket_fd, SHUT_RDWR);
  close(socket_fd);
  if (spawned) pthread_join(thread_cvmfs_talk, NULL);
  LogCvmfs(kLogTalk, kLogDebug, "talk thread stopped");
}

}
