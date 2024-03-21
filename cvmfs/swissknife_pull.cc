/**
 * This file is part of the CernVM File System.
 *
 * Replicates a cvmfs repository.  Uses the cvmfs intrinsic Merkle trees
 * to calculate the difference set.
 */

// NOLINTNEXTLINE
#define _FILE_OFFSET_BITS 64
// NOLINTNEXTLINE
#define __STDC_FORMAT_MACROS

#include "cvmfs_config.h"
#include "swissknife_pull.h"

#include <inttypes.h>
#include <pthread.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "catalog.h"
#include "compression.h"
#include "crypto/hash.h"
#include "crypto/signature.h"
#include "history_sqlite.h"
#include "manifest.h"
#include "manifest_fetch.h"
#include "network/download.h"
#include "object_fetcher.h"
#include "path_filters/relaxed_path_filter.h"
#include "reflog.h"
#include "upload.h"
#include "util/atomic.h"
#include "util/concurrency.h"
#include "util/exception.h"
#include "util/logging.h"
#include "util/posix.h"
#include "util/shared_ptr.h"
#include "util/smalloc.h"
#include "util/string.h"

using namespace std;  // NOLINT

namespace swissknife {

namespace {

typedef HttpObjectFetcher<> ObjectFetcher;

/**
 * This just stores an shash::Any in a predictable way to send it through a
 * POSIX pipe.
 */
class ChunkJob {
 public:
  ChunkJob()
    : suffix(shash::kSuffixNone)
    , hash_algorithm(shash::kAny)
    , compression_alg(zlib::kZlibDefault) {}

  ChunkJob(const shash::Any &hash, zlib::Algorithms compression_alg)
    : suffix(hash.suffix)
    , hash_algorithm(hash.algorithm)
    , compression_alg(compression_alg)
  {
    memcpy(digest, hash.digest, hash.GetDigestSize());
  }

  bool IsTerminateJob() const {
    return (hash_algorithm == shash::kAny);
  }

  shash::Any hash() const {
    assert(!IsTerminateJob());
    return shash::Any(hash_algorithm,
                      digest,
                      suffix);
  }

  const shash::Suffix      suffix;
  const shash::Algorithms  hash_algorithm;
  const zlib::Algorithms   compression_alg;
  unsigned char            digest[shash::kMaxDigestSize];
};

static void SpoolerOnUpload(const upload::SpoolerResult &result) {
  unlink(result.local_path.c_str());
  if (result.return_code != 0) {
    PANIC(kLogStderr, "spooler failure %d (%s, hash: %s)", result.return_code,
          result.local_path.c_str(), result.content_hash.ToString().c_str());
  }
}

SharedPtr<string>    stratum0_url;
SharedPtr<string>    stratum1_url;
SharedPtr<string>    temp_dir;
unsigned             num_parallel = 1;
bool                 pull_history = false;
bool                 apply_timestamp_threshold = false;
uint64_t             timestamp_threshold = 0;
bool                 is_garbage_collectable = false;
bool                 initial_snapshot = false;
upload::Spooler     *spooler = NULL;
int                  pipe_chunks[2];
// required for concurrent reading
pthread_mutex_t      lock_pipe = PTHREAD_MUTEX_INITIALIZER;
unsigned             retries = 3;
catalog::RelaxedPathFilter   *pathfilter = NULL;
atomic_int64         overall_chunks;
atomic_int64         overall_new;
atomic_int64         chunk_queue;
bool                 preload_cache = false;
string              *preload_cachedir = NULL;
bool                 inspect_existing_catalogs = false;
manifest::Reflog    *reflog = NULL;

}  // anonymous namespace


static std::string MakePath(const shash::Any &hash) {
  return (preload_cache)
    ? *preload_cachedir + "/" + hash.MakePathWithoutSuffix()
    : "data/"           + hash.MakePath();
}


static bool Peek(const string &remote_path) {
  return (preload_cache) ? FileExists(remote_path)
                         : spooler->Peek(remote_path);
}

static bool Peek(const shash::Any &remote_hash) {
  return Peek(MakePath(remote_hash));
}

static void ReportDownloadError(const download::JobInfo &download_job) {
  const download::Failures error_code = download_job.error_code();
  const int http_code = download_job.http_code();
  const std::string url = *download_job.url();

  LogCvmfs(kLogCvmfs, kLogStderr, "failed to download %s (%d - %s)",
           url.c_str(), error_code, download::Code2Ascii(error_code));

  switch (error_code) {
    case download::kFailProxyResolve:
    case download::kFailHostResolve:
      LogCvmfs(kLogCvmfs, kLogStderr, "DNS lookup for Stratum 0 failed - "
                                      "please check the network connection");
      break;

    case download::kFailHostHttp:
      LogCvmfs(kLogCvmfs, kLogStderr, "unexpected HTTP error code %d - "
               "please check the stratum 0 health", http_code);
      break;

    case download::kFailBadData:
      LogCvmfs(kLogCvmfs, kLogStderr, "downloaded corrupted data - "
                                      "please check the stratum 0 health");
      break;

    default:
      if (download::IsProxyTransferError(error_code) ||
          download::IsHostTransferError(error_code)) {
        LogCvmfs(kLogCvmfs, kLogStderr, "couldn't reach Stratum 0 - "
                                      "please check the network connection");
      } else {
        LogCvmfs(kLogCvmfs, kLogStderr, "unexpected error - feel free to file "
                                      "a bug report");
      }
  }
}


static void Store(
  const string &local_path,
  const string &remote_path,
  const bool compressed_src)
{
  if (preload_cache) {
    if (!compressed_src) {
      int retval = rename(local_path.c_str(), remote_path.c_str());
      if (retval != 0) {
        PANIC(kLogStderr, "Failed to move '%s' to '%s'", local_path.c_str(),
              remote_path.c_str());
      }
    } else {
      // compressed input
      string tmp_dest;
      FILE *fdest = CreateTempFile(remote_path, 0660, "w", &tmp_dest);
      if (fdest == NULL) {
        PANIC(kLogStderr, "Failed to create temporary file '%s'",
              remote_path.c_str());
      }
      int retval = zlib::DecompressPath2File(local_path, fdest);
      if (!retval) {
        PANIC(kLogStderr, "Failed to preload %s to %s", local_path.c_str(),
              remote_path.c_str());
      }
      fclose(fdest);
      retval = rename(tmp_dest.c_str(), remote_path.c_str());
      assert(retval == 0);
      unlink(local_path.c_str());
    }
  } else {
    spooler->Upload(local_path, remote_path);
  }
}

static void Store(
  const string &local_path,
  const shash::Any &remote_hash,
  const bool compressed_src = true)
{
  Store(local_path, MakePath(remote_hash), compressed_src);
}


static void StoreBuffer(const unsigned char *buffer, const unsigned size,
                        const std::string &dest_path, const bool compress) {
  string tmp_file;
  FILE *ftmp = CreateTempFile(*temp_dir + "/cvmfs", 0600, "w", &tmp_file);
  assert(ftmp);
  int retval;
  if (compress) {
    shash::Any dummy(shash::kSha1);  // hardcoded hash no problem, unused
    retval = zlib::CompressMem2File(buffer, size, ftmp, &dummy);
  } else {
    retval = CopyMem2File(buffer, size, ftmp);
  }
  assert(retval);
  fclose(ftmp);
  Store(tmp_file, dest_path, true);
}

static void StoreBuffer(const unsigned char *buffer, const unsigned size,
                        const shash::Any &dest_hash, const bool compress) {
  StoreBuffer(buffer, size, MakePath(dest_hash), compress);
}


static void WaitForStorage() {
  if (!preload_cache) spooler->WaitForUpload();
}


struct MainWorkerContext {
  download::DownloadManager *download_manager;
};

static void *MainWorker(void *data) {
  MainWorkerContext *mwc = static_cast<MainWorkerContext*>(data);
  download::DownloadManager *download_manager = mwc->download_manager;

  while (1) {
    ChunkJob next_chunk;
    {
      MutexLockGuard m(&lock_pipe);
      ReadPipe(pipe_chunks[0], &next_chunk, sizeof(next_chunk));
    }
    if (next_chunk.IsTerminateJob())
      break;

    shash::Any chunk_hash = next_chunk.hash();
    zlib::Algorithms compression_alg = next_chunk.compression_alg;
    LogCvmfs(kLogCvmfs, kLogVerboseMsg, "processing chunk %s",
             chunk_hash.ToString().c_str());

    if (!Peek(chunk_hash)) {
      string tmp_file;
      FILE *fchunk = CreateTempFile(*temp_dir + "/cvmfs", 0600, "w",
                                    &tmp_file);
      string url_chunk = *stratum0_url + "/data/" + chunk_hash.MakePath();
      cvmfs::FileSink filesink(fchunk);
      download::JobInfo download_chunk(&url_chunk, false, false,
                                       &chunk_hash, &filesink);

      const download::Failures download_result =
                                       download_manager->Fetch(&download_chunk);
      if (download_result != download::kFailOk) {
        ReportDownloadError(download_chunk);
        PANIC(kLogStderr, "Download error");
      }
      fclose(fchunk);
      Store(tmp_file, chunk_hash,
            (compression_alg == zlib::kZlibDefault) ? true : false);
      atomic_inc64(&overall_new);
    }
    if (atomic_xadd64(&overall_chunks, 1) % 1000 == 0)
      LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, ".");
    atomic_dec64(&chunk_queue);
  }
  return NULL;
}


bool CommandPull::PullRecursion(catalog::Catalog   *catalog,
                                const std::string  &path) {
  assert(catalog);

  // Previous catalogs
  if (pull_history) {
    shash::Any previous_catalog = catalog->GetPreviousRevision();
    if (previous_catalog.IsNull()) {
      LogCvmfs(kLogCvmfs, kLogStdout, "Start of catalog, no more history");
    } else {
      LogCvmfs(kLogCvmfs, kLogStdout, "Replicating from historic catalog %s",
               previous_catalog.ToString().c_str());
      bool retval = Pull(previous_catalog, path);
      if (!retval)
        return false;
    }
  }

  // Nested catalogs (in a nested code block because goto fail...)
  {
    const catalog::Catalog::NestedCatalogList nested_catalogs =
      catalog->ListOwnNestedCatalogs();
    for (catalog::Catalog::NestedCatalogList::const_iterator i =
         nested_catalogs.begin(), iEnd = nested_catalogs.end();
         i != iEnd; ++i)
    {
      LogCvmfs(kLogCvmfs, kLogStdout, "Replicating from catalog at %s",
               i->mountpoint.c_str());
      bool retval = Pull(i->hash, i->mountpoint.ToString());
      if (!retval)
        return false;
    }
  }

  return true;
}

bool CommandPull::Pull(const shash::Any   &catalog_hash,
                       const std::string  &path) {
  int retval;
  download::Failures dl_retval;
  assert(shash::kSuffixCatalog == catalog_hash.suffix);

  // Check if the catalog already exists
  if (Peek(catalog_hash)) {
    // Preload: dirtab changed
    if (inspect_existing_catalogs) {
      if (!preload_cache) {
        PANIC(kLogStderr, "to be implemented: -t without -c");
      }
      catalog::Catalog *catalog = catalog::Catalog::AttachFreely(
        path, MakePath(catalog_hash), catalog_hash);
      if (catalog == NULL) {
        LogCvmfs(kLogCvmfs, kLogStderr, "failed to attach catalog %s",
                 catalog_hash.ToString().c_str());
        return false;
      }
      bool retval = PullRecursion(catalog, path);
      delete catalog;
      return retval;
    }

    LogCvmfs(kLogCvmfs, kLogStdout, "  Catalog up to date");
    return true;
  }

  // Check if the catalog matches the pathfilter
  if (path != ""              &&  // necessary to load the root catalog
      pathfilter              &&
     !pathfilter->IsMatching(path)) {
    LogCvmfs(kLogCvmfs, kLogStdout, "  Catalog in '%s' does not match"
             " the path specification", path.c_str());
    return true;
  }

  int64_t gauge_chunks = atomic_read64(&overall_chunks);
  int64_t gauge_new = atomic_read64(&overall_new);

  // Download and uncompress catalog
  shash::Any chunk_hash;
  zlib::Algorithms compression_alg;
  catalog::Catalog *catalog = NULL;
  string file_catalog;
  string file_catalog_vanilla;
  FILE *fcatalog = CreateTempFile(*temp_dir + "/cvmfs", 0600, "w",
                                  &file_catalog);
  if (!fcatalog) {
    LogCvmfs(kLogCvmfs, kLogStderr, "I/O error");
    return false;
  }
  fclose(fcatalog);
  FILE *fcatalog_vanilla = CreateTempFile(*temp_dir + "/cvmfs", 0600, "w",
                                          &file_catalog_vanilla);
  if (!fcatalog_vanilla) {
    LogCvmfs(kLogCvmfs, kLogStderr, "I/O error");
    unlink(file_catalog.c_str());
    return false;
  }
  const string url_catalog = *stratum0_url + "/data/" + catalog_hash.MakePath();
  cvmfs::FileSink filesink(fcatalog_vanilla);
  download::JobInfo download_catalog(&url_catalog, false, false,
                                     &catalog_hash, &filesink);
  dl_retval = download_manager()->Fetch(&download_catalog);
  fclose(fcatalog_vanilla);
  if (dl_retval != download::kFailOk) {
    if (path == "" && is_garbage_collectable) {
      LogCvmfs(kLogCvmfs, kLogStdout, "skipping missing root catalog %s - "
                                      "probably sweeped by garbage collection",
               catalog_hash.ToString().c_str());
      goto pull_skip;
    } else {
      ReportDownloadError(download_catalog);
      goto pull_cleanup;
    }
  }
  retval = zlib::DecompressPath2Path(file_catalog_vanilla, file_catalog);
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "decompression failure (file %s, hash %s)",
             file_catalog_vanilla.c_str(), catalog_hash.ToString().c_str());
    goto pull_cleanup;
  }
  if (path.empty() && reflog != NULL) {
    if (!reflog->AddCatalog(catalog_hash)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to add catalog to Reflog.");
      goto pull_cleanup;
    }
  }

  catalog = catalog::Catalog::AttachFreely(path, file_catalog, catalog_hash);
  if (catalog == NULL) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to attach catalog %s",
             catalog_hash.ToString().c_str());
    goto pull_cleanup;
  }

  // Always pull the HEAD root catalog and nested catalogs
  if (apply_timestamp_threshold && (path == "") &&
      (catalog->GetLastModified() < timestamp_threshold))
  {
    LogCvmfs(kLogCvmfs, kLogStdout,
             "  Pruning at root catalog from %s due to threshold at %s",
             StringifyTime(catalog->GetLastModified(), false).c_str(),
             StringifyTime(timestamp_threshold, false).c_str());
    delete catalog;
    goto pull_skip;
  }
  apply_timestamp_threshold = true;

  // Traverse the chunks
  LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
           "  Processing chunks [%" PRIu64 " registered chunks]: ",
           catalog->GetNumChunks());
  retval = catalog->AllChunksBegin();
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to gather chunks");
    goto pull_cleanup;
  }
  while (catalog->AllChunksNext(&chunk_hash, &compression_alg)) {
    ChunkJob next_chunk(chunk_hash, compression_alg);
    WritePipe(pipe_chunks[1], &next_chunk, sizeof(next_chunk));
    atomic_inc64(&chunk_queue);
  }
  catalog->AllChunksEnd();
  while (atomic_read64(&chunk_queue) != 0) {
    SafeSleepMs(100);
  }
  LogCvmfs(kLogCvmfs, kLogStdout, " fetched %" PRId64 " new chunks out of "
           "%" PRId64 " unique chunks",
           atomic_read64(&overall_new)-gauge_new,
           atomic_read64(&overall_chunks)-gauge_chunks);

  retval = PullRecursion(catalog, path);

  delete catalog;
  unlink(file_catalog.c_str());
  WaitForStorage();
  if (!retval)
    return false;
  Store(file_catalog_vanilla, catalog_hash);
  return true;

 pull_cleanup:
  delete catalog;
  unlink(file_catalog.c_str());
  unlink(file_catalog_vanilla.c_str());
  return false;

 pull_skip:
  unlink(file_catalog.c_str());
  unlink(file_catalog_vanilla.c_str());
  return true;
}


int swissknife::CommandPull::Main(const swissknife::ArgumentList &args) {
  int retval;
  manifest::Failures m_retval;
  download::Failures dl_retval;
  unsigned timeout = 60;
  int fd_lockfile = -1;
  string spooler_definition_str;
  manifest::ManifestEnsemble ensemble;
  shash::Any meta_info_hash;
  string meta_info;

  // Option parsing
  if (args.find('c') != args.end())
    preload_cache = true;
  if (args.find('l') != args.end()) {
    unsigned log_level =
      kLogLevel0 << String2Uint64(*args.find('l')->second);
    if (log_level > kLogNone) {
      LogCvmfs(kLogCvmfs, kLogStderr, "invalid log level");
      return 1;
    }
    SetLogVerbosity(static_cast<LogLevels>(log_level));
  }
  stratum0_url = args.find('u')->second;
  temp_dir = args.find('x')->second;
  if (preload_cache) {
    preload_cachedir = new string(*args.find('r')->second);
  } else {
    spooler_definition_str = *args.find('r')->second;
  }
  string master_keys = *args.find('k')->second;
  if (DirectoryExists(master_keys))
    master_keys = JoinStrings(FindFilesBySuffix(master_keys, ".pub"), ":");
  const string repository_name = *args.find('m')->second;
  if (args.find('n') != args.end())
    num_parallel = String2Uint64(*args.find('n')->second);
  if (args.find('t') != args.end())
    timeout = String2Uint64(*args.find('t')->second);
  if (args.find('a') != args.end())
    retries = String2Uint64(*args.find('a')->second);
  if (args.find('d') != args.end()) {
    pathfilter = catalog::RelaxedPathFilter::Create(*args.find('d')->second);
    assert(pathfilter->IsValid());
  }
  if (args.find('p') != args.end())
    pull_history = true;
  if (args.find('z') != args.end())
    inspect_existing_catalogs = true;
  if (args.find('w') != args.end())
    stratum1_url = args.find('w')->second;
  if (args.find('i') != args.end())
    initial_snapshot = true;
  shash::Any reflog_hash;
  string reflog_chksum_path;
  if (args.find('R') != args.end()) {
    reflog_chksum_path = *args.find('R')->second;
    if (!initial_snapshot) {
      if (!manifest::Reflog::ReadChecksum(reflog_chksum_path, &reflog_hash)) {
        LogCvmfs(kLogCvmfs, kLogStderr, "Could not read reflog checksum");
        return 1;
      }
    }
  }
  if (args.find('Z') != args.end()) {
    timestamp_threshold = String2Int64(*args.find('Z')->second);
  }

  if (!preload_cache && stratum1_url.Get() == NULL) {
    LogCvmfs(kLogCvmfs, kLogStderr, "need -w <stratum 1 URL>");
    return 1;
  }

  typedef std::vector<history::History::Tag> TagVector;
  TagVector historic_tags;

  LogCvmfs(kLogCvmfs, kLogStdout, "CernVM-FS: replicating from %s",
           stratum0_url->c_str());

  int result = 1;

  // Initialization
  atomic_init64(&overall_chunks);
  atomic_init64(&overall_new);
  atomic_init64(&chunk_queue);

  const bool     follow_redirects = false;
  const unsigned max_pool_handles = num_parallel+1;
  const string proxy =
      (args.find('@') != args.end()) ? *args.find('@')->second : "";

  if (!this->InitDownloadManager(follow_redirects, proxy, max_pool_handles)) {
    return 1;
  }

  if (!this->InitSignatureManager(master_keys)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to initialize CVMFS signatures");
    return 1;
  } else {
    LogCvmfs(kLogCvmfs, kLogStdout,
             "CernVM-FS: using public key(s) %s",
             JoinStrings(SplitString(master_keys, ':'), ", ").c_str());
  }

  unsigned current_group;
  vector< vector<download::DownloadManager::ProxyInfo> > proxies;
  download_manager()->GetProxyInfo(&proxies, &current_group, NULL);
  if (proxies.size() > 0) {
    string proxy_str = "\nWarning, replicating through proxies\n";
    proxy_str += "  Load-balance groups:\n";
    for (unsigned i = 0; i < proxies.size(); ++i) {
      vector<string> urls;
      for (unsigned j = 0; j < proxies[i].size(); ++j) {
        urls.push_back(proxies[i][j].url);
      }
      proxy_str +=
        "  [" + StringifyInt(i) + "] " + JoinStrings(urls, ", ") + "\n";
    }
    proxy_str += "  Active proxy: [" + StringifyInt(current_group) + "] " +
                 proxies[current_group][0].url;
    LogCvmfs(kLogCvmfs, kLogStdout, "%s\n", proxy_str.c_str());
  }
  download_manager()->SetTimeout(timeout, timeout);
  download_manager()->SetRetryParameters(retries, 500, 2000);
  download_manager()->Spawn();

  // init the download helper
  ObjectFetcher object_fetcher(repository_name,
                               *stratum0_url,
                               *temp_dir,
                               download_manager(),
                               signature_manager());

  pthread_t *workers =
    reinterpret_cast<pthread_t *>(smalloc(sizeof(pthread_t) * num_parallel));

  // Check if we have a replica-ready server
  const string url_sentinel = *stratum0_url + "/.cvmfs_master_replica";
  download::JobInfo download_sentinel(&url_sentinel, false);
  retval = download_manager()->Fetch(&download_sentinel);
  if (retval != download::kFailOk) {
    if (download_sentinel.http_code() == 404) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "This is not a CernVM-FS server for replication");
    } else {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "Failed to contact stratum 0 server (%d - %s)",
               retval, download::Code2Ascii(download_sentinel.error_code()));
    }
    goto fini;
  }

  m_retval = FetchRemoteManifestEnsemble(*stratum0_url,
                                          repository_name,
                                          &ensemble);
  if (m_retval != manifest::kFailOk) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to fetch manifest (%d - %s)",
             m_retval, manifest::Code2Ascii(m_retval));
    goto fini;
  }

  // Get meta info
  meta_info_hash = ensemble.manifest->meta_info();
  if (!meta_info_hash.IsNull()) {
    meta_info_hash = ensemble.manifest->meta_info();
    const string url = *stratum0_url + "/data/" + meta_info_hash.MakePath();
    cvmfs::MemSink metainfo_memsink;
    download::JobInfo download_metainfo(&url, true, false, &meta_info_hash,
                                        &metainfo_memsink);
    dl_retval = download_manager()->Fetch(&download_metainfo);
    if (dl_retval != download::kFailOk) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to fetch meta info (%d - %s)",
               dl_retval, download::Code2Ascii(dl_retval));
      goto fini;
    }
    meta_info = string(reinterpret_cast<char*>(metainfo_memsink.data()),
                       metainfo_memsink.pos());
  }

  is_garbage_collectable = ensemble.manifest->garbage_collectable();

  // Manifest available, now the spooler's hash algorithm can be determined
  // That doesn't actually matter because the replication does no re-hashing
  if (!preload_cache) {
    const upload::SpoolerDefinition
      spooler_definition(spooler_definition_str,
                         ensemble.manifest->GetHashAlgorithm());
    spooler = upload::Spooler::Construct(spooler_definition);
    assert(spooler);
    spooler->RegisterListener(&SpoolerOnUpload);
  }

  // Open the reflog for modification
  if (!preload_cache) {
    if (initial_snapshot) {
      LogCvmfs(kLogCvmfs, kLogStdout, "Creating an empty Reflog for '%s'",
                                      repository_name.c_str());
      reflog = CreateEmptyReflog(*temp_dir, repository_name);
      if (reflog == NULL) {
        LogCvmfs(kLogCvmfs, kLogStderr, "failed to create initial Reflog");
        goto fini;
      }
    } else {
      ObjectFetcher object_fetcher_stratum1(repository_name,
                                            *stratum1_url,
                                            *temp_dir,
                                            download_manager(),
                                            signature_manager());

      if (!reflog_hash.IsNull()) {
        reflog =
          FetchReflog(&object_fetcher_stratum1, repository_name, reflog_hash);
        assert(reflog != NULL);
      } else {
        LogCvmfs(kLogCvmfs, kLogVerboseMsg, "no reflog (ignoring)");
        if (spooler->Peek(".cvmfsreflog")) {
          LogCvmfs(kLogCvmfs, kLogStderr,
                   "no reflog hash specified but reflog is present");
          goto fini;
        }
      }
    }

    if (reflog != NULL) {
      reflog->BeginTransaction();
      // On commit: use manifest's hash algorithm
      reflog_hash.algorithm = ensemble.manifest->GetHashAlgorithm();
    }
  }

  // Fetch tag list.
  // If we are just preloading the cache it is not strictly necessarily to
  // download the entire tag list
  // TODO(molina): add user option to download tags when preloading the cache
  if (!ensemble.manifest->history().IsNull() && !preload_cache) {
    shash::Any history_hash = ensemble.manifest->history();
    const string history_url = *stratum0_url + "/data/"
                                             + history_hash.MakePath();
    const string history_path = *temp_dir + "/" + history_hash.ToString();

    cvmfs::PathSink pathsink(history_path);
    download::JobInfo download_history(&history_url, false, false,
                                       &history_hash, &pathsink);
    dl_retval = download_manager()->Fetch(&download_history);
    if (dl_retval != download::kFailOk) {
      ReportDownloadError(download_history);
      goto fini;
    }
    const std::string history_db_path = history_path + ".uncompressed";
    retval = zlib::DecompressPath2Path(history_path, history_db_path);
    assert(retval);
    history::History *tag_db = history::SqliteHistory::Open(history_db_path);
    if (NULL == tag_db) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to open history database (%s)",
               history_db_path.c_str());
      unlink(history_db_path.c_str());
      goto fini;
    }
    retval = tag_db->List(&historic_tags);
    delete tag_db;
    unlink(history_db_path.c_str());
    if (!retval) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to read history database (%s)",
               history_db_path.c_str());
      goto fini;
    }

    LogCvmfs(kLogCvmfs, kLogStdout, "Found %lu named snapshots",
             historic_tags.size());
    // TODO(jblomer): We should repliacte the previous history dbs, too,
    // in order to avoid races on fail-over between non-synchronized stratum 1s
    LogCvmfs(kLogCvmfs, kLogStdout, "Uploading history database");
    Store(history_path, history_hash);
    WaitForStorage();
    unlink(history_path.c_str());
    if (reflog != NULL && !reflog->AddHistory(history_hash)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to add history to Reflog.");
      goto fini;
    }
  }

  // Starting threads
  MakePipe(pipe_chunks);
  LogCvmfs(kLogCvmfs, kLogStdout, "Starting %u workers", num_parallel);
  MainWorkerContext mwc;
  mwc.download_manager = download_manager();
  for (unsigned i = 0; i < num_parallel; ++i) {
    int retval = pthread_create(&workers[i], NULL, MainWorker,
                                static_cast<void*>(&mwc));
    assert(retval == 0);
  }

  LogCvmfs(kLogCvmfs, kLogStdout, "Replicating from trunk catalog at /");
  retval = Pull(ensemble.manifest->catalog_hash(), "");
  pull_history = false;
  if (!historic_tags.empty()) {
    LogCvmfs(kLogCvmfs, kLogStdout, "Checking tagged snapshots...");
  }
  for (TagVector::const_iterator i    = historic_tags.begin(),
                                 iend = historic_tags.end();
       i != iend; ++i)
  {
    if (Peek(i->root_hash))
      continue;
    LogCvmfs(kLogCvmfs, kLogStdout, "Replicating from %s repository tag",
             i->name.c_str());
    apply_timestamp_threshold = false;
    bool retval2 = Pull(i->root_hash, "");
    retval = retval && retval2;
  }

  // Stopping threads
  LogCvmfs(kLogCvmfs, kLogStdout, "Stopping %u workers", num_parallel);
  for (unsigned i = 0; i < num_parallel; ++i) {
    ChunkJob terminate_workers;
    WritePipe(pipe_chunks[1], &terminate_workers, sizeof(terminate_workers));
  }
  for (unsigned i = 0; i < num_parallel; ++i) {
    int retval = pthread_join(workers[i], NULL);  // NOLINT (false positive)
    assert(retval == 0);
  }
  ClosePipe(pipe_chunks);

  if (!retval)
    goto fini;

  // Upload manifest ensemble
  {
    LogCvmfs(kLogCvmfs, kLogStdout, "Uploading manifest ensemble");
    WaitForStorage();

    if (!Peek(ensemble.manifest->certificate())) {
      StoreBuffer(ensemble.cert_buf,
                  ensemble.cert_size,
                  ensemble.manifest->certificate(), true);
    }
    if (reflog != NULL &&
        !reflog->AddCertificate(ensemble.manifest->certificate())) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to add certificate to Reflog.");
      goto fini;
    }
    if (!meta_info_hash.IsNull()) {
      const unsigned char *info = reinterpret_cast<const unsigned char *>(
        meta_info.data());
      StoreBuffer(info, meta_info.size(), meta_info_hash, true);
      if (reflog != NULL && !reflog->AddMetainfo(meta_info_hash)) {
        LogCvmfs(kLogCvmfs, kLogStderr, "Failed to add metainfo to Reflog.");
        goto fini;
      }
    }

    // Create alternative bootstrapping symlinks for VOMS secured repos
    if (ensemble.manifest->has_alt_catalog_path()) {
      const bool success =
        spooler->PlaceBootstrappingShortcut(ensemble.manifest->certificate()) &&
        spooler->PlaceBootstrappingShortcut(ensemble.manifest->catalog_hash())
          && (ensemble.manifest->history().IsNull() ||
            spooler->PlaceBootstrappingShortcut(ensemble.manifest->history()))
          && (meta_info_hash.IsNull() ||
            spooler->PlaceBootstrappingShortcut(meta_info_hash));

      if (!success) {
        LogCvmfs(kLogCvmfs, kLogStderr,
                 "failed to place root catalog bootstrapping symlinks");
        goto fini;
      }
    }

    // upload Reflog database
    if (!preload_cache && reflog != NULL) {
      reflog->CommitTransaction();
      reflog->DropDatabaseFileOwnership();
      string reflog_path = reflog->database_file();
      delete reflog;
      manifest::Reflog::HashDatabase(reflog_path, &reflog_hash);
      WaitForStorage();  // Reduce the duration of reflog /wo checksum
      spooler->UploadReflog(reflog_path);
      spooler->WaitForUpload();
      unlink(reflog_path.c_str());
      if (spooler->GetNumberOfErrors()) {
        LogCvmfs(kLogCvmfs, kLogStderr, "Failed to upload Reflog (errors: %d)",
                 spooler->GetNumberOfErrors());
        goto fini;
      }
      assert(!reflog_chksum_path.empty());
      manifest::Reflog::WriteChecksum(reflog_chksum_path, reflog_hash);
    }

    if (preload_cache) {
      bool retval =
        ensemble.manifest->ExportBreadcrumb(*preload_cachedir, 0660);
      assert(retval);
    } else {
      // pkcs#7 structure contains content + certificate + signature
      // So there is no race with whitelist and pkcs7 signature being out of
      // sync
      if (ensemble.whitelist_pkcs7_buf) {
        StoreBuffer(ensemble.whitelist_pkcs7_buf, ensemble.whitelist_pkcs7_size,
                    ".cvmfswhitelist.pkcs7", false);
      }
      StoreBuffer(ensemble.whitelist_buf, ensemble.whitelist_size,
                  ".cvmfswhitelist", false);
      StoreBuffer(ensemble.raw_manifest_buf, ensemble.raw_manifest_size,
                  ".cvmfspublished", false);
    }
    LogCvmfs(kLogCvmfs, kLogStdout, "Serving revision %" PRIu64,
             ensemble.manifest->revision());
  }

  WaitForStorage();
  LogCvmfs(kLogCvmfs, kLogStdout, "Fetched %" PRId64 " new chunks out of %"
           PRId64 " processed chunks",
           atomic_read64(&overall_new), atomic_read64(&overall_chunks));
  result = 0;

 fini:
  if (fd_lockfile >= 0)
    UnlockFile(fd_lockfile);
  free(workers);
  delete spooler;
  delete pathfilter;
  return result;
}

}  // namespace swissknife
