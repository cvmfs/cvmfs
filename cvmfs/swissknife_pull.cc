/**
 * This file is part of the CernVM File System.
 *
 * Replicates a cvmfs repository.  Uses the cvmfs intrinsic Merkle trees
 * to calculate the difference set.
 */

#define _FILE_OFFSET_BITS 64
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

#include "atomic.h"
#include "catalog.h"
#include "download.h"
#include "hash.h"
#include "history_sqlite.h"
#include "logging.h"
#include "manifest.h"
#include "manifest_fetch.h"
#include "signature.h"
#include "smalloc.h"
#include "upload.h"
#include "util.h"

using namespace std;  // NOLINT

namespace swissknife {

namespace {

struct ChunkJob {
  unsigned char type;
  shash::Algorithms hash_algorithm;
  unsigned char digest[shash::kMaxDigestSize];
};

static void SpoolerOnUpload(const upload::SpoolerResult &result) {
  unlink(result.local_path.c_str());
  if (result.return_code != 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "spooler failure %d (%s, hash: %s)",
             result.return_code,
             result.local_path.c_str(),
             result.content_hash.ToString().c_str());
    abort();
  }
}

string              *stratum0_url = NULL;
string              *temp_dir = NULL;
unsigned             num_parallel = 1;
bool                 pull_history = false;
bool                 is_garbage_collectable = false;
upload::Spooler     *spooler = NULL;
int                  pipe_chunks[2];
// required for concurrent reading
pthread_mutex_t      lock_pipe = PTHREAD_MUTEX_INITIALIZER;
unsigned             retries = 3;
atomic_int64         overall_chunks;
atomic_int64         overall_new;
atomic_int64         chunk_queue;
bool                 preload_cache = false;
string              *preload_cachedir = NULL;

}  // anonymous namespace


static bool Peek(const string &remote_path, const char suffix)
{
  if (preload_cache) {
    // Strip "data"
    return FileExists(*preload_cachedir + remote_path.substr(4));
  } else {
    string real_remote_path = remote_path;
    if (suffix != 0)
      real_remote_path.push_back(suffix);
    return spooler->Peek(real_remote_path);
  }
}


static void Store(const string &local_path, const string &remote_path,
                  const char suffix)
{
  if (preload_cache) {
    // Strip "data"
    const string dest_path = *preload_cachedir + remote_path.substr(4);
    string tmp_dest;
    FILE *fdest = CreateTempFile(dest_path, 0660, "w", &tmp_dest);
    if (fdest == NULL) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to create temporary file");
      abort();
    }
    int retval = zlib::DecompressPath2File(local_path, fdest);
    if (!retval) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to preload %s to %s",
               local_path.c_str(), dest_path.c_str());
      abort();
    }
    fclose(fdest);
    retval = rename(tmp_dest.c_str(), dest_path.c_str());
    assert(retval == 0);
    unlink(local_path.c_str());
  } else {
    string real_remote_path = remote_path;
    if (suffix != 0)
      real_remote_path.push_back(suffix);
    spooler->Upload(local_path, real_remote_path);
  }
}


static void StoreBuffer(const unsigned char *buffer, const unsigned size,
                        const std::string dest_path, const char suffix,
                        const bool compress)
{
  string tmp_file;
  FILE *ftmp = CreateTempFile(*temp_dir + "/cvmfs", 0600, "w", &tmp_file);
  assert(ftmp);
  int retval;
  if (compress) {
    shash::Any dummy(shash::kSha1);  // hardcoded hash no problem, unsused
    retval = zlib::CompressMem2File(buffer, size, ftmp, &dummy);
  } else {
    retval = CopyMem2File(buffer, size, ftmp);
  }
  assert(retval);
  fclose(ftmp);
  Store(tmp_file, dest_path, suffix);
}


static void WaitForStorage() {
  if (!preload_cache) spooler->WaitForUpload();
}


static void *MainWorker(void *data) {
  while (1) {
    ChunkJob next_chunk;
    pthread_mutex_lock(&lock_pipe);
    ReadPipe(pipe_chunks[0], &next_chunk, sizeof(next_chunk));
    pthread_mutex_unlock(&lock_pipe);
    if (next_chunk.type == 255)
      break;

    shash::Any chunk_hash(next_chunk.hash_algorithm, next_chunk.digest,
                          shash::kDigestSizes[next_chunk.hash_algorithm]);
    LogCvmfs(kLogCvmfs, kLogVerboseMsg, "processing chunk %s",
             chunk_hash.ToString().c_str());
    string chunk_path = "data" + chunk_hash.MakePathExplicit(1, 2);

    if (!Peek(chunk_path, next_chunk.type)) {
      string tmp_file;
      FILE *fchunk = CreateTempFile(*temp_dir + "/cvmfs", 0600, "w",
                                    &tmp_file);
      assert(fchunk);
      string url_chunk = *stratum0_url + "/" + chunk_path;
      if (next_chunk.type != 0)
        url_chunk.push_back(next_chunk.type);
      download::JobInfo download_chunk(&url_chunk, false, false, fchunk,
                                       &chunk_hash);

      unsigned attempts = 0;
      download::Failures retval;
      do {
        retval = g_download_manager->Fetch(&download_chunk);
        if (retval != download::kFailOk) {
          LogCvmfs(kLogCvmfs, kLogStderr, "failed to download %s (%d - %s), "
                   "abort", url_chunk.c_str(),
                   retval, download::Code2Ascii(retval));
          abort();
        }
        attempts++;
      } while ((retval != download::kFailOk) && (attempts < retries));
      fclose(fchunk);
      Store(tmp_file, chunk_path, next_chunk.type);
      atomic_inc64(&overall_new);
    }
    if (atomic_xadd64(&overall_chunks, 1) % 1000 == 0)
      LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, ".");
    atomic_dec64(&chunk_queue);
  }
  return NULL;
}


static bool Pull(const shash::Any &catalog_hash, const std::string &path) {
  int retval;
  download::Failures dl_retval;

  // Check if the catalog already exists
  if (Peek("data" + catalog_hash.MakePathExplicit(1, 2), 'C')) {
    LogCvmfs(kLogCvmfs, kLogStdout, "  Catalog up to date");
    return true;
  }

  int64_t gauge_chunks = atomic_read64(&overall_chunks);
  int64_t gauge_new = atomic_read64(&overall_new);

  // Download and uncompress catalog
  shash::Any chunk_hash;
  catalog::ChunkTypes chunk_type;
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
  const string url_catalog = *stratum0_url + "/data" +
                             catalog_hash.MakePathExplicit(1, 2) + "C";
  download::JobInfo download_catalog(&url_catalog, false, false,
                                     fcatalog_vanilla, &catalog_hash);
  dl_retval = g_download_manager->Fetch(&download_catalog);
  fclose(fcatalog_vanilla);
  if (dl_retval != download::kFailOk) {
    if (path == "" && is_garbage_collectable) {
      LogCvmfs(kLogCvmfs, kLogStdout, "skipping missing root catalog %s - "
                                      "probably sweeped by garbage collection",
               catalog_hash.ToString().c_str());
      goto pull_skip;
    } else {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to download catalog %s (%d - %s)",
               catalog_hash.ToString().c_str(), dl_retval,
               download::Code2Ascii(dl_retval));
      goto pull_cleanup;
    }
  }
  retval = zlib::DecompressPath2Path(file_catalog_vanilla, file_catalog);
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "decompression failure (file %s, hash %s)",
             file_catalog_vanilla.c_str(), catalog_hash.ToString().c_str());
    goto pull_cleanup;
  }

  catalog = catalog::Catalog::AttachFreely(path, file_catalog, catalog_hash);
  if (catalog == NULL) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to attach catalog %s",
             catalog_hash.ToString().c_str());
    goto pull_cleanup;
  }

  // Traverse the chunks
  LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
           "  Processing chunks: ");
  retval = catalog->AllChunksBegin();
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to gather chunks");
    goto pull_cleanup;
  }
  while (catalog->AllChunksNext(&chunk_hash, &chunk_type)) {
    ChunkJob next_chunk;
    switch (chunk_type) {
      case catalog::kChunkMicroCatalog:
        next_chunk.type = 'L';
        break;
      case catalog::kChunkPiece:
        next_chunk.type = shash::kSuffixPartial;
        break;
      default:
        next_chunk.type = '\0';
    }
    next_chunk.hash_algorithm = chunk_hash.algorithm;
    memcpy(next_chunk.digest, chunk_hash.digest, chunk_hash.GetDigestSize());
    WritePipe(pipe_chunks[1], &next_chunk, sizeof(next_chunk));
    atomic_inc64(&chunk_queue);
  }
  catalog->AllChunksEnd();
  while (atomic_read64(&chunk_queue) != 0) {
    SafeSleepMs(100);
  }
  LogCvmfs(kLogCvmfs, kLogStdout, " fetched %"PRId64" new chunks out of "
           "%"PRId64" processed chunks",
           atomic_read64(&overall_new)-gauge_new,
           atomic_read64(&overall_chunks)-gauge_chunks);

  // Previous catalogs
  if (pull_history) {
    shash::Any previous_catalog = catalog->GetPreviousRevision();
    if (previous_catalog.IsNull()) {
      LogCvmfs(kLogCvmfs, kLogStdout, "Start of catalog, no more history");
    } else {
      LogCvmfs(kLogCvmfs, kLogStdout, "Replicating from historic catalog %s",
               previous_catalog.ToString().c_str());
      retval = Pull(previous_catalog, path);
      if (!retval)
        return false;
    }
  }

  // Nested catalogs (in a nested code block because goto fail...)
  {
    const catalog::Catalog::NestedCatalogList &nested_catalogs =
      catalog->ListNestedCatalogs();
    for (catalog::Catalog::NestedCatalogList::const_iterator i =
         nested_catalogs.begin(), iEnd = nested_catalogs.end();
         i != iEnd; ++i)
    {
      LogCvmfs(kLogCvmfs, kLogStdout, "Replicating from catalog at %s",
               i->path.c_str());
      retval = Pull(i->hash, i->path.ToString());
      if (!retval)
        return false;
    }
  }

  delete catalog;
  unlink(file_catalog.c_str());
  WaitForStorage();
  Store(file_catalog_vanilla, "data" + catalog_hash.MakePathExplicit(1, 2),
        'C');
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
  unsigned timeout = 10;
  int fd_lockfile = -1;
  string spooler_definition_str;
  manifest::ManifestEnsemble ensemble;

  // Option parsing
  if (args.find('c') != args.end())
    preload_cache = true;
  if (args.find('l') != args.end()) {
    unsigned log_level =
    1 << (kLogLevel0 + String2Uint64(*args.find('l')->second));
    if (log_level > kLogNone) {
      swissknife::Usage();
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
  const string master_keys = *args.find('k')->second;
  const string repository_name = *args.find('m')->second;
  string trusted_certs;
  if (args.find('y') != args.end())
    trusted_certs = *args.find('y')->second;
  if (args.find('n') != args.end())
    num_parallel = String2Uint64(*args.find('n')->second);
  if (args.find('t') != args.end())
    timeout = String2Uint64(*args.find('t')->second);
  if (args.find('a') != args.end())
    retries = String2Uint64(*args.find('a')->second);
  if (args.find('p') != args.end())
    pull_history = true;
  pthread_t *workers =
    reinterpret_cast<pthread_t *>(smalloc(sizeof(pthread_t) * num_parallel));
  typedef std::vector<history::History::Tag> TagVector;
  TagVector historic_tags;

  LogCvmfs(kLogCvmfs, kLogStdout, "CernVM-FS: replicating from %s",
           stratum0_url->c_str());

  int result = 1;
  const string url_sentinel = *stratum0_url + "/.cvmfs_master_replica";
  download::JobInfo download_sentinel(&url_sentinel, false);

  // Initialization
  atomic_init64(&overall_chunks);
  atomic_init64(&overall_new);
  atomic_init64(&chunk_queue);
  g_download_manager->Init(num_parallel+1, true, g_statistics);
  // download::ActivatePipelining();
  unsigned current_group;
  vector< vector<download::DownloadManager::ProxyInfo> > proxies;
  g_download_manager->GetProxyInfo(&proxies, &current_group, NULL);
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
  g_download_manager->SetTimeout(timeout, timeout);
  g_download_manager->SetRetryParameters(retries, timeout, 3*timeout);
  g_download_manager->Spawn();
  g_signature_manager->Init();
  if (!g_signature_manager->LoadPublicRsaKeys(master_keys)) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "cvmfs public master key could not be loaded.");
    goto fini;
  } else {
    LogCvmfs(kLogCvmfs, kLogStdout,
             "CernVM-FS: using public key(s) %s",
             JoinStrings(SplitString(master_keys, ':'), ", ").c_str());
  }
  if (trusted_certs != "") {
    if (!g_signature_manager->LoadTrustedCaCrl(trusted_certs)) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "trusted certificates from %s could not be loaded",
               trusted_certs.c_str());
      goto fini;
    }
    LogCvmfs(kLogCvmfs, kLogStdout,
             "CernVM-FS: using trusted certificates in %s",
             JoinStrings(SplitString(trusted_certs, ':'), ", ").c_str());
  }

  m_retval = manifest::Fetch(*stratum0_url, repository_name, 0, NULL,
                           g_signature_manager, g_download_manager, &ensemble);
  if (m_retval != manifest::kFailOk) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to fetch manifest (%d - %s)",
             m_retval, manifest::Code2Ascii(m_retval));
    goto fini;
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

  // Fetch tag list
  if (!ensemble.manifest->history().IsNull()) {
    shash::Any history_hash = ensemble.manifest->history();
    const string history_url = *stratum0_url + "/data" +
      history_hash.MakePathExplicit(1, 2) + "H";
    const string history_path = *temp_dir + "/" + history_hash.ToString();
    download::JobInfo download_history(&history_url, false, false,
                                       &history_path,
                                       &history_hash);
    dl_retval = g_download_manager->Fetch(&download_history);
    if (dl_retval != download::kFailOk) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to download history (%d - %s)",
               dl_retval, download::Code2Ascii(dl_retval));
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

    LogCvmfs(kLogCvmfs, kLogStdout, "Found %u named snapshots",
             historic_tags.size());
    LogCvmfs(kLogCvmfs, kLogStdout, "Uploading history database");
    Store(history_path, "data" + history_hash.MakePathExplicit(1, 2), 'H');
    WaitForStorage();
    unlink(history_path.c_str());
  }

  // Check if we have a replica-ready server
  retval = g_download_manager->Fetch(&download_sentinel);
  if (retval != download::kFailOk) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "This is not a CernVM-FS server for replication");
    goto fini;
  }

  // Starting threads
  MakePipe(pipe_chunks);
  LogCvmfs(kLogCvmfs, kLogStdout, "Starting %u workers", num_parallel);
  for (unsigned i = 0; i < num_parallel; ++i) {
    int retval = pthread_create(&workers[i], NULL, MainWorker, NULL);
    assert(retval == 0);
  }

  LogCvmfs(kLogCvmfs, kLogStdout, "Replicating from trunk catalog at /");
  retval = Pull(ensemble.manifest->catalog_hash(), "");
  pull_history = false;
  for (TagVector::const_iterator i    = historic_tags.begin(),
                                 iend = historic_tags.end();
       i != iend; ++i) {
    LogCvmfs(kLogCvmfs, kLogStdout, "Replicating from %s repository tag",
             i->name.c_str());
    bool retval2 = Pull(i->root_hash, "");
    retval = retval && retval2;
  }

  // Stopping threads
  LogCvmfs(kLogCvmfs, kLogStdout, "Stopping %u workers", num_parallel);
  for (unsigned i = 0; i < num_parallel; ++i) {
    ChunkJob terminate_workers;
    terminate_workers.type = 255;
    WritePipe(pipe_chunks[1], &terminate_workers, sizeof(terminate_workers));
  }
  for (unsigned i = 0; i < num_parallel; ++i) {
    int retval = pthread_join(workers[i], NULL);
    assert(retval == 0);
  }
  ClosePipe(pipe_chunks);

  if (!retval)
    goto fini;

  if (g_download_manager->GetCounters().n_num_retries > 0) {
    LogCvmfs(kLogCvmfs, kLogStdout, "Overall number of retries: %"PRId64,
             g_download_manager->GetCounters().n_num_retries);
  }

  // Upload manifest ensemble
  {
    LogCvmfs(kLogCvmfs, kLogStdout, "Uploading manifest ensemble");
    WaitForStorage();
    const string certificate_path =
      "data" + ensemble.manifest->certificate().MakePathExplicit(1, 2);
    if (!Peek(certificate_path, 'X')) {
      StoreBuffer(ensemble.cert_buf, ensemble.cert_size, certificate_path,
                  'X', true);
    }
    if (preload_cache) {
      bool retval = ensemble.manifest->ExportChecksum(*preload_cachedir, 0660);
      assert(retval);
    } else {
      // pkcs#7 structure contains content + certificate + signature
      // So there is no race with whitelist and pkcs7 signature being out of
      // sync
      if (ensemble.whitelist_pkcs7_buf) {
        StoreBuffer(ensemble.whitelist_pkcs7_buf, ensemble.whitelist_pkcs7_size,
                    ".cvmfswhitelist.pkcs7", 0, false);
      }
      StoreBuffer(ensemble.whitelist_buf, ensemble.whitelist_size,
                  ".cvmfswhitelist", 0, false);
      StoreBuffer(ensemble.raw_manifest_buf, ensemble.raw_manifest_size,
                  ".cvmfspublished", 0, false);
    }
  }

  WaitForStorage();
  LogCvmfs(kLogCvmfs, kLogStdout, "Fetched %"PRId64" new chunks out of %"
           PRId64" processed chunks",
           atomic_read64(&overall_new), atomic_read64(&overall_chunks));
  result = 0;

 fini:
  if (fd_lockfile >= 0)
    UnlockFile(fd_lockfile);
  free(workers);
  g_signature_manager->Fini();
  g_download_manager->Fini();
  delete spooler;
  return result;
}

}  // namespace swissknife
