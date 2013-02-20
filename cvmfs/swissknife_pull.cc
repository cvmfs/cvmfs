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

#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>
#include <inttypes.h>

#include <string>
#include <vector>

#include <cstring>
#include <cstdlib>

#include "upload.h"
#include "logging.h"
#include "download.h"
#include "util.h"
#include "manifest.h"
#include "manifest_fetch.h"
#include "signature.h"
#include "catalog.h"
#include "smalloc.h"
#include "hash.h"
#include "atomic.h"

using namespace std;  // NOLINT

namespace {

struct ChunkJob {
  unsigned char type;
  unsigned char digest[hash::kMaxDigestSize];
};


static void AbortSpoolerOnError(const upload::SpoolerResult &result) {
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
upload::Spooler     *spooler = NULL;
int                  pipe_chunks[2];
// required for concurrent reading
pthread_mutex_t      lock_pipe = PTHREAD_MUTEX_INITIALIZER;
unsigned             retries = 3;
atomic_int64         overall_chunks;
atomic_int64         overall_new;
atomic_int64         chunk_queue;

}


static void *MainWorker(void *data) {
  while (1) {
    ChunkJob next_chunk;
    pthread_mutex_lock(&lock_pipe);
    ReadPipe(pipe_chunks[0], &next_chunk, sizeof(next_chunk));
    pthread_mutex_unlock(&lock_pipe);
    if (next_chunk.type == 255)
      break;

    hash::Any chunk_hash(hash::kSha1, next_chunk.digest,
                         hash::kDigestSizes[hash::kSha1]);
    LogCvmfs(kLogCvmfs, kLogVerboseMsg, "processing chunk %s",
             chunk_hash.ToString().c_str());
    string chunk_path = "data" + chunk_hash.MakePath(1, 2);
    if (next_chunk.type != 0)
      chunk_path.push_back(next_chunk.type);

    if (!spooler->Peek(chunk_path)) {
      string tmp_file;
      FILE *fchunk = CreateTempFile(*temp_dir + "/cvmfs", 0600, "w",
                                    &tmp_file);
      assert(fchunk);
      const string url_chunk = *stratum0_url + "/" + chunk_path;
      download::JobInfo download_chunk(&url_chunk, false, false, fchunk,
                                       &chunk_hash);

      unsigned attempts = 0;
      download::Failures retval;
      do {
        retval = download::Fetch(&download_chunk);
        if (retval != download::kFailOk) {
          LogCvmfs(kLogCvmfs, kLogStderr, "failed to download %s (%d), abort",
                   url_chunk.c_str(), retval);
          abort();
        }
        attempts++;
      } while ((retval != download::kFailOk) && (attempts < retries));
      fclose(fchunk);
      spooler->Upload(tmp_file, chunk_path);
      atomic_inc64(&overall_new);
    }
    if (atomic_xadd64(&overall_chunks, 1) % 1000 == 0)
      LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, ".");
    atomic_dec64(&chunk_queue);
  }
  return NULL;
}


static bool Pull(const hash::Any &catalog_hash, const std::string &path,
                 const bool with_nested)
{
  int retval;

  // Check if the catalog already exists
  if (spooler->Peek("data" + catalog_hash.MakePath(1, 2) + "C")) {
    LogCvmfs(kLogCvmfs, kLogStdout, "  Catalog up to date");
    return true;
  }

  int64_t gauge_chunks = atomic_read64(&overall_chunks);
  int64_t gauge_new = atomic_read64(&overall_new);

  // Download and uncompress catalog
  hash::Any chunk_hash;
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
                             catalog_hash.MakePath(1, 2) + "C";
  download::JobInfo download_catalog(&url_catalog, false, false,
                                     fcatalog_vanilla, &catalog_hash);
  retval = download::Fetch(&download_catalog);
  fclose(fcatalog_vanilla);
  if (retval != download::kFailOk) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to download catalog %s (%d)",
             catalog_hash.ToString().c_str(), retval);
    goto pull_cleanup;
  }
  retval = zlib::DecompressPath2Path(file_catalog_vanilla, file_catalog);
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "decompression failure (file %s, hash %s)",
             file_catalog_vanilla.c_str(), catalog_hash.ToString().c_str());
    goto pull_cleanup;
  }

  catalog = catalog::AttachFreely(path, file_catalog);
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
        next_chunk.type = FileChunk::kCasSuffix.c_str()[0];
        break;
      default:
        next_chunk.type = '\0';
    }
    memcpy(next_chunk.digest, chunk_hash.digest, sizeof(chunk_hash.digest));
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
    hash::Any previous_catalog = catalog->GetPreviousRevision();
    if (previous_catalog.IsNull()) {
      LogCvmfs(kLogCvmfs, kLogStdout, "Start of catalog, no more history");
    } else {
      LogCvmfs(kLogCvmfs, kLogStdout, "Replicating from historic catalog %s",
               previous_catalog.ToString().c_str());
      retval = Pull(previous_catalog, path, false);
      if (!retval)
        return false;
    }
  }


  // Nested catalogs
  if (with_nested) {
    catalog::Catalog::NestedCatalogList *nested_catalogs =
      catalog->ListNestedCatalogs();
    assert(nested_catalogs);
    for (catalog::Catalog::NestedCatalogList::const_iterator i =
         nested_catalogs->begin(), iEnd = nested_catalogs->end();
         i != iEnd; ++i)
    {
      LogCvmfs(kLogCvmfs, kLogStdout, "Replicating from catalog at %s",
               i->path.c_str());
      retval = Pull(i->hash, i->path.ToString(), true);
      if (!retval)
        return false;
    }
  }

  delete catalog;
  unlink(file_catalog.c_str());
  spooler->WaitForUpload();
  spooler->Upload(file_catalog_vanilla,
                  "data" + catalog_hash.MakePath(1, 2) + "C");
  return true;

 pull_cleanup:
  delete catalog;
  unlink(file_catalog.c_str());
  unlink(file_catalog_vanilla.c_str());
  return false;
}


static void UploadBuffer(const unsigned char *buffer, const unsigned size,
                         const std::string dest_path, const bool compress)
{
  string tmp_file;
  FILE *ftmp = CreateTempFile(*temp_dir + "/cvmfs", 0600, "w", &tmp_file);
  assert(ftmp);
  int retval;
  if (compress) {
    hash::Any dummy(hash::kSha1);
    retval = zlib::CompressMem2File(buffer, size, ftmp, &dummy);
  } else {
    retval = CopyMem2File(buffer, size, ftmp);
  }
  assert(retval);
  fclose(ftmp);
  spooler->Upload(tmp_file, dest_path);
}


int swissknife::CommandPull::Main(const swissknife::ArgumentList &args) {
  int retval;
  unsigned timeout = 10;
  manifest::ManifestEnsemble ensemble;

  // Option parsing
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
  const upload::SpoolerDefinition spooler_definition(*args.find('r')->second);
  spooler = upload::Spooler::Construct(spooler_definition);
  assert(spooler);
  spooler->RegisterListener(&AbortSpoolerOnError);
  const string master_keys = *args.find('k')->second;
  const string repository_name = *args.find('m')->second;
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

  LogCvmfs(kLogCvmfs, kLogStdout, "CernVM-FS: replicating from %s",
           stratum0_url->c_str());

  int result = 1;
  const string url_sentinel = *stratum0_url + "/.cvmfs_master_replica";
  download::JobInfo download_sentinel(&url_sentinel, false);

  // Initialization
  atomic_init64(&overall_chunks);
  atomic_init64(&overall_new);
  atomic_init64(&chunk_queue);
  download::Init(num_parallel+1);
  download::SetTimeout(timeout, timeout);
  download::SetRetryParameters(retries, timeout, 3*timeout);
  download::Spawn();
  signature::Init();
  if (!signature::LoadPublicRsaKeys(master_keys)) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "cvmfs public master key could not be loaded.");
    goto fini;
  } else {
    LogCvmfs(kLogCvmfs, kLogStdout,
             "CernVM-FS: using public key(s) %s",
             JoinStrings(SplitString(master_keys, ':'), ", ").c_str());
  }

  retval = manifest::Fetch(*stratum0_url, repository_name, 0, NULL, &ensemble);
  if (retval != manifest::kFailOk) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to fetch manifest (%d)", retval);
    goto fini;
  }

  // Check if we have a replica-ready server
  retval = download::Fetch(&download_sentinel);
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

  LogCvmfs(kLogCvmfs, kLogStdout, "Replicating from catalog at /");
  retval = Pull(ensemble.manifest->catalog_hash(), "", true);

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

  if (download::GetStatistics().num_retries > 0) {
    LogCvmfs(kLogCvmfs, kLogStdout, "Overall number of retries: %"PRId64,
             download::GetStatistics().num_retries);
  }

  // Upload manifest ensemble
  {
    LogCvmfs(kLogCvmfs, kLogStdout, "Uploading manifest ensemble");
    spooler->WaitForUpload();
    const string certificate_path =
      "data" + ensemble.manifest->certificate().MakePath(1, 2) + "X";
    if (!spooler->Peek(certificate_path)) {
      UploadBuffer(ensemble.cert_buf, ensemble.cert_size, certificate_path,
                   true);
    }
    UploadBuffer(ensemble.whitelist_buf, ensemble.whitelist_size,
                 ".cvmfswhitelist", false);
    UploadBuffer(ensemble.raw_manifest_buf, ensemble.raw_manifest_size,
                 ".cvmfspublished", false);
  }

  spooler->WaitForUpload();
  LogCvmfs(kLogCvmfs, kLogStdout, "Fetched %"PRId64" new chunks out of %"
           PRId64" processed chunks",
           atomic_read64(&overall_new), atomic_read64(&overall_chunks));
  result = 0;

 fini:
  free(workers);
  signature::Fini();
  download::Fini();
  delete spooler;
  return result;
}
