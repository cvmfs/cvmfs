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


class AbortSpoolerOnError : public upload::SpoolerCallback {
 public:
  void Callback(const string &path, int retval, const string &digest) {
    if (retval != 0) {
      LogCvmfs(kLogCvmfs, kLogStderr, "spooler failure %d (%s, hash: %s)",
               retval, path.c_str(), digest.c_str());
      abort();
    }
  }
};


string *stratum0_url = NULL;
string *temp_dir = NULL;
unsigned num_parallel = 1;
bool pull_history = false;
upload::Spooler *spooler = NULL;
int pipe_chunks[2];
// required for concurrent reading
pthread_mutex_t lock_pipe = PTHREAD_MUTEX_INITIALIZER;
upload::BackendStat *backend_stat = NULL;
unsigned retries = 3;
atomic_int64 overall_retries;

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

    if (!backend_stat->Stat(chunk_path)) {
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
          if (attempts < retries) {
            // Backoff
            atomic_inc64(&overall_retries);
            usleep((100 + random()%100) * 1000);
            rewind(fchunk);
            int retval = ftruncate(fileno(fchunk), 0);
            assert(retval == 0);
          } else {
            LogCvmfs(kLogCvmfs, kLogStderr, "failed to download %s (%d), abort",
                     url_chunk.c_str(), retval);
            abort();
          }
        }
        attempts++;
      } while ((retval != download::kFailOk) && (attempts < retries));
      fclose(fchunk);
      spooler->SpoolCopy(tmp_file, chunk_path);
    }
  }
  return NULL;
}


static bool Pull(const hash::Any &catalog_hash, const std::string &path) {
  int retval;

  // Check if the catalog already exists
  if (backend_stat->Stat("data" + catalog_hash.MakePath(1, 2) + "C")) {
    LogCvmfs(kLogCvmfs, kLogStdout, "Catalog up to date");
    return true;
  }

  hash::Any chunk_hash;
  catalog::ChunkTypes chunk_type;
  catalog::Catalog *catalog = NULL;
  string file_catalog;
  FILE *fcatalog = CreateTempFile(*temp_dir + "/cvmfs", 0600, "w",
                                  &file_catalog);
  if (!fcatalog) {
    LogCvmfs(kLogCvmfs, kLogStderr, "I/O error");
    return false;
  }
  const string url_catalog = *stratum0_url + "/data" +
                             catalog_hash.MakePath(1, 2) + "C";
  download::JobInfo download_catalog(&url_catalog, true, false, fcatalog,
                                     &catalog_hash);
  retval = download::Fetch(&download_catalog);
  fclose(fcatalog);
  if (retval != download::kFailOk) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to download catalog %s (%d)",
             catalog_hash.ToString().c_str(), retval);
    goto pull_cleanup;
  }

  catalog = catalog::AttachFreely(path, file_catalog);
  if (catalog == NULL) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to attach catalog %s",
             catalog_hash.ToString().c_str());
    goto pull_cleanup;
  }

  // Traverse the chunks
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
        next_chunk.type = 'C';
        break;
      default:
        next_chunk.type = '\0';
    }
    memcpy(next_chunk.digest, chunk_hash.digest, sizeof(chunk_hash.digest));
    WritePipe(pipe_chunks[1], &next_chunk, sizeof(next_chunk));
  }
  catalog->AllChunksEnd();

  // Previous catalogs

  // Nested catalogs

  delete catalog;
  spooler->WaitFor();
  spooler->SpoolProcess(file_catalog, "data", "C");
  return true;

 pull_cleanup:
  delete catalog;
  unlink(file_catalog.c_str());
  return false;
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
  spooler = upload::MakeSpoolerEnsemble(*args.find('r')->second);
  assert(spooler);
  backend_stat = upload::GetBackendStat(*args.find('r')->second);
  assert(backend_stat);
  spooler->set_move_mode(true);
  spooler->SetCallback(new AbortSpoolerOnError());
  const string master_keys = *args.find('k')->second;
  const string repository_name = *args.find('m')->second;
  if (args.find('n') != args.end())
    num_parallel = String2Uint64(*args.find('n')->second);
  if (args.find('t') != args.end())
    timeout = String2Uint64(*args.find('t')->second);
  if (args.find('a') != args.end())
    retries = String2Uint64(*args.find('a')->second);
  atomic_init64(&overall_retries);
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
  download::Init(num_parallel+1);
  download::SetTimeout(timeout, timeout);
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

  LogCvmfs(kLogCvmfs, kLogStdout, "Replicating chunks from catalog at /");
  Pull(ensemble.manifest->catalog_hash(), "");

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

  if (atomic_read64(&overall_retries) > 0) {
    LogCvmfs(kLogCvmfs, kLogStdout, "Overall number of retries: %"PRId64,
             atomic_read64(&overall_retries));
  }

  // Upload manifest ensemble
  spooler->WaitFor();
  // TODO

  spooler->WaitFor();
  result = 0;

 fini:
  free(workers);
  signature::Fini();
  download::Fini();
  delete backend_stat;
  delete spooler;
  return result;
}
