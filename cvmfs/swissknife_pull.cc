/**
 * This file is part of the CernVM File System.
 *
 * Replicates a cvmfs repository.  Uses the cvmfs intrinsic Merkle trees
 * to calculate the difference set.
 */

#define _FILE_OFFSET_BITS 64

#include "cvmfs_config.h"
#include "swissknife_pull.h"

#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>

#include <string>
#include <vector>

#include <cstring>

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
// TODO:: retries

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
      download::Failures retval = download::Fetch(&download_chunk);
      if (retval != download::kFailOk) {
        LogCvmfs(kLogCvmfs, kLogStderr, "failed to download %s (%d), abort",
                 url_chunk.c_str(), retval);
        abort();
      }
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

  spooler->WaitFor();
  result = 0;

 fini:
  free(workers);
  signature::Fini();
  download::Fini();
  delete backend_stat;
  delete spooler;
  return result;


  // Connect to the spooler
  //params.spooler = new upload::Spooler(params.paths_out, params.digests_in);
  //bool retval = params.spooler->Connect();
  //if (!retval) {
  //  PrintError("Failed to connect to spooler");
  //  return 1;
  //}

  /*int timeout = 10;

  bool curl_ready = false;
  bool signature_ready = false;
  bool catalog_ready = false;
  bool override_master_test = false;
  string proxies = "";
  string snapshot_name = "";
  hash::t_sha1 dummy;
  string faulty_file = "";
  string diff_file = "";
  ofstream ffaulty;
  ofstream fdiff;

  int result = 3;

  char c;
  while ((c = getopt(argc, argv, "d:u:p:t:b:k:n:r:m:s:ieo:q:wz")) != -1) {
    switch (c) {
      case 'd':
        dir_target = canonical_path(optarg);
        dir_data = dir_target + "/data";
        dir_catalogs = dir_target + "/catalogs";
        break;
      case 't':
        timeout = atoi(optarg);
        break;
      case 'b':
        blacklist = optarg;
        break;
      case 'k':
        pubkey = optarg;
        break;
      case 'n': {
        int tmp = atoi(optarg);
        if (tmp < 1) {
          usage();
          return 1;
        }
        num_parallel = (unsigned)tmp;
        break;
      }
      case 'r': {
        int tmp = atoi(optarg);
        if (tmp < 2) {
          cout << "Set number of retries at least to 2" << endl;
          usage();
          return 1;
        }
        retries = (unsigned)tmp;
        break;
      }
      case 'p':
        proxies = optarg;
        break;
      case 's':
        snapshot_name = optarg;
        break;
      case 'm':
        repo_name = optarg;
        break;
      case 'i':
        initial = true;
        break;
      case 'e':
        exit_on_error = true;
        break;
      case 'o':
        diff_file = optarg;
        keep_log = true;
        break;
      case 'q':
        faulty_file = optarg;
        break;
      case 'w':
        dont_load_chunks = true;
        break;
      case 'z':
        override_master_test = true;
        break;
      case '?':
      default:
        usage();
        return 1;
    }
  }

  if (snapshot_name != "") {
    dir_catalogs = dir_target + "/" + snapshot_name;
  }

  // Sanity checks
  if (!mkdir_deep(dir_data, plain_dir_mode)) {
    cerr << "failed to create data store directory" << endl;
    return 2;
  }
  if (!mkdir_deep(dir_catalogs, plain_dir_mode)) {
    cerr << "failed to create catalog store directory" << endl;
    return 2;
  }
  if (!make_cache_dir(dir_data, plain_dir_mode)) {
    cerr << "failed to initialize data store" << endl;
    return 2;
  }
  if (faulty_file != "") {
    ffaulty.open(faulty_file.c_str());
    if (!ffaulty.is_open()) {
      cerr << "failed to open faulty chunks output file" << endl;
      return 2;
    }
  }
  if (diff_file != "") {
    fdiff.open(diff_file.c_str());
    if (!fdiff.is_open()) {
      cerr << "failed to open difference set output file" << endl;
      return 2;
    }
  }


  signature::init();
  if (!signature::load_public_keys(pubkey)) {
    cout << "Warning: cvmfs public master key could not be loaded." << endl;
    goto pull_cleanup;
  } else {
    cout << "CernVM-FS Pull: using public key(s) "
    <<  join_strings(split_string(pubkey, ':'), ", ") << endl;
  }
  signature_ready = true;

  if (!catalog::init(getuid(), getgid())) {
    cerr << "Failed to initialize catalog" << endl;
    goto pull_cleanup;
  }
  catalog_ready = true;


  // Check if we have a valid starting point
  if (!initial && exit_on_error && !file_exists(dir_catalogs + "/.cvmfs_replica")) {
    cerr << "This is not a consistent repository replica to start with" << endl;
    goto pull_cleanup;
  }

  // start work
  if (!recursive_pull("") && exit_on_error)
    goto pull_cleanup;

  cout << endl << "Overall number of retries required: " << overall_retries << endl;
  if (!faulty_chunks.empty()) {
    cout << "Failed to download: " << endl;
    for (set<string>::const_iterator i = faulty_chunks.begin(), iEnd = faulty_chunks.end();
         i != iEnd; ++i)
    {
      cout << *i << endl;
      ffaulty << *i << endl;
    }
    result = 4;
  } else {
    if (!dont_load_chunks) {
      int fd = open((dir_catalogs + "/.cvmfs_replica").c_str(), O_WRONLY | O_CREAT, plain_file_mode);
      if (fd >= 0) {
        close(fd);
        result = 0;
      } else {
        cout << "Failed to mark replica as consistent" << endl;
        result = 5;
      }
    } else {
      result = 5;
    }
  }

  if (keep_log) {
    for (set<string>::const_iterator i = diff_set.begin(), iEnd = diff_set.end();
         i != iEnd; ++i)
    {
      fdiff << (*i) << endl;
    }
  }

pull_cleanup:
  if (catalog_ready) catalog::fini();
  if (signature_ready) signature::fini();
  if (ffaulty.is_open()) ffaulty.close();
  if (fdiff.is_open()) fdiff.close();
  return result;
*/
}
