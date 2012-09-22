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

#include <string>

#include "upload.h"
#include "logging.h"
#include "download.h"
#include "util.h"
#include "manifest.h"
#include "manifest_fetch.h"
#include "signature.h"

using namespace std;  // NOLINT

namespace {
string *url = NULL;
string *temp_dir = NULL;
unsigned num_parallel = 1;
/*string dir_target = "";
string dir_data = "";
string dir_catalogs = "";
string url = "";
string blacklist = "/etc/cvmfs/blacklist";
string pubkey = "/etc/cvmfs/keys/cern.ch.pub";
string repo_name = "";
bool dont_load_chunks = false;
unsigned retries = 2;
unsigned overall_retries = 0;
bool initial = false;
bool exit_on_error = false;
set<string> faulty_chunks;
set<string> download_bucket;
set<string> diff_set;
bool keep_log = false;
*/
}

/*static bool fetch_deprecated_catalog(const hash::t_sha1 &snapshot_id, string &tmp_path) {
  FILE *tmp_fp = temp_file(dir_data + "/txn/cvmfs.catalog", plain_file_mode, "w", tmp_path);
  if (!tmp_fp)
    return false;

  hash::t_sha1 sha1_rcvd;
  const string sha1_clg_str = snapshot_id.to_string();
  const string url_clg = "/data/" + sha1_clg_str.substr(0, 2) + "/" +
  sha1_clg_str.substr(2) + "C";
  if ((curl_download_stream(url_clg.c_str(), tmp_fp, sha1_rcvd.digest, 1, 1) != CURLE_OK) ||
      (sha1_rcvd != snapshot_id))
  {
    cerr << "Warning: failed to load deprecated catalog from " << url_clg << endl;
  }
  fclose(tmp_fp);
  if (sha1_rcvd != snapshot_id) {
    unlink(tmp_path.c_str());
    return false;
  }
*/
  /* write catalog */
  /*const string clg_data_path = dir_catalogs + "/data/" + sha1_clg_str.substr(0, 2) + "/" +
  sha1_clg_str.substr(2) + "C";
  if (!file_exists(clg_data_path)) {
    cout << "Writing catalog to " << clg_data_path << endl;
    if (compress_file(tmp_path.c_str(), clg_data_path.c_str()) != 0) {
      cerr << "Warning: failed to store catalog" << endl;
    }
  }

  return true;
}*/


/**
 * Returns number of successful downloads
 */
/*static int download_bunch(const bool ignore_errors = false) {
  unsigned num = download_bucket.size();
  int *curl_result = (int *)alloca(num * sizeof(int));
  vector<char *> tmp_path_cstr;
  vector<char *> rpath_cstr;
  vector<string> final_path;
  unsigned char *digest = (unsigned char *)alloca(num * hash::t_sha1::BIT_SIZE / 8);
  vector<hash::t_sha1> expected;
  vector<unsigned> remaining;

  for (set<string>::const_iterator i = download_bucket.begin(), iEnd = download_bucket.end();
       i != iEnd; ++i)
  {
    const string tmp_path = dir_data + "/txn/" + (*i);
    tmp_path_cstr.push_back(strdupa(tmp_path.c_str()));
    const string sha1_str = "/" + i->substr(0, 2) + "/" + i->substr(2);
    const string rpath = url + "/data" + sha1_str;
    rpath_cstr.push_back(strdupa(rpath.c_str()));
    final_path.push_back(dir_data + sha1_str);
    hash::t_sha1 expected_sha1;
    expected_sha1.from_hash_str(*i);
    expected.push_back(expected_sha1);
  }
  download_bucket.clear();

  //cout << "WE GOT " << string(rpath_cstr[0]) << " AND " << string(tmp_path_cstr[0]) << " AND " << final_path[0] << endl;
  curl_download_parallel(num, &(rpath_cstr[0]), &(tmp_path_cstr[0]), digest, curl_result);

  for (unsigned i = 0; i < num; ++i) {
    if ((curl_result[i] == CURLE_OK) && (expected[i] == hash::t_sha1(digest+i*20, 20))) {
      if (rename(tmp_path_cstr[i], final_path[i].c_str()) != 0) {
        remaining.push_back(i);
      }
    } else {
      remaining.push_back(i);
    }
  }

  int faulty = 0;
  if (!remaining.empty()) {
    cerr << "Parallel download failures " << remaining.size() << "/" << num << endl;
    for (vector<unsigned>::const_iterator i = remaining.begin(), iEnd = remaining.end();
         i != iEnd; ++i)
    {
      hash::t_sha1 sha1;
      unsigned again = 1;
      bool result;
      do {
        result = (curl_download_path_nocache(rpath_cstr[*i], tmp_path_cstr[*i], sha1.digest, 0, 0) == CURLE_OK) &&
        (sha1 == expected[*i]);
        if (!result)
          unlink(tmp_path_cstr[*i]);
        again++;
      } while (!result && (again < retries));
      overall_retries += again-1;

      if (result) {
        if ((rename(tmp_path_cstr[*i], (final_path[*i]).c_str()) != 0) && !ignore_errors) {
          cerr << "Warning: rename to " << final_path[*i] << " failed" << endl;
          faulty++;
          faulty_chunks.insert(rpath_cstr[*i]);
          unlink(tmp_path_cstr[*i]);
        }
      } else {
        cerr << "Warning: failed to download " << final_path[*i] << endl;
        if (!ignore_errors) {
          faulty++;
          faulty_chunks.insert(string(rpath_cstr[*i]));
        }
      }
    }
  }

  return num-faulty;
}

static void recursive_ls(const hash::t_md5 dir, const string &path,
                         uint64_t &no_entries, uint64_t &no_regular, uint64_t &no_prels, uint64_t &no_downloads,
                         const bool ignore_errors = false)
{
  vector<catalog::t_dirent> entries;
  entries = catalog::ls_unprotected(dir);

  for (unsigned i = 0; i < entries.size(); ++i) {
    if ((no_entries++ % 1000) == 0)
      cout << "." << flush;

    const string full_path = path + "/" + entries[i].name;
    if (entries[i].flags & catalog::DIR) {
      recursive_ls(hash::t_md5(full_path), full_path,
                   no_entries, no_regular, no_prels, no_downloads);
    }
    if (entries[i].checksum != hash::t_sha1()) {
      string suffix = "";
      if (entries[i].flags & catalog::FILE) {
        no_regular++;
      } else if (entries[i].flags & catalog::DIR) {
        no_prels++;
        suffix = "L";
      }
      const string sha1_str = entries[i].checksum.to_string() + suffix;
      const string chunk_path = "/" + sha1_str.substr(0, 2) + "/" + sha1_str.substr(2);
      if (!file_exists(dir_data + chunk_path)) {
        if (keep_log)
          diff_set.insert(url + "/data" + chunk_path);
        if (!dont_load_chunks)
          download_bucket.insert(sha1_str);

        if (download_bucket.size() == num_parallel)
          no_downloads += download_bunch(ignore_errors);
      }
    }
  }
}



static bool recursive_pull(const string &path)
{
  hash::t_md5 md5(path);

  cout << "Pulling catalog at " << ((path == "") ? "/" : path) << endl;
  if (!mkdir_deep(dir_catalogs + path, plain_dir_mode)) {
    cerr << "Warning: failed to create " << dir_catalogs + path << endl;
    return false;
  }

  string backlink = "../";
  string parent = get_parent_path(dir_catalogs + path);
  while (parent != get_parent_path(dir_data)) {
    if (parent == "") {
      cerr << "Warning: cannot find data dir" << endl;
      return false;
    }
    parent = get_parent_path(parent);
    backlink += "../";
  }

  const string lnk_path_data = dir_catalogs + path + "/data";
  const string backlink_data = backlink + get_file_name(dir_data);

  struct stat64 info;
  if (lstat64(lnk_path_data.c_str(), &info) != 0)  {
    if (symlink(backlink_data.c_str(), lnk_path_data.c_str()) != 0) {
      cerr << "Warning: cannot create catalog store -> data store symlink" << endl;
      if (exit_on_error) return false;
    }
  }

  // Save current checksum if available
  map<char, string> ext_chksum;
  hash::t_sha1 current_snapshot;
  if (parse_keyval(dir_catalogs + path + "/.cvmfspublished", ext_chksum)) {
    current_snapshot.from_hash_str(ext_chksum['C']);
    cout << "Found local catalog with snapshot id " << ext_chksum['C'] << endl;
  }

  string tmp_path;
  unsigned again = 0;
  int result;
  do {
    result = fetch_catalog(path, false, md5, tmp_path);
    if (result == 0) {
      cout << "Catalog up to date" << endl;
      return true;
    }
    again++;
  } while ((result == -1)  && (again < retries));
  overall_retries += again-1;
  if (result != 1)
    return false;

  // New catalog, go on
  if (!catalog::attach(tmp_path, "", true, false)) {
    cerr << "Warning: failed to attach " << tmp_path;
    return false;
  }
  unlink(tmp_path.c_str());

  // Recursive listing
  uint64_t no_entries = 0;
  uint64_t no_regular = 0;
  uint64_t no_prels = 0;
  uint64_t no_downloads = 0;
  cout << "Pulling data chunks: " << flush;
  recursive_ls(md5, path, no_entries, no_regular, no_prels, no_downloads);
  if (!download_bucket.empty())
    no_downloads += download_bunch();
  cout << " fetched " << no_downloads <<  " new files and " << no_prels << " mucro catalogs out of "
  << no_regular << " regular files out of " << no_entries << " catalog entries" << endl;

  // Store nested catalogs
  vector<string> nested;
  if (!catalog::ls_nested(catalog::get_num_catalogs()-1, nested)) {
    cerr << "Warning: failed to get nested catalogs" << endl;
    if (exit_on_error) return false;
  }

  // Walk through the linked list of previous revisions, best effort
  if (!initial && (current_snapshot != hash::t_sha1())) {
    hash::t_sha1 previous;
    do {
      previous = catalog::get_previous_revision(catalog::get_num_catalogs()-1);
      cout << "Previous catalog snapshot id " << previous.to_string() << endl;
      if ((previous != hash::t_sha1()) && (previous != current_snapshot)) {
        // Pull previous catalog
        cout << "Pulling previous catalog..." << endl;
        unsigned again = 0;
        bool result;
        string prev_clg_path;
        do {
          result = fetch_deprecated_catalog(previous, prev_clg_path);
          again++;
        } while (!result && (again < retries));
        overall_retries += again-1;
        if (!result) {
          break;
        }

        // Attach / detach
        catalog::detach(catalog::get_num_catalogs()-1);
        if (!catalog::attach(prev_clg_path, "", true, false)) {
          cerr << "Warning: failed to attach " << prev_clg_path;
          break;
        }
        unlink(tmp_path.c_str());

        // Recursive ls
        uint64_t no_entries = 0;
        uint64_t no_regular = 0;
        uint64_t no_prels = 0;
        uint64_t no_downloads = 0;
        cout << "Pulling data chunks: " << flush;
        recursive_ls(md5, path, no_entries, no_regular, no_prels, no_downloads, true);
        if (!download_bucket.empty())
          no_downloads += download_bunch(true);
        cout << " fetched " << no_downloads <<  " new files and " << no_prels << " mucro catalogs out of "
        << no_regular << " regular files out of " << no_entries << " catalog entries" << endl;
      }
    } while ((previous != hash::t_sha1()) && (previous != current_snapshot));
  }
  catalog::detach(catalog::get_num_catalogs()-1);

  // Nested catalogs
  for (unsigned i = 0; i < nested.size(); ++i) {
    if (!recursive_pull(nested[i]) && exit_on_error)
      return false;
  }

  return true;
}

*/

/*static void Usage() {
  LogCvmfs(kLogCvmfs, kLogStdout,
           "CernVM-FS repository replication tool, version %s\n\n"
           "Usage: cvmfs_pull -u <repository url> [-t timeout]\n"
           "         [-r retries] [-k <CernVM public key>]\n"
           "         [-b <certificate blacklist>] [-n <parallel downloads>]\n"
           "         [-m <repository name>] [-s <snapshot name>] [-i(inital)]\n"
           "         [-e(xit on errors)] [-o <difference set output>]\n"
           "         [-q <fault chunks output>] [-w(rite only difference set)]\n"
           "         [-l(ocal spooler)] <pub_dir>\n",
           VERSION);
}*/


int swissknife::CommandPull::Main(const swissknife::ArgumentList &args) {
  int retval;
  unsigned timeout = 10;
  manifest::Manifest *manifest = NULL;
  upload::Spooler *spooler = NULL;

  url = args.find('u')->second;
  temp_dir = args.find('x')->second;
  spooler = upload::MakeSpoolerEnsemble(*args.find('r')->second);
  assert(spooler);
  const string master_keys = *args.find('k')->second;
  const string repository_name = *args.find('m')->second;
  if (args.find('n') != args.end())
    num_parallel = String2Uint64(*args.find('n')->second);
  if (args.find('t') != args.end())
    timeout = String2Uint64(*args.find('t')->second);

  LogCvmfs(kLogCvmfs, kLogStdout, "CernVM-FS: replicating from %s",
           url->c_str());

  int result = 1;
  download::Init(num_parallel);
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

  unsigned char *cert_buf;
  unsigned char *whitelist_buf;
  unsigned cert_size;
  unsigned whitelist_size;
  retval = manifest::Fetch(*url, repository_name, 0, &manifest,
                           &cert_buf, &cert_size,
                           &whitelist_buf, &whitelist_size);
  if (retval != manifest::kFailOk) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to fetch manifest (%d)", retval);
    goto fini;
  }

  result = 0;


  // Check if we have a replica-ready server
  //if (!override_master_test && (curl_download_path("/.cvmfs_master_replica", "/dev/null", dummy.digest, 1, 0) != CURLE_OK)) {
  //  cerr << "This is not a CernVM-FS server for replication, try http://cernvm-bkp.cern.ch" << endl;
  //  goto pull_cleanup;
  //}

 fini:
  delete spooler;
  delete manifest;
  signature::Fini();
  download::Fini();
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
