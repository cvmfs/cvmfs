/**
 * \file cvmfs_pull.cc
 *
 * This tool synchronizes a local cvmfs-pub directory for a
 * web server with a master replica.
 *
 * Developed by Jakob Blomer 2010 at CERN
 * jakob.blomer@cern.ch
 */


#define _FILE_OFFSET_BITS 64

#include "cvmfs_config.h"

#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include <vector>
#include <set>
#include <map>
#include <cstdlib>
#include <cstring>

#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#include "platform.h"
#include "util.h"
#include "catalog.h"
#include "signature.h"
#include "hash.h"
#include "download.h"
#include "compression.h"


using namespace std;

string dir_target = "";
string dir_data = "";
string dir_catalogs = "";
string url = "";
string blacklist = "/etc/cvmfs/blacklist";
string pubkey = "/etc/cvmfs/keys/cern.ch.pub";
string repo_name = "";
bool dont_load_chunks = false;
unsigned num_parallel = 1;
unsigned retries = 2;
unsigned overall_retries = 0;
bool initial = false;
bool exit_on_error = false;
set<string> faulty_chunks;
set<string> download_bucket;
set<string> diff_set;
bool keep_log = false;

static void usage() {
  cout << "CernVM-FS pull repository from master replica to local webserver" << endl;
  cout << "Usage: cvmfs_pull -d <pub dir> -u <repository url> [-t timeout] [-r retries]" << endl;
  cout << "                  [-k <CernVM public key>] [-b <certificate blacklist>] [-n <parallel downloads>]" << endl;
  cout << "                  [-m <repository name>] [-s <snapshot name>] [-i(inital)] [-e(xit on errors)]" << endl;
  cout << "                  [-o <difference set output>] [-q <fault chunks output>] [-w(rite only difference set)]" << endl;
  cout << "(use http://cvmfs-stratum-zero.cern.ch) " << endl
  << endl;
}

/**
 * Checks, if the SHA1 checksum of a PEM certificate is listed on the
 * whitelist at URL cvmfs::cert_whitelist.
 * With nocache, whitelist is downloaded with pragma:no-cache
 */
static bool valid_certificate(const string &save_as, const bool nocache) {
  const string fingerprint = signature::fingerprint();
  if (fingerprint == "") {
    cerr << "Warning: invalid catalog signature" << endl;
    return false;
  }

  /* download whitelist */
  const string url_whitelist = "/.cvmfswhitelist";
  download::JobInfo download_whitelist(&url_whitelist, false, true, NULL);
  if ((download::Fetch(&download_whitelist) != download::kFailOk) ||
      !download_whitelist.destination_mem.data)
  {
    cerr << "Warning: whitelist could not be loaded" << endl;
    return false;
  }
  const string buffer = string(download_whitelist.destination_mem.data,
                               download_whitelist.destination_mem.size);
  free(download_whitelist.destination_mem.data);

  /* parse whitelist */
  unsigned skip = 0;
  istringstream stream;
  stream.str(buffer);
  string line;

  /* check timestamp (UTC) */
  if (!getline(stream, line) || (line.length() != 14)) {
    cerr << "Warning: invalid timestamp format" << endl;
    return false;
  }
  skip += 15;
  /* Ignore issue date (legacy) */

  /* Now expiry date */
  if (!getline(stream, line) || (line.length() != 15)) {
    cerr << "Warning: invalid timestamp format" << endl;
    return false;
  }
  skip += 16;
  struct tm tm_wl;
  memset(&tm_wl, 0, sizeof(struct tm));
  tm_wl.tm_year = atoi(line.substr(1, 4).c_str())-1900;
  tm_wl.tm_mon = atoi(line.substr(5, 2).c_str()) - 1;
  tm_wl.tm_mday = atoi(line.substr(7, 2).c_str());
  tm_wl.tm_hour = atoi(line.substr(9, 2).c_str());
  tm_wl.tm_min = 0; /* exact on hours level */
  tm_wl.tm_sec = 0;
  time_t timestamp = timegm(&tm_wl);
  if (timestamp < 0) {
    cerr << "Warning: invalid timestamp" << endl;
    return false;
  }
  time_t local_timestamp = time(NULL);
  if (local_timestamp > timestamp) {
    cerr << "Warning: whitelist lifetime verification failed, expired" << endl;
    return false;
  }

  /* Check repository name */
  if (!getline(stream, line)) {
    cerr << "failed to get repository name" << endl;
    return false;
  }
  skip += line.length() + 1;
  if ((repo_name != "") && ("N" + repo_name != line)) {
    cerr << "Warning: repository name does not match (found " << line <<
    ", expected " << repo_name << ")" << endl;
    return false;
  }

  /* search the fingerprint */
  bool found = false;
  while (getline(stream, line)) {
    skip += line.length() + 1;
    if (line == "--") break;
    if (line.substr(0, 59) == fingerprint)
      found = true;
  }
  if (!found) {
    cerr << "Warning: the certificate's fingerprint is not on the whitelist" << endl;
    return false;
  }

  /* check whitelist signature */
  if (!getline(stream, line) || (line.length() < 40)) {
    cerr << "Warning: no checksum at the end of whitelist found" << endl;
    return false;
  }
  hash::t_sha1 sha1;
  sha1.from_hash_str(line.substr(0, 40));
  if (sha1 != hash::t_sha1(buffer.substr(0, skip-3))) {
    cerr << "Warning: whitelist checksum does not match" << endl;
    return false;
  }

  /* check local blacklist */
  ifstream fblacklist;
  fblacklist.open(blacklist.c_str());
  if (fblacklist) {
    string blackline;
    while (getline(fblacklist, blackline)) {
      if (blackline.substr(0, 59) == fingerprint) {
        cerr << "Warning: fingerprint " << fingerprint << " is blacklisted" << endl;
        fblacklist.close();
        return false;
      }
    }
    fblacklist.close();
  }

  void *sig_buf;
  unsigned sig_buf_size;
  if (!read_sig_tail(&buffer[0], buffer.length(), skip,
                     &sig_buf, &sig_buf_size))
  {
    cerr << "Warning: no signature at the end of whitelist found" << endl;
    return false;
  }
  const string sha1str = sha1.to_string();
  bool result = signature::verify_rsa(&sha1str[0], 40, sig_buf, sig_buf_size);
  free(sig_buf);
  if (!result) {
    cerr << "Warning: whitelist signature verification failed, "
    << signature::get_crypto_err() << endl;
  } else {
    cout << "Writing whitelist to " << save_as << endl;
    if (!CopyMem2Path(&(buffer[0]), buffer.length(), save_as)) {
      cerr << "Warning: failed to write " << save_as << endl;
    }
  }
  return result;
}


/**
 * Loads a catalog from an url into local cache if there is a newer version.
 * Catalogs are stored like data chunks.
 * This funktions returns a temporary uncrompressed fil.
 *
 * We first download the checksum of the catalog to quickly see if anyting changed.
 *
 * The checksum has to be signed by an X.509 certificate.  We only proceed
 * with a valid signature and a valid certificate.
 *
 * \return -1 on error, 0 if cache is up to date, 1 if a new catalog was loaded
 */
static int fetch_catalog(const string &path, const bool no_proxy,
                         const hash::t_md5 &mount_point, string &tmp_path)
{
  const string lpath_chksum = dir_catalogs + path + "/.cvmfspublished";
  const string rpath_chksum = path + "/.cvmfspublished";
  bool have_cached = false;
  hash::t_sha1 sha1_download;
  hash::t_sha1 sha1_local;
  hash::t_sha1 sha1_chksum; /* required for signature verification */
  map<char, string> chksum_keyval_local;
  map<char, string> chksum_keyval_remote;
  int64_t local_modified = 0;
  char *checksum;

  /* load local checksum */
  if (parse_keyval(lpath_chksum, chksum_keyval_local)) {
    sha1_local.from_hash_str(chksum_keyval_local['C']);
    have_cached = true;

    /* try to get local last modified time */
    map<char, string>::const_iterator published = chksum_keyval_local.find('T');
    if (published != chksum_keyval_local.end()) {
      local_modified = atoll(published->second.c_str());
    }
  }

  /* load remote checksum */
  download::JobInfo download_checksum(&rpath_chksum, false, true, NULL);
  if (download::Fetch(&download_checksum) != download::kFailOk) {
    cerr << "Warning: failed to load checksum" << endl;
    return -1;
  }
  checksum = (char *)alloca(download_checksum.destination_mem.size);
  memcpy(checksum, download_checksum.destination_mem.data,
         download_checksum.destination_mem.size);
  free(download_checksum.destination_mem.data);

  /* parse remote checksum */
  int sig_start;
  parse_keyval(checksum, download_checksum.destination_mem.size, sig_start, sha1_chksum, chksum_keyval_remote);

  map<char, string>::const_iterator clg_key = chksum_keyval_remote.find('C');
  if (clg_key == chksum_keyval_remote.end()) {
    cerr << "Warning: failed to find catalog key in checksum" << endl;
    return -1;
  }
  sha1_download.from_hash_str(clg_key->second);


  /* Sanity check: repository name */
  if (repo_name != "") {
    map<char, string>::const_iterator name = chksum_keyval_remote.find('N');
    if (name == chksum_keyval_remote.end()) {
      cerr << "Warning: failed to find repository name in checksum" << endl;
      return -1;
    }
    if (name->second != repo_name) {
      cerr << "Warning: expected repository name does not match" << endl;
      return -1;
    }
  }

  /* Sanity check: root prefix */
  map<char, string>::const_iterator root_prefix = chksum_keyval_remote.find('R');
  if (root_prefix == chksum_keyval_remote.end()) {
    cerr << "Warning: failed to find root prefix in checksum" << endl;
    return -1;
  }
  if (root_prefix->second != mount_point.to_string()) {
    cerr << "Warning: expected mount point does not match" << endl;
    return -1;
  }

  /* verify remote checksum signature, failure is handled like checksum could not be downloaded. */
  void *sig_buf_heap;
  unsigned sig_buf_size;
  if ((sig_start > 0) &&
      read_sig_tail(checksum, download_checksum.destination_mem.size, sig_start,
                    &sig_buf_heap, &sig_buf_size))
  {
    void *sig_buf = alloca(sig_buf_size);
    memcpy(sig_buf, sig_buf_heap, sig_buf_size);
    free(sig_buf_heap);

    /* retrieve certificate */
    map<char, string>::const_iterator key_cert = chksum_keyval_remote.find('X');
    if ((key_cert == chksum_keyval_remote.end()) || (key_cert->second.length() < 40)) {
      cerr << "Warning: invalid certificate in checksum" << endl;
      return -1;
    }

    hash::t_sha1 cert_sha1;
    cert_sha1.from_hash_str(key_cert->second.substr(0, 40));

    const string url_cert = "/data/" + key_cert->second.substr(0,2) + "/" +
    key_cert->second.substr(2) + "X";
    download::JobInfo download_certificate(&url_cert, true, true, NULL);
    if (download::Fetch(&download_certificate) != download::kFailOk) {
      cerr << "Warning: failed to load certificate from " << url_cert
      << "(" << download_certificate.error_code << ")" << endl;
      if (download_certificate.destination_mem.size > 0) free(download_certificate.destination_mem.data);
      return -1;
    }

    /* verify downloaded chunk */
    void *outbuf;
    int64_t outsize;
    hash::t_sha1 verify_sha1;
    bool verify_result;
    if (!zlib::CompressMem2Mem(download_certificate.destination_mem.data, download_certificate.destination_mem.size, &outbuf, &outsize)) {
      verify_result = false;
    } else {
      hash::sha1_mem(outbuf, outsize, verify_sha1.digest);
      free(outbuf);
      verify_result = (verify_sha1 == cert_sha1);
    }
    if (!verify_result) {
      cerr << "Warning: certificate invalid" << endl;
      free(download_certificate.destination_mem.data);
      return -1;
    }

    /* read certificate */
    if (!signature::load_certificate(download_certificate.destination_mem.data, download_certificate.destination_mem.size, false)) {
      cerr << "Warning: failed to read certificate" << endl;
      free(download_certificate.destination_mem.data);
      return -1;
    }

    /* verify certificate and signature */
    if (!valid_certificate(dir_catalogs + path + "/.cvmfswhitelist", no_proxy) ||
        !signature::verify(&((sha1_chksum.to_string())[0]), 40, sig_buf, sig_buf_size))
    {
      cerr << "Warning: signature verification failed against " << sha1_chksum.to_string() << endl;
      free(download_certificate.destination_mem.data);
      return -1;
    }

    /* write certificate */
    if (!file_exists(dir_catalogs + url_cert)) {
      cout << "Writing certificate to " << dir_catalogs + url_cert << endl;
      void *out_buf = NULL;
      int64_t out_size;
      if (!zlib::CompressMem2Mem(download_certificate.destination_mem.data, download_certificate.destination_mem.size, &out_buf, &out_size) ||
          !CopyMem2Path(out_buf, out_size, dir_catalogs + url_cert))
      {
        cerr << "Failed to write certificate" << endl;
      }
      if (out_buf) free(out_buf);
    }
    free(download_certificate.destination_mem.data);
  } else {
    cerr << "Warning: remote checksum is not signed" << endl;
    return -1;
  }

  /* short way out, use cached copy */
  if (have_cached && !initial) {
    if (sha1_download == sha1_local)
      return 0;

    /* Sanity check, last modified (if available, i.e. if signed) */
    map<char, string>::const_iterator published = chksum_keyval_remote.find('T');
    if (published != chksum_keyval_remote.end()) {
      if (local_modified > atoll(published->second.c_str())) {
        cerr << "Warning: cached checksum newer than loaded checksum" << endl;
        return -1;
      }
    }
  }

  /* load new catalog */
  tmp_path = dir_catalogs + "/cvmfs.catalog.XXXXXX";
  char *tmp_file = strdupa(tmp_path.c_str());
  int tmp_fd = mkstemp(tmp_file);
  if (tmp_fd < 0) return -1;
  if (fchmod(tmp_fd, plain_file_mode) != 0) {
    close(tmp_fd);
    return -1;
  }

  tmp_path = tmp_file;
  FILE *tmp_fp = fdopen(tmp_fd, "w");
  if (!tmp_fp) {
    close(tmp_fd);
    unlink(tmp_file);
    return -1;
  }

  const string sha1_clg_str = sha1_download.to_string();
  const string url_clg = "/data/" + sha1_clg_str.substr(0, 2) + "/" +
  sha1_clg_str.substr(2) + "C";
  download::JobInfo download_catalog(&url_clg, true, true, tmp_fp, &sha1_download);
  download::Fetch(&download_catalog);
  if (download_catalog.error_code != download::kFailOk) {
    cerr << "Warning: failed to load catalog from " << url_clg << endl;
    fclose(tmp_fp);
    return -1;
  }
  fclose(tmp_fp);

  /* we have all bits and pieces, write checksum */
  cout << "Writing checksum to " << lpath_chksum << endl;
  if (!CopyMem2Path(checksum, download_checksum.destination_mem.size, lpath_chksum)) {
    cerr << "Warning: failed to write " << lpath_chksum << endl;
    return -1;
  }

  /* write catalog */
  const string sha1_str = sha1_download.to_string();
  const string clg_data_path = dir_catalogs + "/data/" + sha1_str.substr(0, 2) + "/" +
  sha1_str.substr(2) + "C";
  if (!file_exists(clg_data_path)) {
    cout << "Writing catalog to " << clg_data_path << endl;
    if (!zlib::CompressPath2Path(tmp_path, clg_data_path)) {
      cerr << "Warning: failed to store catalog" << endl;
      return -1;
    }
  }

  return 1;
}



static bool fetch_deprecated_catalog(const hash::t_sha1 &snapshot_id, string &tmp_path) {
  FILE *tmp_fp = CreateTempFile(dir_data + "/txn/cvmfs.catalog", plain_file_mode, "w", &tmp_path);
  if (!tmp_fp)
    return false;

  hash::t_sha1 sha1_rcvd;
  const string sha1_clg_str = snapshot_id.to_string();
  const string url_clg = "/data/" + sha1_clg_str.substr(0, 2) + "/" +
  sha1_clg_str.substr(2) + "C";
  download::JobInfo download_catalog(&url_clg, true, true, tmp_fp, &snapshot_id);
  download::Fetch(&download_catalog);
  if (download_catalog.error_code != download::kFailOk) {
    unlink(tmp_path.c_str());
    fclose(tmp_fp);
    cerr << "Warning: failed to load deprecated catalog from " << url_clg << endl;
    return false;
  }
  fclose(tmp_fp);

  /* write catalog */
  const string clg_data_path = dir_catalogs + "/data/" + sha1_clg_str.substr(0, 2) + "/" +
  sha1_clg_str.substr(2) + "C";
  if (!file_exists(clg_data_path)) {
    cout << "Writing catalog to " << clg_data_path << endl;
    if (!zlib::CompressPath2Path(tmp_path, clg_data_path)) {
      cerr << "Warning: failed to store catalog" << endl;
    }
  }

  return true;
}

/**
 * Returns number of successful downloads
 */
static int download_bunch(const bool ignore_errors = false) {
  vector<string> chunk_urls;
  vector<string> lpaths;
  vector<string> final_paths;
  vector<hash::t_sha1> hashes;

  for (set<string>::const_iterator i = download_bucket.begin(), iEnd = download_bucket.end();
       i != iEnd; ++i)
  {
    const string sha1_str = "/" + i->substr(0, 2) + "/" + i->substr(2);
    chunk_urls.push_back(url + "/data" + sha1_str);
    lpaths.push_back(dir_data + "/txn/" + (*i));
    final_paths.push_back(dir_data + sha1_str);
    hash::t_sha1 expected_sha1;
    expected_sha1.from_hash_str(*i);
    hashes.push_back(expected_sha1);
  }
  int num = download_bucket.size();
  download_bucket.clear();

  int faulty = 0;
#pragma omp parallel for num_threads(num_parallel)
  for (int i = 0; i < num; ++i) {
    download::JobInfo download_chunk(&chunk_urls[i], true, false, &lpaths[i],
                                     &hashes[i]);
    unsigned attempts = 0;
    do {
      download::Fetch(&download_chunk);
      attempts++;
    } while ((download_chunk.error_code != download::kFailOk) &&
             (attempts < retries));

    if (download_chunk.error_code != download::kFailOk) {
#pragma omp critical
      {
        if (!ignore_errors) {
          faulty++;
          faulty_chunks.insert(chunk_urls[i]);
        }
        cerr << "Warning: failed to download " << chunk_urls[i] << endl;
      }
    } else {
      if (rename(lpaths[i].c_str(), final_paths[i].c_str()) != 0) {
#pragma omp critical

        {
          cerr << "Warning: failed to commit " << chunk_urls[i] << endl;
          faulty++;
          faulty_chunks.insert(chunk_urls[i]);
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

  platform_stat64 info;
  if (platform_lstat(lnk_path_data.c_str(), &info) != 0)  {
    if (symlink(backlink_data.c_str(), lnk_path_data.c_str()) != 0) {
      cerr << "Warning: cannot create catalog store -> data store symlink" << endl;
      if (exit_on_error) return false;
    }
  }

  /* Save current checksum if available */
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

  /* New catalog, go on */
  if (!catalog::attach(tmp_path, "", true, false)) {
    cerr << "Warning: failed to attach " << tmp_path;
    return false;
  }
  unlink(tmp_path.c_str());

  /* Recursive listing */
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

  /* Store nested catalogs */
  vector<string> nested;
  if (!catalog::ls_nested(catalog::get_num_catalogs()-1, nested)) {
    cerr << "Warning: failed to get nested catalogs" << endl;
    if (exit_on_error) return false;
  }

  /* Walk through the linked list of previous revisions, best effort */
  if (!initial && (current_snapshot != hash::t_sha1())) {
    hash::t_sha1 previous;
    do {
      previous = catalog::get_previous_revision(catalog::get_num_catalogs()-1);
      cout << "Previous catalog snapshot id " << previous.to_string() << endl;
      if ((previous != hash::t_sha1()) && (previous != current_snapshot)) {
        /* Pull previous catalog */
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

        /* Attach / detach */
        catalog::detach(catalog::get_num_catalogs()-1);
        if (!catalog::attach(prev_clg_path, "", true, false)) {
          cerr << "Warning: failed to attach " << prev_clg_path;
          break;
        }
        unlink(tmp_path.c_str());

        /* Recursive ls */
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

  /* Nested catalogs */
  for (unsigned i = 0; i < nested.size(); ++i) {
    if (!recursive_pull(nested[i]) && exit_on_error)
      return false;
  }

  return true;
}


int main(int argc, char **argv) {
  if ((argc < 2) || (string(argv[1]) == "-h") || (string(argv[1]) == "--help") ||
      (string(argv[1]) == "-v") || (string(argv[1]) == "--version"))
  {
    usage();
    return 0;
  }

  umask(022);

  int timeout = 10;

  bool download_ready = false;
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
      case 'u': {
        url = optarg;
        break;
      }
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

  /* Sanity checks */
  if ((dir_target == "") || (url == "")) {
    usage();
    return 1;
  }
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

  download::Init(num_parallel);
  download::SetHostChain(url);
  cout << "CernVM-FS Pull: synchronizing with " << url << endl;
  if (proxies != "")
    download::SetProxyChain(proxies);
  download::SetTimeout(timeout, timeout);
  download::Spawn();
  download_ready = true;

  /* Check if we have a replica-ready server */
  string url_master_replica("/.cvmfs_master_replica");
  download::JobInfo download_master_replica(&url_master_replica, false, true, NULL);
  if (!override_master_test && (download::Fetch(&download_master_replica) != download::kFailOk)) {
    cerr << "This is not a CernVM-FS server for replication, try http://cvmfs-stratum-zero.cern.ch" << endl;
    goto pull_cleanup;
  }
  free(download_master_replica.destination_mem.data);

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

  /* Check if we have a valid starting point */
  if (!initial && exit_on_error && !file_exists(dir_catalogs + "/.cvmfs_replica")) {
    cerr << "This is not a consistent repository replica to start with" << endl;
    goto pull_cleanup;
  }

  /* start work */
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
  if (download_ready) download::Fini();
  if (ffaulty.is_open()) ffaulty.close();
  if (fdiff.is_open()) fdiff.close();
  return result;
}
