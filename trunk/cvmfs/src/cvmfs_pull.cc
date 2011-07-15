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

#include "config.h"

#include "util.h"
#include "catalog.h"
#include "signature.h"
#include "hash.h"

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

extern "C" {
   #include "http_curl.h"
   #include "compression.h"
}


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
   cout << "(use http://cernvm-bkp.cern.ch) " << endl
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
   int curl_result;
   struct mem_url mem_url_wl;
   mem_url_wl.data = NULL;
   if (nocache) curl_result = curl_download_mem_nocache("/.cvmfswhitelist", &mem_url_wl, 1, 0); /* TODO */
   else curl_result = curl_download_mem("/.cvmfswhitelist", &mem_url_wl, 1, 0);
   if ((curl_result != CURLE_OK) || !mem_url_wl.data) {
      cerr << "Warning: whitelist could not be loaded" << endl;
      return false;
   } 
   const string buffer = string(mem_url_wl.data, mem_url_wl.size);
   free(mem_url_wl.data);
   
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
      if (!write_memchunk(save_as, &(buffer[0]), buffer.length())) {
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
   const string lpath_compat_chksum = dir_catalogs + path + "/.cvmfschecksum";
   const string rpath_chksum = path + "/.cvmfspublished";
   const string rpath_compat_chksum = path + "/.cvmfschecksum";
   bool have_cached = false;
   hash::t_sha1 sha1_download;
   hash::t_sha1 sha1_local;
   hash::t_sha1 sha1_chksum; /* required for signature verification */
   struct mem_url mem_url_chksum;
   struct mem_url mem_url_compat_chksum;
   struct mem_url mem_url_cert;
   map<char, string> chksum_keyval_local;
   map<char, string> chksum_keyval_remote;
   int curl_result;
   int64_t local_modified = 0;
   char *checksum;
   char *compat_checksum = NULL;
   
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
   if (no_proxy) curl_result = curl_download_mem_nocache(rpath_chksum.c_str(), &mem_url_chksum, 1, 0);
   else curl_result = curl_download_mem(rpath_chksum.c_str(), &mem_url_chksum, 1, 0);
   if (curl_result != CURLE_OK) {
      cerr << "Warning: failed to load checksum" << endl;
      return -1;
   }
   checksum = (char *)alloca(mem_url_chksum.size);
   memcpy(checksum, mem_url_chksum.data, mem_url_chksum.size);
   free(mem_url_chksum.data);
   
   /* load compat checksum */
   if (no_proxy) curl_result = curl_download_mem_nocache(rpath_compat_chksum.c_str(), &mem_url_compat_chksum, 1, 0);
   else curl_result = curl_download_mem(rpath_compat_chksum.c_str(), &mem_url_compat_chksum, 1, 0);
   if (curl_result != CURLE_OK) {
      cerr << "Warning: failed to load compat checksum" << endl;
   } else {
      compat_checksum = (char *)alloca(mem_url_compat_chksum.size);
      memcpy(compat_checksum, mem_url_compat_chksum.data, mem_url_compat_chksum.size);
      free(mem_url_compat_chksum.data);
   }
   
   /* parse remote checksum */
   int sig_start;
   parse_keyval(checksum, mem_url_chksum.size, sig_start, sha1_chksum, chksum_keyval_remote);
   
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
       read_sig_tail(checksum, mem_url_chksum.size, sig_start, 
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
      if (curl_download_mem(url_cert.c_str(), &mem_url_cert, 1, 1) != CURLE_OK) {
         cerr << "Warning: failed to load certificate from " << url_cert 
              << "(" << curl_result << ")" << endl;
         if (mem_url_cert.size > 0) free(mem_url_cert.data); 
         return -1;
      }
      
      /* verify downloaded chunk */
      void *outbuf;
      size_t outsize;
      hash::t_sha1 verify_sha1;
      bool verify_result;
      if (compress_mem(mem_url_cert.data, mem_url_cert.size, &outbuf, &outsize) != 0) {
         verify_result = false;
      } else {
         sha1_mem(outbuf, outsize, verify_sha1.digest);
         free(outbuf);
         verify_result = (verify_sha1 == cert_sha1);
      }
      if (!verify_result) {
         cerr << "Warning: certificate invalid" << endl;
         free(mem_url_cert.data);
         return -1;
      }
      
      /* read certificate */
      if (!signature::load_certificate(mem_url_cert.data, mem_url_cert.size, false)) {
         cerr << "Warning: failed to read certificate" << endl;
         free(mem_url_cert.data);
         return -1;
      }
      
      /* verify certificate and signature */
      if (!valid_certificate(dir_catalogs + path + "/.cvmfswhitelist", no_proxy) ||
          !signature::verify(&((sha1_chksum.to_string())[0]), 40, sig_buf, sig_buf_size)) 
      {
         cerr << "Warning: signature verification failed against " << sha1_chksum.to_string() << endl;
         free(mem_url_cert.data);
         return -1;
      }
      
      /* write certificate */
      if (!file_exists(dir_catalogs + url_cert)) {
         cout << "Writing certificate to " << dir_catalogs + url_cert << endl;
         void *out_buf = NULL;
         size_t out_size;
         if ((compress_mem(mem_url_cert.data, mem_url_cert.size, &out_buf, &out_size) != 0) ||
             !write_memchunk(dir_catalogs + url_cert, out_buf, out_size))
         {
            cerr << "Failed to write certificate" << endl;
         }
         if (out_buf) free(out_buf);
      }
      free(mem_url_cert.data);
      
      /* compat certificate */
      const string compat_cert = dir_catalogs + path + "/.cvmfspublisher.x509";
      cout << "Writing compat certificate to " << compat_cert << endl;
      unlink(compat_cert.c_str());
      if (symlink(url_cert.substr(1).c_str(), compat_cert.c_str()) != 0) {
         cerr << "Warning: failed to create " << compat_cert << endl;
      }
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
   if ((curl_download_stream(url_clg.c_str(), tmp_fp, sha1_local.digest, 1, 1) != CURLE_OK) ||
       (sha1_local != sha1_download)) 
   {
      cerr << "Warning: failed to load catalog from " << url_clg << endl;
      fclose(tmp_fp);
      return -1;
   }
   fclose(tmp_fp);
   if (sha1_local != sha1_download) {
      cerr << "Warning: invalid catalog from " << url_clg << endl;
      unlink(tmp_file);
      return -1;
   }
   
   /* we have all bits and pieces, write checksum */ 
   cout << "Writing checksum to " << lpath_chksum << endl;
   if (!write_memchunk(lpath_chksum, checksum, mem_url_chksum.size)) {
      cerr << "Warning: failed to write " << lpath_chksum << endl;
      return -1;
   }
   if (compat_checksum) {
      cout << "Writing compat checksum to " << lpath_compat_chksum << endl;
      if (!write_memchunk(lpath_compat_chksum, compat_checksum, mem_url_compat_chksum.size)) {
         cerr << "Warning: failed to write " << lpath_compat_chksum << endl;
         return -1;
      }
   }

   /* write catalog */
   const string sha1_str = sha1_download.to_string();
   const string clg_data_path = dir_catalogs + "/data/" + sha1_str.substr(0, 2) + "/" +
                                sha1_str.substr(2) + "C";
   if (!file_exists(clg_data_path)) {
      cout << "Writing catalog to " << clg_data_path << endl;
      if (compress_file(tmp_file, clg_data_path.c_str()) != 0) {
         cerr << "Warning: failed to store catalog" << endl;
         return -1;
      }
   }
   
   /* compat catalog */
   const string compat_catalog = dir_catalogs + path + "/.cvmfscatalog";
   const string compat_catalog_link = "data/" + sha1_str.substr(0, 2) + "/" +
                                      sha1_str.substr(2) + "C";
   cout << "Writing compat catalog to " << compat_catalog << endl;
   unlink(compat_catalog.c_str());
   if (symlink(compat_catalog_link.c_str(), compat_catalog.c_str()) != 0) {
      cerr << "Warning: failed to create " << compat_catalog << endl;
      return -1;
   }

   return 1;
}



static bool fetch_deprecated_catalog(const hash::t_sha1 &snapshot_id, string &tmp_path) {
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
   
   /* write catalog */
   const string clg_data_path = dir_catalogs + "/data/" + sha1_clg_str.substr(0, 2) + "/" +
                                sha1_clg_str.substr(2) + "C";
   if (!file_exists(clg_data_path)) {
      cout << "Writing catalog to " << clg_data_path << endl;
      if (compress_file(tmp_path.c_str(), clg_data_path.c_str()) != 0) {
         cerr << "Warning: failed to store catalog" << endl;
      }
   }
   
   return true;
}


/**
 * Returns number of successful downloads
 */
static int download_bunch(const bool ignore_errors = false) {
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
   
   curl_set_host_chain(url.c_str());
   cout << "CernVM-FS Pull: synchronizing with " << url << endl;
   if (proxies != "")
      curl_set_proxy_chain(proxies.c_str());
   curl_set_timeout(timeout, timeout);
   
   curl_ready = true;
   
   signature::init();
   if (!signature::load_public_key(pubkey)) {
      cout << "Warning: cvmfs public master key could not be loaded." << endl;
      goto pull_cleanup;
   } else {
      cout << "CernVM-FS Pull: using public key " << pubkey << endl;
   }
   signature_ready = true;
   
   if (!catalog::init(getuid(), getgid())) {
      cerr << "Failed to initialize catalog" << endl;
      goto pull_cleanup;
   }
   catalog_ready = true;
   
   /* Check if we have a replica-ready server */
   if (!override_master_test && (curl_download_path("/.cvmfs_master_replica", "/dev/null", dummy.digest, 1, 0) != CURLE_OK)) {
      cerr << "This is not a CernVM-FS server for replication, try http://cernvm-bkp.cern.ch" << endl;
      goto pull_cleanup;
   }
   
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
   if (ffaulty.is_open()) ffaulty.close();
   if (fdiff.is_open()) fdiff.close();
   return result;
}
