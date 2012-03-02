#include "RemoteCatalogManager.h"

#include <errno.h>
#include <fstream>

#include "platform.h"
#include "cache.h"
#include "signature.h"
#include "download.h"
#include "compression.h"
#include "logging.h"
#include "hash.h"

using namespace std;

namespace cvmfs {

  RemoteCatalogManager::RemoteCatalogManager(const string &root_url, const string &repo_name, const string &whitelist,
                                             const string &blacklist, const bool force_signing)
  {
    LogCvmfs(kLogCatalog, kLogDebug, "constructing remote catalog mgr");
    root_url_ = root_url;
    repo_name_ = repo_name;
    whitelist_ = whitelist;
    blacklist_ = blacklist;
    force_signing_ = force_signing;

    atomic_init32(&certificate_hits_);
    atomic_init32(&certificate_misses_);
  }

  RemoteCatalogManager::~RemoteCatalogManager() {
  }

  Catalog* RemoteCatalogManager::CreateCatalogStub(const std::string &mountpoint, Catalog *parent_catalog) const {
    return new Catalog(mountpoint, parent_catalog);
  }

  int RemoteCatalogManager::LoadCatalogFile(const string &url_path, const hash::Md5 &mount_point,
                                            const int existing_cat_id, const bool no_cache,
                                            const hash::Any &expected_clg, std::string *catalog_file)
  {

    // TODO: there is a lot of clutter in here which should be done by a dedicated
    //       FileManager class...

    string old_file;
    hash::Any sha1_old(hash::kSha1);
    hash::Any sha1_cat(hash::kSha1);
    bool cached_copy;

    int result = FetchCatalog(url_path, no_cache, mount_point,
                              *catalog_file, sha1_cat, old_file, sha1_old, cached_copy, expected_clg);
    if (((result == -EPERM) || (result == -EAGAIN) || (result == -EINVAL)) && !no_cache) {
      /* retry with no-cache pragma */
      LogCvmfs(kLogCvmfs, kLogSyslog | kLogDebug,
               "possible data corruption while trying to retrieve catalog "
               "from %s, trying with no-cache", (root_url_ + url_path).c_str());
      result = FetchCatalog(url_path, true, mount_point,
                            *catalog_file, sha1_cat, old_file, sha1_old, cached_copy, expected_clg);
    }
    /* log certain failures */
    if (result == -EPERM) {
      LogCvmfs(kLogCvmfs, kLogSyslog,
               "signature verification failure while trying to retrieve "
               "catalog from %s", (root_url_ + url_path).c_str());
    }
    else if ((result == -EINVAL) || (result == -EAGAIN)) {
      LogCvmfs(kLogCvmfs, kLogSyslog,
               "data corruption while trying to retrieve catalog from %s",
               (root_url_ + url_path).c_str());
    }
    else if (result < 0) {
      LogCvmfs(kLogCvmfs, kLogSyslog,
               "catalog load failure while try to retrieve catalog from %s",
               (root_url_ + url_path).c_str());
    }

    /* LRU handling, could still fail due to cache size restrictions */
    if (((result == 0) && !cached_copy) ||
        ((existing_cat_id < 0) && ((result == 0) || cached_copy)))
    {
      platform_stat64 info;
      if (platform_stat(catalog_file->c_str(), &info) != 0) {
        /* should never happen */
        lru::Remove(sha1_cat);
        cached_copy = false;
        result = -EIO;
        LogCvmfs(kLogCvmfs, kLogSyslog | kLogDebug,
                 "catalog access failure for %s", catalog_file->c_str());
      } else {
        if (((uint64_t)info.st_size > lru::GetMaxFileSize()) ||
            (!lru::Pin(sha1_cat, info.st_size, root_url_ + url_path)))
        {
          LogCvmfs(kLogCvmfs, kLogSyslog | kLogDebug,
                   "catalog load failure for %s (no space)",
                   catalog_file->c_str());
          lru::Remove(sha1_cat);
          unlink(catalog_file->c_str());
          cached_copy = false;
          result = -ENOSPC;
        } else {
          /* From now on we have to go with the new catalog */
          if (!sha1_old.IsNull() && (sha1_old != sha1_cat)) {
            lru::Remove(sha1_old);
            unlink(old_file.c_str());
          }
        }
      }
    }

    // rename the loaded catalog file
    if ((result == 0) || cached_copy) {
      const string sha1_cat_str = sha1_cat.ToString();
      const string final_file = "./" + sha1_cat_str.substr(0, 2) + "/" +
      sha1_cat_str.substr(2);
      (void)rename(catalog_file->c_str(), final_file.c_str());
      *catalog_file = final_file;
    }

    return result;
  }

  string RemoteCatalogManager::MakeFilesystemKey(string url) const {
    string::size_type pos;
    while ((pos = url.find(':', 0)) != string::npos) {
      url[pos] = '-';
    }
    while ((pos = url.find('/', 0)) != string::npos){
      url[pos] = '-';
    }
    return url;
  }


  // TODO: code from here on DOES NOT belong here
  //       should be hidden in a FileManager class or something
  //       currently this is here for convenience!!
  //       see: https://cernvm.cern.ch/project/trac/cernvm/wiki/private/evolving-cvmfs


  /**
   * Loads a catalog from an url into local cache if there is a newer version.
   * Catalogs are stored like data chunks.
   * This funktions returns a temporary file that is not tampered with by LRU.
   *
   * We first download the checksum of the catalog to quickly see if anyting changed.
   *
   * The checksum can be signed by an X.509 certificate.  If so, we only load succeed
   * only with a valid signature and a valid certificate.
   *
   * @param[in] url, relative directory path starting from root_url
   * @param[in] no_proxy, if true, fetch checksum and signature/whitelist with pragma: no-cache
   * @param[in] mount_point, expected mount path (required for sanity check)
   * @param[out] cat_file, file name of the catalog cache copy or the new catalog on success.
   * @param[out] cat_sha1, sha1 value of the catalog returned by cat_file.
   * @param[out] old_file, file name of the old catalog cache copy if new catalog is loaded.
   * @param[out] old_sha1, sha1 value of the old catalog cache copy if new catalog is loaded.
   * @param[out] cached_copy, indicates if a new catalog version was loaded.
   * \return 0 on success, a standard error code else
   */
  int RemoteCatalogManager::FetchCatalog(
                                         const string &url_path, const bool no_proxy, const hash::Md5 &mount_point,
                                         string &cat_file, hash::Any &cat_sha1, string &old_file, hash::Any &old_sha1,
                                         bool &cached_copy, const hash::Any &sha1_expected, const bool dry_run) {
    const string fskey = (repo_name_ == "") ? root_url_ : repo_name_;
    const string lpath_chksum = "./cvmfs.checksum." + MakeFilesystemKey(fskey + url_path);
    const string rpath_chksum = url_path + "/.cvmfspublished";
    bool have_cached = false;
    bool signature_ok = false;
    unsigned checksum_size = 0;
    hash::Any sha1_download(hash::kSha1);
    hash::Any sha1_local(hash::kSha1);
    hash::Any sha1_chksum(hash::kSha1); /* required for signature verification */
    map<char, string> chksum_keyval;
    int64_t local_modified;
    char *checksum = NULL;

    LogCvmfs(kLogCvmfs, kLogDebug, "searching for filesystem at %s",
             (root_url_ + url_path).c_str());

    cached_copy = false;
    cat_file = old_file = "";
    old_sha1 = cat_sha1 = hash::Any(hash::kSha1);
    local_modified = 0;

    /* load local checksum */
    LogCvmfs(kLogCvmfs, kLogDebug, "local checksum file is %s",
             lpath_chksum.c_str());
    FILE *fchksum = fopen(lpath_chksum.c_str(), "r");
    char tmp[40];
    if (fchksum && (fread(tmp, 1, 40, fchksum) == 40))
    {
      sha1_local = hash::Any(hash::kSha1, hash::HexPtr(string(tmp, 40)));
      cat_file = "./" + string(tmp, 2) + "/" + string(tmp+2, 38);

      /* try to get local last modified time */
      char buf_modified;
      string str_modified;
      if ((fread(&buf_modified, 1, 1, fchksum) == 1) && (buf_modified == 'T')) {
        while (fread(&buf_modified, 1, 1, fchksum) == 1)
          str_modified += string(&buf_modified, 1);
        local_modified = atoll(str_modified.c_str());
        LogCvmfs(kLogCvmfs, kLogDebug, "cached copy publish date %s",
                 StringifyTime(local_modified, true).c_str());
      }

      /* Sanity check, do we have the catalog? If yes, save it to temporary file. */
      if (!dry_run) {
        if (rename(cat_file.c_str(), (cat_file + "T").c_str()) != 0) {
          cat_file = "";
          unlink(lpath_chksum.c_str());
          LogCvmfs(kLogCvmfs, kLogDebug,
                   "checksum existed but no catalog with it");
        } else {
          cat_file += "T";
          old_file = cat_file;
          cat_sha1 = old_sha1 = sha1_local;
          have_cached = cached_copy = true;
          LogCvmfs(kLogCvmfs, kLogDebug, "local checksum is %s",
                    sha1_local.ToString().c_str());
        }
      } else {
        old_file = cat_file;
        cat_sha1 = old_sha1 = sha1_local;
        have_cached = cached_copy = true;
      }
    } else {
      LogCvmfs(kLogCvmfs, kLogDebug, "unable to read local checksum");
    }
    if (fchksum) fclose(fchksum);

    /* load remote checksum */
    int sig_start = 0;
    if (sha1_expected == hash::Any(hash::kSha1)) {
      download::JobInfo download_checksum(&rpath_chksum, false, true, NULL);
      download::Fetch(&download_checksum);
      if (download_checksum.error_code != download::kFailOk) {
        LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog,
                  "unable to load checksum from %s (%d), going to offline mode",
                 rpath_chksum.c_str(), download_checksum.error_code);
        return -EIO;
      }
      checksum_size = download_checksum.destination_mem.size;

      checksum = (char *)alloca(download_checksum.destination_mem.size);
      memcpy(checksum, download_checksum.destination_mem.data, download_checksum.destination_mem.size);
      free(download_checksum.destination_mem.data);

      /* parse remote checksum */
      ParseKeyvalMem((const unsigned char *)checksum, download_checksum.destination_mem.size, &sig_start, &sha1_chksum, &chksum_keyval);

      map<char, string>::const_iterator clg_key = chksum_keyval.find('C');
      if (clg_key == chksum_keyval.end()) {
        LogCvmfs(kLogCvmfs, kLogDebug,
                 "failed to find catalog key in checksum");
        return -EINVAL;
      }
      sha1_download = hash::Any(hash::kSha1, hash::HexPtr(clg_key->second));
      LogCvmfs(kLogCvmfs, kLogDebug, "remote checksum is %s",
               sha1_download.ToString().c_str());
    } else {
      sha1_download = sha1_expected;
    }

    /* short way out, use cached copy */
    if (have_cached) {
      if (sha1_download == sha1_local)
        return 0;

      /* Sanity check, last modified (if available, i.e. if signed) */
      map<char, string>::const_iterator published = chksum_keyval.find('T');
      if (published != chksum_keyval.end()) {
        if (local_modified > atoll(published->second.c_str())) {
          LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog,
                   "cached copy of %s newer than remote copy",
                   rpath_chksum.c_str());
          return 0;
        }
      }
    }

    if (sha1_expected.IsNull()) {
      /* Sanity check: repository name */
      if (repo_name_ != "") {
        map<char, string>::const_iterator name = chksum_keyval.find('N');
        if (name == chksum_keyval.end()) {
          LogCvmfs(kLogCvmfs, kLogDebug,
                   "failed to find repository name in checksum");
          return -EINVAL;
        }
        if (name->second != repo_name_) {
          LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog,
                   "expected repository name does not match in %s",
                   rpath_chksum.c_str());
          return -EINVAL;
        }
      }


      /* Sanity check: root prefix */
      map<char, string>::const_iterator root_prefix = chksum_keyval.find('R');
      if (root_prefix == chksum_keyval.end()) {
        LogCvmfs(kLogCvmfs, kLogDebug,
                 "failed to find root prefix in checksum");
        return -EINVAL;
      }
      if (root_prefix->second != mount_point.ToString()) {
        LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog,
                 "expected mount point does not match in %s",
                 rpath_chksum.c_str());
        return -EINVAL;
      }

      /* verify remote checksum signature, failure is handled like checksum could not be downloaded,
       except for error code -2 instead of -1. */
      void *sig_buf_heap;
      unsigned sig_buf_size;
      if ((sig_start > 0) &&
          signature::ReadSignatureTail((const unsigned char *)checksum, checksum_size, sig_start,
                                       (unsigned char **)&sig_buf_heap, &sig_buf_size))
      {
        void *sig_buf = alloca(sig_buf_size);
        memcpy(sig_buf, sig_buf_heap, sig_buf_size);
        free(sig_buf_heap);

        /* retrieve certificate */
        map<char, string>::const_iterator key_cert = chksum_keyval.find('X');
        if ((key_cert == chksum_keyval.end()) || (key_cert->second.length() < 40)) {
          LogCvmfs(kLogCvmfs, kLogDebug, "invalid certificate in checksum");
          return -EINVAL;
        }

        bool cached_cert = false;
        hash::Any cert_sha1(hash::kSha1, hash::HexPtr(key_cert->second.substr(0, 40)));

        char *data_certificate;
        size_t size_certificate;
        if (cache::Open2Mem(cert_sha1, &data_certificate, &size_certificate)) {
          atomic_inc32(&certificate_hits_);
          cached_cert = true;
        } else {
          atomic_inc32(&certificate_misses_);
          cached_cert = false;

          const string url_cert = "/data/" + key_cert->second.substr(0, 2) + "/" +
          key_cert->second.substr(2) + "X";
          download::JobInfo download_certificate(&url_cert, true, true, NULL);
          download::Fetch(&download_certificate);
          if (download_certificate.error_code != download::kFailOk) {
            LogCvmfs(kLogCvmfs, kLogDebug,
                     "unable to load certificate from %s (%d)",
                     url_cert.c_str(), download_certificate.error_code);
            return -EAGAIN;
          }

          /* verify downloaded chunk */
          void *outbuf;
          int64_t outsize;
          hash::Any verify_sha1(hash::kSha1);
          bool verify_result;
          if (!zlib::CompressMem2Mem(download_certificate.destination_mem.data,
                                     download_certificate.destination_mem.size,
                                     &outbuf, &outsize))
          {
            verify_result = false;
          } else {
            hash::HashMem((unsigned char *)outbuf, outsize, &verify_sha1);
            free(outbuf);
            verify_result = (verify_sha1 == cert_sha1);
          }
          if (!verify_result) {
            LogCvmfs(kLogCvmfs, kLogDebug, "data corruption for %s",
                     url_cert.c_str());
            free(download_certificate.destination_mem.data);
            return -EAGAIN;
          }
          data_certificate = download_certificate.destination_mem.data;
          size_certificate = download_certificate.destination_mem.size;
        }

        /* read certificate */
        if (!signature::LoadCertificateMem((const unsigned char *)data_certificate, size_certificate)) {
          LogCvmfs(kLogCvmfs, kLogDebug, "could not read certificate");
          free(data_certificate);
          return -EINVAL;
        }

        /* verify certificate and signature */
        if (!IsValidCertificate(no_proxy) ||
            !signature::Verify((const unsigned char *)&((sha1_chksum.ToString())[0]), 40, (const unsigned char *)sig_buf, sig_buf_size))
        {
          LogCvmfs(kLogCvmfs, kLogDebug,
                   "signature verification failed against %s",
                   sha1_chksum.ToString().c_str());
          free(data_certificate);
          return -EPERM;
        }
        LogCvmfs(kLogCvmfs, kLogDebug, "catalog signed by: %s",
                 signature::Whois().c_str());
        signature_ok = true;

        if (!cached_cert) {
          cache::CommitFromMem(cert_sha1, data_certificate, size_certificate,
                               "certificate of " + signature::Whois());
        }
        free(data_certificate);
      } else {
        LogCvmfs(kLogCvmfs, kLogDebug, "remote checksum is not signed");
        if (force_signing_) {
          LogCvmfs(kLogCvmfs, kLogSyslog, "remote checksum %s is not signed",
                   rpath_chksum.c_str());
          return -EPERM;
        }
      }
    }

    if (dry_run) {
      cat_sha1 = sha1_download;
      return 1;
    }

    /* load new catalog */
    const string tmp_file_template = "./cvmfs.catalog.XXXXXX";
    char *tmp_file = strdupa(tmp_file_template.c_str());
    int tmp_fd = mkstemp(tmp_file);
    if (tmp_fd < 0) return -EIO;
    FILE *tmp_fp = fdopen(tmp_fd, "w");
    if (!tmp_fp) {
      close(tmp_fd);
      unlink(tmp_file);
      return -EIO;
    }
    int retval;
    char strmbuf[4096];
    retval = setvbuf(tmp_fp, strmbuf, _IOFBF, 4096);
    assert(retval == 0);

    const string sha1_clg_str = sha1_download.ToString();
    const string url_clg = "/data/" + sha1_clg_str.substr(0, 2) + "/" +
    sha1_clg_str.substr(2) + "C";
    download::JobInfo download_catalog(&url_clg, true, true, tmp_fp, &sha1_download);
    download::Fetch(&download_catalog);
    fclose(tmp_fp);
    if (download_catalog.error_code != download::kFailOk) {
      LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog,
               "unable to load catalog from %s, going to offline mode (%d)",
               url_clg.c_str(), download_catalog.error_code);
      unlink(tmp_file);
      return -EAGAIN;
    }

    /* we have all bits and pieces, write checksum and catalog into cache directory */
    cat_file = tmp_file;
    cat_sha1 = sha1_download;
    cached_copy = false;

    int fdchksum = open(lpath_chksum.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0600);
    if (fdchksum >= 0) {
      string local_chksum = sha1_download.ToString();
      map<char, string>::const_iterator published = chksum_keyval.find('T');
      if (published != chksum_keyval.end())
        local_chksum += "T" + published->second;

      fchksum = fdopen(fdchksum, "w");
      if (fchksum) {
        if (fwrite(&(local_chksum[0]), 1, local_chksum.length(), fchksum) != local_chksum.length())
          unlink(lpath_chksum.c_str());
        fclose(fchksum);
      } else {
        unlink(lpath_chksum.c_str());
      }
    } else {
      unlink(lpath_chksum.c_str());
    }
    if (sha1_expected.IsNull() && signature_ok) {
      LogCvmfs(kLogCvmfs, kLogSyslog,
               "signed catalog loaded from %s, signed by %s",
               (root_url_ + url_path).c_str(), signature::Whois().c_str());
    }
    return 0;
  }

  /**
   * Checks, if the SHA1 checksum of a PEM certificate is listed on the
   * whitelist at URL cvmfs::cert_whitelist.
   * With nocache, whitelist is downloaded with pragma:no-cache
   */
  bool RemoteCatalogManager::IsValidCertificate(bool nocache) {
    const string fingerprint = signature::FingerprintCertificate();
    if (fingerprint == "") {
      LogCvmfs(kLogCvmfs, kLogDebug, "invalid catalog signature");
      return false;
    }
    LogCvmfs(kLogCvmfs, kLogDebug,
             "checking certificate with fingerprint %s against whitelist",
             fingerprint.c_str());

    time_t local_timestamp = time(NULL);
    string buffer;
    istringstream stream;
    string line;
    unsigned skip = 0;

    /* download whitelist */
    download::JobInfo download_whitelist(&whitelist_, false, true, NULL);
    download::Fetch(&download_whitelist);
    if ((download_whitelist.error_code != download::kFailOk) || !download_whitelist.destination_mem.data) {
      LogCvmfs(kLogCvmfs, kLogDebug, "whitelist could not be loaded from %s",
               whitelist_.c_str());
      return false;
    }
    buffer = string(download_whitelist.destination_mem.data, download_whitelist.destination_mem.size);

    /* parse whitelist */
    stream.str(buffer);

    /* check timestamp (UTC) */
    if (!getline(stream, line) || (line.length() != 14)) {
      LogCvmfs(kLogCvmfs, kLogDebug, "invalid timestamp format");
      free(download_whitelist.destination_mem.data);
      return false;
    }
    skip += 15;
    /* Ignore issue date (legacy) */

    /* Now expiry date */
    if (!getline(stream, line) || (line.length() != 15)) {
      LogCvmfs(kLogCvmfs, kLogDebug, "invalid timestamp format");
      free(download_whitelist.destination_mem.data);
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
    LogCvmfs(kLogCvmfs, kLogDebug,
             "whitelist UTC expiry timestamp in localtime: %s",
             StringifyTime(timestamp, false).c_str());
    if (timestamp < 0) {
      LogCvmfs(kLogCvmfs, kLogDebug, "invalid timestamp");
      free(download_whitelist.destination_mem.data);
      return false;
    }
    LogCvmfs(kLogCvmfs, kLogDebug,  "local time: %s",
             StringifyTime(local_timestamp, true).c_str());
    if (local_timestamp > timestamp) {
      LogCvmfs(kLogCvmfs, kLogDebug,
               "whitelist lifetime verification failed, expired");
      free(download_whitelist.destination_mem.data);
      return false;
    }

    /* Check repository name */
    if (!getline(stream, line)) {
      LogCvmfs(kLogCvmfs, kLogDebug, "failed to get repository name");
      free(download_whitelist.destination_mem.data);
      return false;
    }
    skip += line.length() + 1;
    if ((repo_name_ != "") && ("N" + repo_name_ != line)) {
      LogCvmfs(kLogCvmfs, kLogDebug,
               "repository name does not match (found %s, expected %s)",
               line.c_str(), repo_name_.c_str());
      free(download_whitelist.destination_mem.data);
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
      LogCvmfs(kLogCvmfs, kLogDebug,
               "the certificate's fingerprint is not on the whitelist");
      if (download_whitelist.destination_mem.data)
        free(download_whitelist.destination_mem.data);
      return false;
    }

    /* check whitelist signature */
    if (!getline(stream, line) || (line.length() < 40)) {
      LogCvmfs(kLogCvmfs, kLogDebug,
               "no checksum at the end of whitelist found");
      free(download_whitelist.destination_mem.data);
      return false;
    }
    hash::Any sha1(hash::kSha1, hash::HexPtr(line.substr(0, 40)));
    hash::Any compare(hash::kSha1);
    hash::HashMem((const unsigned char *)&buffer[0], skip-3, &compare);
    if (sha1 != compare) {
      LogCvmfs(kLogCvmfs, kLogDebug, "whitelist checksum does not match");
      free(download_whitelist.destination_mem.data);
      return false;
    }

    /* check local blacklist */
    ifstream fblacklist;
    fblacklist.open(blacklist_.c_str());
    if (fblacklist) {
      string blackline;
      while (getline(fblacklist, blackline)) {
        if (blackline.substr(0, 59) == fingerprint) {
          LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog,
                   "blacklisted fingerprint (%s)", fingerprint.c_str());
          fblacklist.close();
          free(download_whitelist.destination_mem.data);
          return false;
        }
      }
      fblacklist.close();
    }

    void *sig_buf;
    unsigned sig_buf_size;
    if (!signature::ReadSignatureTail((const unsigned char *)&buffer[0], buffer.length(), skip,
                                      (unsigned char **)&sig_buf, &sig_buf_size))
    {
      LogCvmfs(kLogCvmfs, kLogDebug,
               "no signature at the end of whitelist found");
      free(download_whitelist.destination_mem.data);
      return false;
    }
    const string sha1str = sha1.ToString();
    bool result = signature::VerifyRsa((const unsigned char *)&sha1str[0], 40,
                                       (const unsigned char *)sig_buf, sig_buf_size);
    free(sig_buf);
    if (!result)
      LogCvmfs(kLogCvmfs, kLogDebug,
               "whitelist signature verification failed, %s",
               signature::GetCryptoError().c_str());
    else
      LogCvmfs(kLogCvmfs, kLogDebug, "whitelist signature verification passed");

    if (result) {
      return true;
    } else {
      free(download_whitelist.destination_mem.data);
      return false;
    }
  }

}
