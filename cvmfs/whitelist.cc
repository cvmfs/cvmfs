/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "whitelist.h"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <ctime>

#include "download.h"
#include "logging.h"
#include "signature.h"
#include "smalloc.h"
#include "util.h"

using namespace std;  // NOLINT

namespace whitelist {

const int Whitelist::kFlagVerifyRsa     = 0x01;
const int Whitelist::kFlagVerifyPkcs7   = 0x02;
const int Whitelist::kFlagVerifyCaChain = 0x04;


void Whitelist::CopyBuffers(unsigned *plain_size, unsigned char **plain_buf,
                            unsigned *pkcs7_size, unsigned char **pkcs7_buf)
  const
{
  *plain_size = plain_size_;
  *pkcs7_size = pkcs7_size_;
  *plain_buf = NULL;
  *pkcs7_buf = NULL;
  if (plain_size_ > 0) {
    *plain_buf = reinterpret_cast<unsigned char *>(smalloc(plain_size_));
    memcpy(*plain_buf, plain_buf_, plain_size_);
  }
  if (pkcs7_size_ > 0) {
    *pkcs7_buf = reinterpret_cast<unsigned char *>(smalloc(pkcs7_size_));
    memcpy(*pkcs7_buf, pkcs7_buf_, pkcs7_size_);
  }
}


time_t Whitelist::expires() {
  assert(status_ == kStAvailable);
  return expires_;
}


bool Whitelist::IsExpired() const {
  assert(status_ == kStAvailable);
  return time(NULL) > expires_;
}


Failures Whitelist::VerifyLoadedCertificate() const {
  assert(status_ == kStAvailable);

  vector<string> blacklist = signature_manager_->GetBlacklistedCertificates();
  for (unsigned i = 0; i < blacklist.size(); ++i) {
    shash::Any this_hash =
      signature::SignatureManager::MkFromFingerprint(blacklist[i]);
    if (this_hash.IsNull())
      continue;

    shash::Algorithms algorithm = this_hash.algorithm;
    if (this_hash == signature_manager_->HashCertificate(algorithm))
      return kFailBlacklisted;
  }

  for (unsigned i = 0; i < fingerprints_.size(); ++i) {
    shash::Algorithms algorithm = fingerprints_[i].algorithm;
    if (signature_manager_->HashCertificate(algorithm) == fingerprints_[i]) {
      if (verification_flags_ & kFlagVerifyCaChain) {
        bool retval = signature_manager_->VerifyCaChain();
        if (!retval)
          return kFailBadCaChain;
      }
      return kFailOk;
    }
  }
  return kFailNotListed;
}


Failures Whitelist::Load(const std::string &base_url) {
  const bool probe_hosts = base_url == "";
  bool retval_b;
  download::Failures retval_dl;
  whitelist::Failures retval_wl;

  Reset();

  const string whitelist_url = base_url + string("/.cvmfswhitelist");
  download::JobInfo download_whitelist(&whitelist_url,
                                       false, probe_hosts, NULL);
  retval_dl = download_manager_->Fetch(&download_whitelist);
  if (retval_dl != download::kFailOk)
    return kFailLoad;
  plain_size_ = download_whitelist.destination_mem.size;
  if (plain_size_ == 0)
    return kFailEmpty;
  plain_buf_ =
    reinterpret_cast<unsigned char *>(download_whitelist.destination_mem.data);

  retval_wl = ParseWhitelist(plain_buf_, plain_size_);
  if (retval_wl != kFailOk)
    return retval_wl;

  if (verification_flags_ & kFlagVerifyRsa) {
    retval_b = signature_manager_->VerifyLetter(plain_buf_, plain_size_, true);
    if (!retval_b) {
      LogCvmfs(kLogCvmfs, kLogDebug, "failed to verify repository whitelist");
      return kFailBadSignature;
    }
  }

  if (verification_flags_ & kFlagVerifyPkcs7) {
    // Load the separate whitelist pkcs7 structure
    const string whitelist_pkcs7_url =
      base_url + string("cvmfswhitelist.pkcs7");
    download::JobInfo download_whitelist_pkcs7(&whitelist_pkcs7_url, false,
                                               probe_hosts, NULL);
    retval_dl = download_manager_->Fetch(&download_whitelist_pkcs7);
    if (retval_dl != download::kFailOk)
      return kFailLoadPkcs7;
    pkcs7_size_ = download_whitelist_pkcs7.destination_mem.size;
    if (pkcs7_size_ == 0)
      return kFailEmptyPkcs7;
    pkcs7_buf_ = reinterpret_cast<unsigned char *>
      (download_whitelist_pkcs7.destination_mem.data);

    unsigned char *extracted_whitelist;
    unsigned extracted_whitelist_size;
    vector<string> alt_uris;
    retval_b =
      signature_manager_->VerifyPkcs7(pkcs7_buf_, pkcs7_size_,
                                      &extracted_whitelist,
                                      &extracted_whitelist_size,
                                      &alt_uris);
    if (!retval_b) {
      LogCvmfs(kLogCvmfs, kLogDebug,
               "failed to verify repository whitelist (pkcs#7): %s",
               signature_manager_->GetCryptoError().c_str());
      return kFailBadPkcs7;
    }

    // Check for subject alternative name matching the repository name
    bool found_uri = false;
    for (unsigned i = 0; i < alt_uris.size(); ++i) {
      LogCvmfs(kLogSignature, kLogDebug, "found pkcs#7 signer uri %s",
               alt_uris[i].c_str());
      if (alt_uris[i] == "cvmfs:" + fqrn_) {
        found_uri = true;
        break;
      }
    }
    if (!found_uri) {
      LogCvmfs(kLogCvmfs, kLogDebug,
               "failed to find whitelist signer with SAN/URI cvmfs:%s",
               fqrn_.c_str());
      free(extracted_whitelist);
      return kFailBadSignaturePkcs7;
    }

    // Check once again the extracted whitelist
    Reset();
    LogCvmfs(kLogCvmfs, kLogDebug, "Extracted pkcs#7 whitelist:\n%s",
             string(reinterpret_cast<char *>(extracted_whitelist),
                    extracted_whitelist_size).c_str());
    retval_wl = ParseWhitelist(extracted_whitelist, extracted_whitelist_size);
    if (retval_wl != kFailOk) {
      LogCvmfs(kLogCvmfs, kLogDebug,
               "failed to verify repository certificate against pkcs#7 "
               "whitelist");
      return kFailMalformedPkcs7;
    }
  }

  status_ = kStAvailable;
  return kFailOk;
}


Failures Whitelist::ParseWhitelist(const unsigned char *whitelist,
                                   const unsigned whitelist_size)
{
  time_t local_timestamp = time(NULL);
  string line;
  unsigned payload_bytes = 0;
  bool verify_pkcs7 = false;
  bool verify_cachain = false;

  // Check timestamp (UTC), ignore issue date (legacy)
  line = GetLineMem(reinterpret_cast<const char *>(whitelist), whitelist_size);
  if (line.length() != 14) {
    LogCvmfs(kLogSignature, kLogDebug, "invalid timestamp format");
    return kFailMalformed;
  }
  payload_bytes += 15;

  // Expiry date
  line = GetLineMem(reinterpret_cast<const char *>(whitelist)+payload_bytes,
                    whitelist_size-payload_bytes);
  if (line.length() != 15) {
    LogCvmfs(kLogSignature, kLogDebug, "invalid timestamp format");
    return kFailMalformed;
  }
  struct tm tm_wl;
  memset(&tm_wl, 0, sizeof(struct tm));
  tm_wl.tm_year = String2Int64(line.substr(1, 4))-1900;
  tm_wl.tm_mon = String2Int64(line.substr(5, 2)) - 1;
  tm_wl.tm_mday = String2Int64(line.substr(7, 2));
  tm_wl.tm_hour = String2Int64(line.substr(9, 2));
  tm_wl.tm_min = tm_wl.tm_sec = 0;  // exact on hours level
  time_t timestamp = timegm(&tm_wl);
  LogCvmfs(kLogSignature, kLogDebug,
           "whitelist UTC expiry timestamp in localtime: %s",
           StringifyTime(timestamp, false).c_str());
  if (timestamp < 0) {
    LogCvmfs(kLogSignature, kLogDebug, "invalid timestamp");
    return kFailMalformed;
  }
  LogCvmfs(kLogSignature, kLogDebug,  "local time: %s",
           StringifyTime(local_timestamp, true).c_str());
  if (local_timestamp > timestamp) {
    LogCvmfs(kLogSignature, kLogDebug | kLogSyslogErr,
             "whitelist lifetime verification failed, expired");
    return kFailExpired;
  }
  expires_ = timestamp;
  payload_bytes += 16;

  // Check repository name
  line = GetLineMem(reinterpret_cast<const char *>(whitelist)+payload_bytes,
                    whitelist_size-payload_bytes);
  if ((fqrn_ != "") && ("N" + fqrn_ != line)) {
    LogCvmfs(kLogSignature, kLogDebug,
             "repository name on the whitelist does not match "
             "(found %s, expected %s)",
             line.c_str(), fqrn_.c_str());
    return kFailNameMismatch;
  }
  payload_bytes += line.length() + 1;

  // Check for PKCS7
  line = GetLineMem(reinterpret_cast<const char *>(whitelist)+payload_bytes,
                    whitelist_size-payload_bytes);
  if (line == "Vpkcs7") {
    LogCvmfs(kLogSignature, kLogDebug, "whitelist verification: pkcs#7");
    verify_pkcs7 = true;
    payload_bytes += line.length() + 1;
    line = GetLineMem(reinterpret_cast<const char *>(whitelist)+payload_bytes,
                      whitelist_size-payload_bytes);
  }

  // Check for CA chain verification
  line = GetLineMem(reinterpret_cast<const char *>(whitelist)+payload_bytes,
                    whitelist_size-payload_bytes);
  if (line == "Wcachain") {
    LogCvmfs(kLogSignature, kLogDebug,
             "whitelist imposes ca chain verification of manifest signature");
    verify_cachain = true;
    payload_bytes += line.length() + 1;
    line = GetLineMem(reinterpret_cast<const char *>(whitelist)+payload_bytes,
                      whitelist_size-payload_bytes);
  }

  do {
    if (line == "--") break;
    shash::Any this_hash = signature::SignatureManager::MkFromFingerprint(line);
    if (!this_hash.IsNull())
      fingerprints_.push_back(this_hash);

    payload_bytes += line.length() + 1;
    line = GetLineMem(reinterpret_cast<const char *>(whitelist)+payload_bytes,
                      whitelist_size-payload_bytes);
  } while (payload_bytes < whitelist_size);

  verification_flags_ = verify_pkcs7 ? kFlagVerifyPkcs7 : kFlagVerifyRsa;
  if (verify_cachain)
    verification_flags_ |= kFlagVerifyCaChain;
  return kFailOk;
}


void Whitelist::Reset() {
  status_ = kStNone;
  fingerprints_.clear();
  expires_ = 0;
  verification_flags_ = 0;
  if (plain_buf_)
    free(plain_buf_);
  if (pkcs7_buf_)
    free(pkcs7_buf_);
  plain_buf_ = NULL;
  pkcs7_buf_ = NULL;
  plain_size_ = 0;
  pkcs7_size_ = 0;
}


Whitelist::Whitelist(const string &fqrn,
                     download::DownloadManager *download_manager,
                     signature::SignatureManager *signature_manager) :
  fqrn_(fqrn),
  download_manager_(download_manager),
  signature_manager_(signature_manager),
  plain_buf_(NULL),
  plain_size_(0),
  pkcs7_buf_(NULL),
  pkcs7_size_(0)
{
  Reset();
}


Whitelist::Whitelist(const Whitelist &other) :
  fqrn_(other.fqrn_),
  download_manager_(other.download_manager_),
  signature_manager_(other.signature_manager_),
  status_(other.status_),
  fingerprints_(other.fingerprints_),
  expires_(other.expires_),
  verification_flags_(other.verification_flags_)
{
  other.CopyBuffers(&plain_size_, &plain_buf_, &pkcs7_size_, &pkcs7_buf_);
}


Whitelist &Whitelist::operator= (const Whitelist &other) {
  if (&other == this)
    return *this;

  Reset();
  fqrn_ = other.fqrn_;
  download_manager_ = other.download_manager_;
  signature_manager_ = other.signature_manager_;
  status_ = other.status_;
  fingerprints_ = other.fingerprints_;
  expires_ = other.expires_;
  verification_flags_ = other.verification_flags_;
  other.CopyBuffers(&plain_size_, &plain_buf_, &pkcs7_size_, &pkcs7_buf_);

  return *this;
}


Whitelist::~Whitelist() {
  Reset();
}

}  // namespace whitelist
