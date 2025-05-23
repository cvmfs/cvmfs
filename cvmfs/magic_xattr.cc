/**
 * This file is part of the CernVM File System.
 */

#include "magic_xattr.h"

#include <cassert>
#include <string>
#include <vector>

#include "catalog_mgr_client.h"
#include "crypto/signature.h"
#include "fetch.h"
#include "mountpoint.h"
#include "quota.h"
#include "util/logging.h"
#include "util/string.h"

MagicXattrManager::MagicXattrManager(
    MountPoint *mountpoint,
    EVisibility visibility,
    const std::set<std::string> &protected_xattrs,
    const std::set<gid_t> &priviledged_xattr_gids)
    : mount_point_(mountpoint)
    , visibility_(visibility)
    , protected_xattrs_(protected_xattrs)
    , privileged_xattr_gids_(priviledged_xattr_gids)
    , is_frozen_(false) {
  Register("user.catalog_counters", new CatalogCountersMagicXattr());
  Register("user.external_host", new ExternalHostMagicXattr());
  Register("user.external_timeout", new ExternalTimeoutMagicXattr());
  Register("user.fqrn", new FqrnMagicXattr());
  Register("user.host", new HostMagicXattr());
  Register("user.host_list", new HostListMagicXattr());
  Register("user.ncleanup24", new NCleanup24MagicXattr());
  Register("user.nclg", new NClgMagicXattr());
  Register("user.ndiropen", new NDirOpenMagicXattr());
  Register("user.ndownload", new NDownloadMagicXattr());
  Register("user.nioerr", new NIOErrMagicXattr());
  Register("user.nopen", new NOpenMagicXattr());
  Register("user.hitrate", new HitrateMagicXattr());
  Register("user.logbuffer", new LogBufferXattr());
  Register("user.proxy", new ProxyMagicXattr());
  Register("user.proxy_list", new ProxyListMagicXattr());
  Register("user.proxy_list_external", new ProxyListExternalMagicXattr());
  Register("user.pubkeys", new PubkeysMagicXattr());
  Register("user.repo_counters", new RepoCountersMagicXattr());
  Register("user.repo_metainfo", new RepoMetainfoMagicXattr());
  Register("user.revision", new RevisionMagicXattr());
  Register("user.root_hash", new RootHashMagicXattr());
  Register("user.rx", new RxMagicXattr());
  Register("user.speed", new SpeedMagicXattr());
  Register("user.tag", new TagMagicXattr());
  Register("user.timeout", new TimeoutMagicXattr());
  Register("user.timeout_direct", new TimeoutDirectMagicXattr());
  Register("user.timestamp_last_ioerr", new TimestampLastIOErrMagicXattr());
  Register("user.usedfd", new UsedFdMagicXattr());
  Register("user.useddirp", new UsedDirPMagicXattr());
  Register("user.version", new VersionMagicXattr());

  Register("user.hash", new HashMagicXattr());
  Register("user.lhash", new LHashMagicXattr());

  Register("user.chunk_list", new ChunkListMagicXattr());
  Register("user.chunks", new ChunksMagicXattr());
  Register("user.compression", new CompressionMagicXattr());
  Register("user.direct_io", new DirectIoMagicXattr());
  Register("user.external_file", new ExternalFileMagicXattr());

  Register("user.rawlink", new RawlinkMagicXattr());
  Register("xfsroot.rawlink", new RawlinkMagicXattr());

  Register("user.authz", new AuthzMagicXattr());
  Register("user.external_url", new ExternalURLMagicXattr());
}

std::string MagicXattrManager::GetListString(catalog::DirectoryEntry *dirent) {
  if (visibility() == kVisibilityNever) {
    return "";
  }
  // Only the root entry has an empty name
  if (visibility() == kVisibilityRootOnly && !dirent->name().IsEmpty()) {
    return "";
  }

  std::string result;
  std::map<std::string, BaseMagicXattr *>::iterator it = xattr_list_.begin();
  for (; it != xattr_list_.end(); ++it) {
    MagicXattrFlavor flavor = (*it).second->GetXattrFlavor();
    // Skip those which should not be displayed
    switch (flavor) {
      case kXattrBase:
        break;
      case kXattrWithHash:
        if (dirent->checksum().IsNull())
          continue;
        break;
      case kXattrRegular:
        if (!dirent->IsRegular())
          continue;
        break;
      case kXattrExternal:
        if (!(dirent->IsRegular() && dirent->IsExternalFile()))
          continue;
        break;
      case kXattrSymlink:
        if (!dirent->IsLink())
          continue;
        break;
      case kXattrAuthz:
        if (!mount_point_->has_membership_req())
          continue;
        break;
      default:
        PANIC("unknown magic xattr flavor");
    }
    result += (*it).first;
    result.push_back('\0');
  }

  return result;
}

BaseMagicXattr *MagicXattrManager::GetLocked(const std::string &name,
                                             PathString path,
                                             catalog::DirectoryEntry *d) {
  BaseMagicXattr *result;
  if (xattr_list_.count(name) > 0) {
    result = xattr_list_[name];
  } else {
    return NULL;
  }

  result->Lock(path, d);

  return result;
}

void MagicXattrManager::Register(const std::string &name,
                                 BaseMagicXattr *magic_xattr) {
  assert(!is_frozen_);
  if (xattr_list_.count(name) > 0) {
    PANIC(kLogSyslogErr,
          "Magic extended attribute with name %s already registered",
          name.c_str());
  }
  magic_xattr->xattr_mgr_ = this;
  xattr_list_[name] = magic_xattr;

  // Mark Xattr protected so that only certain fuse_gids' can access it.
  // If Xattr with registered "name" is part of *protected_xattrs
  if (protected_xattrs_.count(name) > 0) {
    magic_xattr->MarkProtected();
  }
}

bool MagicXattrManager::IsPrivilegedGid(gid_t gid) {
  return privileged_xattr_gids_.count(gid) > 0;
}

bool BaseMagicXattr::PrepareValueFencedProtected(gid_t gid) {
  assert(xattr_mgr_->is_frozen());
  if (is_protected_ && !xattr_mgr_->IsPrivilegedGid(gid)) {
    return false;
  }

  return PrepareValueFenced();
}

void MagicXattrManager::SanityCheckProtectedXattrs() {
  std::set<std::string>::const_iterator iter;
  std::vector<string> tmp;
  for (iter = protected_xattrs_.begin(); iter != protected_xattrs_.end();
       iter++) {
    if (xattr_list_.find(*iter) == xattr_list_.end()) {
      tmp.push_back(*iter);
    }
  }

  if (tmp.size() > 0) {
    std::string msg = JoinStrings(tmp, ",");
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogWarn,
             "Following CVMFS_XATTR_PROTECTED_XATTRS are "
             "set but not recognized: %s",
             msg.c_str());
  }

  tmp.clear();
  std::set<gid_t>::const_iterator iter_gid;
  for (iter_gid = privileged_xattr_gids_.begin();
       iter_gid != privileged_xattr_gids_.end();
       iter_gid++) {
    tmp.push_back(StringifyUint(*iter_gid));
  }

  if (tmp.size() > 0) {
    std::string msg = JoinStrings(tmp, ",");
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog,
             "Following CVMFS_XATTR_PRIVILEGED_GIDS are set: %s", msg.c_str());
  }
}

std::string BaseMagicXattr::HeaderMultipageHuman(uint32_t requested_page) {
  return "# Access page at idx: " + StringifyUint(requested_page) + ". "
         + "Total num pages: " + StringifyUint(result_pages_.size())
         + " (access other pages: xattr~<page_num>, starting "
         + " with 0; number of pages available: xattr~?)\n";
}

std::pair<bool, std::string> BaseMagicXattr::GetValue(
    int32_t requested_page, const MagicXattrMode mode) {
  assert(requested_page >= -1);
  result_pages_.clear();
  FinalizeValue();

  std::string res = "";
  if (mode == kXattrMachineMode) {
    if (requested_page >= static_cast<int32_t>(result_pages_.size())) {
      return std::pair<bool, std::string>(false, "");
    }
    if (requested_page == -1) {
      return std::pair<bool, std::string>(
          true, "num_pages, " + StringifyUint(result_pages_.size()));
    }
  } else if (mode == kXattrHumanMode) {
    if (requested_page >= static_cast<int32_t>(result_pages_.size())) {
      return std::pair<bool, std::string>(
          true,
          "Page requested does not exists. There are "
              + StringifyUint(result_pages_.size()) + " pages available.\n"
              + "Access them with xattr~<page_num> (machine-readable mode) "
              + "or xattr@<page_num> (human-readable mode).\n"
              + "Use xattr@? or xattr~? to get extra info about the attribute");
    } else if (requested_page == -1) {
      return std::pair<bool, std::string>(
          true,
          "Access xattr with xattr~<page_num> (machine-readable mode) or "
              + std::string(" xattr@<page_num> (human-readable mode).\n")
              + "Pages available: " + StringifyUint(result_pages_.size()));
    } else {
      res = HeaderMultipageHuman(requested_page);
    }
  } else {
    PANIC(kLogStderr | kLogSyslogErr,
          "Unknown mode of magic xattr requested: %d", mode);
  }

  res += result_pages_[requested_page];

  return std::pair<bool, std::string>(true, res);
}

bool AuthzMagicXattr::PrepareValueFenced() {
  return xattr_mgr_->mount_point()->has_membership_req();
}

void AuthzMagicXattr::FinalizeValue() {
  result_pages_.push_back(xattr_mgr_->mount_point()->membership_req());
}

MagicXattrFlavor AuthzMagicXattr::GetXattrFlavor() { return kXattrAuthz; }

bool CatalogCountersMagicXattr::PrepareValueFenced() {
  counters_ = xattr_mgr_->mount_point()->catalog_mgr()->LookupCounters(
      path_, &subcatalog_path_, &hash_);
  return true;
}

void CatalogCountersMagicXattr::FinalizeValue() {
  std::string res;
  res = "catalog_hash: " + hash_.ToString() + "\n";
  res += "catalog_mountpoint: " + subcatalog_path_ + "\n";
  res += counters_.GetCsvMap();

  result_pages_.push_back(res);
}

bool ChunkListMagicXattr::PrepareValueFenced() {
  chunk_list_.clear();

  const std::string header = "hash,offset,size\n";
  std::string chunk_list_page_(header);
  if (!dirent_->IsRegular()) {
    chunk_list_.push_back(chunk_list_page_);
    return false;
  }
  if (dirent_->IsChunkedFile()) {
    FileChunkList chunks;
    if (!xattr_mgr_->mount_point()->catalog_mgr()->ListFileChunks(
            path_, dirent_->hash_algorithm(), &chunks)
        || chunks.IsEmpty()) {
      LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
               "file %s is marked as "
               "'chunked', but no chunks found.",
               path_.c_str());
      return false;
    } else {
      for (size_t i = 0; i < chunks.size(); ++i) {
        chunk_list_page_ += chunks.At(i).content_hash().ToString() + ",";
        chunk_list_page_ += StringifyInt(chunks.At(i).offset()) + ",";
        chunk_list_page_ += StringifyUint(chunks.At(i).size()) + "\n";

        if (chunk_list_page_.size() > kMaxCharsPerPage) {
          chunk_list_.push_back(chunk_list_page_);
          chunk_list_page_ = header;
        }
      }
    }
  } else {
    chunk_list_page_ += dirent_->checksum().ToString() + ",";
    chunk_list_page_ += "0,";
    chunk_list_page_ += StringifyUint(dirent_->size()) + "\n";
  }

  // add the last page
  if (chunk_list_page_.size() > header.size()) {
    chunk_list_.push_back(chunk_list_page_);
  }

  return true;
}

void ChunkListMagicXattr::FinalizeValue() { result_pages_ = chunk_list_; }

bool ChunksMagicXattr::PrepareValueFenced() {
  if (!dirent_->IsRegular()) {
    return false;
  }
  if (dirent_->IsChunkedFile()) {
    FileChunkList chunks;
    if (!xattr_mgr_->mount_point()->catalog_mgr()->ListFileChunks(
            path_, dirent_->hash_algorithm(), &chunks)
        || chunks.IsEmpty()) {
      LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
               "file %s is marked as "
               "'chunked', but no chunks found.",
               path_.c_str());
      return false;
    } else {
      n_chunks_ = chunks.size();
    }
  } else {
    n_chunks_ = 1;
  }

  return true;
}

void ChunksMagicXattr::FinalizeValue() {
  result_pages_.push_back(StringifyUint(n_chunks_));
}

bool CompressionMagicXattr::PrepareValueFenced() {
  return dirent_->IsRegular();
}

void CompressionMagicXattr::FinalizeValue() {
  result_pages_.push_back(
      zlib::AlgorithmName(dirent_->compression_algorithm()));
}

bool DirectIoMagicXattr::PrepareValueFenced() { return dirent_->IsRegular(); }

void DirectIoMagicXattr::FinalizeValue() {
  result_pages_.push_back(dirent_->IsDirectIo() ? "1" : "0");
}

bool ExternalFileMagicXattr::PrepareValueFenced() {
  return dirent_->IsRegular();
}

void ExternalFileMagicXattr::FinalizeValue() {
  result_pages_.push_back(dirent_->IsExternalFile() ? "1" : "0");
}

void ExternalHostMagicXattr::FinalizeValue() {
  std::vector<string> host_chain;
  std::vector<int> rtt;
  unsigned current_host;
  xattr_mgr_->mount_point()->external_download_mgr()->GetHostInfo(
      &host_chain, &rtt, &current_host);
  if (host_chain.size()) {
    result_pages_.push_back(std::string(host_chain[current_host]));
  } else {
    result_pages_.push_back("internal error: no hosts defined");
  }
}

void ExternalTimeoutMagicXattr::FinalizeValue() {
  unsigned seconds, seconds_direct;
  xattr_mgr_->mount_point()->external_download_mgr()->GetTimeout(
      &seconds, &seconds_direct);
  result_pages_.push_back(StringifyUint(seconds_direct));
}

void FqrnMagicXattr::FinalizeValue() {
  result_pages_.push_back(xattr_mgr_->mount_point()->fqrn());
}

bool HashMagicXattr::PrepareValueFenced() {
  return !dirent_->checksum().IsNull();
}

void HashMagicXattr::FinalizeValue() {
  result_pages_.push_back(dirent_->checksum().ToString());
}

void HostMagicXattr::FinalizeValue() {
  std::vector<std::string> host_chain;
  std::vector<int> rtt;
  unsigned current_host;
  xattr_mgr_->mount_point()->download_mgr()->GetHostInfo(&host_chain, &rtt,
                                                         &current_host);
  if (host_chain.size()) {
    result_pages_.push_back(std::string(host_chain[current_host]));
  } else {
    result_pages_.push_back("internal error: no hosts defined");
  }
}

void HostListMagicXattr::FinalizeValue() {
  std::string result;
  std::vector<std::string> host_chain;
  std::vector<int> rtt;
  unsigned current_host;
  xattr_mgr_->mount_point()->download_mgr()->GetHostInfo(&host_chain, &rtt,
                                                         &current_host);
  if (host_chain.size()) {
    result = host_chain[current_host];
    for (unsigned i = 1; i < host_chain.size(); ++i) {
      result += ";" + host_chain[(i + current_host) % host_chain.size()];
    }
  } else {
    result = "internal error: no hosts defined";
  }
  result_pages_.push_back(result);
}

bool LHashMagicXattr::PrepareValueFenced() {
  return !dirent_->checksum().IsNull();
}

void LHashMagicXattr::FinalizeValue() {
  string result;
  CacheManager::Label label;
  label.path = path_.ToString();
  if (xattr_mgr_->mount_point()->catalog_mgr()->volatile_flag())
    label.flags = CacheManager::kLabelVolatile;
  int fd = xattr_mgr_->mount_point()->file_system()->cache_mgr()->Open(
      CacheManager::LabeledObject(dirent_->checksum(), label));
  if (fd < 0) {
    result = "Not in cache";
  } else {
    shash::Any hash(dirent_->checksum().algorithm);
    int retval_i = xattr_mgr_->mount_point()
                       ->file_system()
                       ->cache_mgr()
                       ->ChecksumFd(fd, &hash);
    if (retval_i != 0)
      result = "I/O error (" + StringifyInt(retval_i) + ")";
    else
      result = hash.ToString();
    xattr_mgr_->mount_point()->file_system()->cache_mgr()->Close(fd);
  }
  result_pages_.push_back(result);
}

LogBufferXattr::LogBufferXattr() : BaseMagicXattr(), throttle_(1, 500, 2000) { }

void LogBufferXattr::FinalizeValue() {
  throttle_.Throttle();
  std::vector<LogBufferEntry> buffer = GetLogBuffer();
  std::string result;
  for (std::vector<LogBufferEntry>::reverse_iterator itr = buffer.rbegin();
       itr != buffer.rend();
       ++itr) {
    if (itr->message.size() > kMaxLogLine) {
      itr->message.resize(kMaxLogLine);
      itr->message += " <snip>";
    }
    result += "[" + StringifyLocalTime(itr->timestamp) + "] " + itr->message
              + "\n";
  }
  result_pages_.push_back(result);
}

void NCleanup24MagicXattr::FinalizeValue() {
  QuotaManager *quota_mgr = xattr_mgr_->mount_point()
                                ->file_system()
                                ->cache_mgr()
                                ->quota_mgr();
  if (!quota_mgr->HasCapability(QuotaManager::kCapIntrospectCleanupRate)) {
    result_pages_.push_back(StringifyInt(-1));
  } else {
    const uint64_t period_s = 24 * 60 * 60;
    const uint64_t rate = quota_mgr->GetCleanupRate(period_s);
    result_pages_.push_back(StringifyUint(rate));
  }
}

bool NClgMagicXattr::PrepareValueFenced() {
  n_catalogs_ = xattr_mgr_->mount_point()->catalog_mgr()->GetNumCatalogs();
  return true;
}

void NClgMagicXattr::FinalizeValue() {
  result_pages_.push_back(StringifyInt(n_catalogs_));
}

void NDirOpenMagicXattr::FinalizeValue() {
  result_pages_.push_back(
      xattr_mgr_->mount_point()->file_system()->n_fs_dir_open()->ToString());
}

void NDownloadMagicXattr::FinalizeValue() {
  result_pages_.push_back(xattr_mgr_->mount_point()
                              ->statistics()
                              ->Lookup("fetch.n_downloads")
                              ->Print());
}

void NIOErrMagicXattr::FinalizeValue() {
  result_pages_.push_back(StringifyInt(
      xattr_mgr_->mount_point()->file_system()->io_error_info()->count()));
}

void NOpenMagicXattr::FinalizeValue() {
  result_pages_.push_back(
      xattr_mgr_->mount_point()->file_system()->n_fs_open()->ToString());
}

void HitrateMagicXattr::FinalizeValue() {
  int64_t n_invocations = xattr_mgr_->mount_point()
                              ->statistics()
                              ->Lookup("fetch.n_invocations")
                              ->Get();
  if (n_invocations == 0) {
    result_pages_.push_back("n/a");
    return;
  }

  int64_t n_downloads = xattr_mgr_->mount_point()
                            ->statistics()
                            ->Lookup("fetch.n_downloads")
                            ->Get();
  float hitrate = 100.
                  * (1.
                     - (static_cast<float>(n_downloads)
                        / static_cast<float>(n_invocations)));
  result_pages_.push_back(StringifyDouble(hitrate));
}

void ProxyMagicXattr::FinalizeValue() {
  vector<vector<download::DownloadManager::ProxyInfo> > proxy_chain;
  unsigned current_group;
  xattr_mgr_->mount_point()->download_mgr()->GetProxyInfo(&proxy_chain,
                                                          &current_group, NULL);
  if (proxy_chain.size()) {
    result_pages_.push_back(proxy_chain[current_group][0].url);
  } else {
    result_pages_.push_back("DIRECT");
  }
}

static void ListProxy(download::DownloadManager *dm,
                      std::vector<std::string> *result_pages) {
  vector<vector<download::DownloadManager::ProxyInfo> > proxy_chain;
  unsigned current_group;
  dm->GetProxyInfo(&proxy_chain, &current_group, NULL);
  std::string buf = "";
  for (unsigned int i = 0; i < proxy_chain.size(); i++) {
    for (unsigned int j = 0; j < proxy_chain[i].size(); j++) {
      buf += proxy_chain[i][j].url;
      buf += "\n";
    }

    if (buf.size() > BaseMagicXattr::kMaxCharsPerPage) {
      result_pages->push_back(buf);
      buf = "";
    }
  }

  if (buf.size() > 0 || result_pages->size() == 0) {
    result_pages->push_back(buf);
  }
}

void ProxyListMagicXattr::FinalizeValue() {
  ListProxy(xattr_mgr_->mount_point()->download_mgr(), &result_pages_);
}

void ProxyListExternalMagicXattr::FinalizeValue() {
  ListProxy(xattr_mgr_->mount_point()->external_download_mgr(), &result_pages_);
}

bool PubkeysMagicXattr::PrepareValueFenced() {
  pubkeys_ = xattr_mgr_->mount_point()
                 ->signature_mgr()
                 ->GetActivePubkeysAsVector();
  return true;
}

void PubkeysMagicXattr::FinalizeValue() {
  size_t full_size = 0;

  for (size_t i = 0; i < pubkeys_.size(); i++) {
    full_size += pubkeys_[i].size();
  }

  if (full_size == 0) {
    return;
  }

  size_t size_within_page = 0;
  std::string res = "";

  for (size_t i = 0; i < pubkeys_.size(); i++) {
    if (size_within_page + pubkeys_[i].size() >= kMaxCharsPerPage) {
      result_pages_.push_back(res);
      res = "";
      size_within_page = 0;
    }

    res += pubkeys_[i];
    size_within_page += pubkeys_[i].size();
  }
  if (res.size() > 0) {
    result_pages_.push_back(res);
  }
}

bool RawlinkMagicXattr::PrepareValueFenced() { return dirent_->IsLink(); }

void RawlinkMagicXattr::FinalizeValue() {
  result_pages_.push_back(dirent_->symlink().ToString());
}

bool RepoCountersMagicXattr::PrepareValueFenced() {
  counters_ = xattr_mgr_->mount_point()
                  ->catalog_mgr()
                  ->GetRootCatalog()
                  ->GetCounters();
  return true;
}

void RepoCountersMagicXattr::FinalizeValue() {
  result_pages_.push_back(counters_.GetCsvMap());
}

uint64_t RepoMetainfoMagicXattr::kMaxMetainfoLength = 65536;

bool RepoMetainfoMagicXattr::PrepareValueFenced() {
  if (!xattr_mgr_->mount_point()->catalog_mgr()->manifest()) {
    error_reason_ = "manifest not available";
    return true;
  }

  metainfo_hash_ = xattr_mgr_->mount_point()
                       ->catalog_mgr()
                       ->manifest()
                       ->meta_info();
  if (metainfo_hash_.IsNull()) {
    error_reason_ = "metainfo not available";
    return true;
  }
  return true;
}

void RepoMetainfoMagicXattr::FinalizeValue() {
  if (metainfo_hash_.IsNull()) {
    result_pages_.push_back(error_reason_);
    return;
  }

  CacheManager::Label label;
  label.path = xattr_mgr_->mount_point()->fqrn() + "("
               + metainfo_hash_.ToString() + ")";
  label.flags = CacheManager::kLabelMetainfo;
  int fd = xattr_mgr_->mount_point()->fetcher()->Fetch(
      CacheManager::LabeledObject(metainfo_hash_, label));
  if (fd < 0) {
    result_pages_.push_back("Failed to open metadata file");
    return;
  }
  uint64_t actual_size = xattr_mgr_->mount_point()
                             ->file_system()
                             ->cache_mgr()
                             ->GetSize(fd);
  if (actual_size > kMaxMetainfoLength) {
    xattr_mgr_->mount_point()->file_system()->cache_mgr()->Close(fd);
    result_pages_.push_back("Failed to open: metadata file is too big");
    return;
  }
  char buffer[kMaxMetainfoLength];
  int64_t
      bytes_read = xattr_mgr_->mount_point()->file_system()->cache_mgr()->Pread(
          fd, buffer, actual_size, 0);
  xattr_mgr_->mount_point()->file_system()->cache_mgr()->Close(fd);
  if (bytes_read < 0) {
    result_pages_.push_back("Failed to read metadata file");
    return;
  }
  result_pages_.push_back(string(buffer, buffer + bytes_read));
}

bool RevisionMagicXattr::PrepareValueFenced() {
  revision_ = xattr_mgr_->mount_point()->catalog_mgr()->GetRevision();
  return true;
}

void RevisionMagicXattr::FinalizeValue() {
  result_pages_.push_back(StringifyUint(revision_));
}

bool RootHashMagicXattr::PrepareValueFenced() {
  root_hash_ = xattr_mgr_->mount_point()->catalog_mgr()->GetRootHash();
  return true;
}

void RootHashMagicXattr::FinalizeValue() {
  result_pages_.push_back(root_hash_.ToString());
}

void RxMagicXattr::FinalizeValue() {
  perf::Statistics *statistics = xattr_mgr_->mount_point()->statistics();
  int64_t rx = statistics->Lookup("download.sz_transferred_bytes")->Get();
  result_pages_.push_back(StringifyInt(rx / 1024));
}

void SpeedMagicXattr::FinalizeValue() {
  perf::Statistics *statistics = xattr_mgr_->mount_point()->statistics();
  int64_t rx = statistics->Lookup("download.sz_transferred_bytes")->Get();
  int64_t time = statistics->Lookup("download.sz_transfer_time")->Get();
  if (time == 0) {
    result_pages_.push_back("n/a");
  } else {
    result_pages_.push_back(StringifyInt((1000 * (rx / 1024)) / time));
  }
}

bool TagMagicXattr::PrepareValueFenced() {
  tag_ = xattr_mgr_->mount_point()->repository_tag();
  return true;
}

void TagMagicXattr::FinalizeValue() { result_pages_.push_back(tag_); }

void TimeoutMagicXattr::FinalizeValue() {
  unsigned seconds, seconds_direct;
  xattr_mgr_->mount_point()->download_mgr()->GetTimeout(&seconds,
                                                        &seconds_direct);
  result_pages_.push_back(StringifyUint(seconds));
}

void TimeoutDirectMagicXattr::FinalizeValue() {
  unsigned seconds, seconds_direct;
  xattr_mgr_->mount_point()->download_mgr()->GetTimeout(&seconds,
                                                        &seconds_direct);
  result_pages_.push_back(StringifyUint(seconds_direct));
}

void TimestampLastIOErrMagicXattr::FinalizeValue() {
  result_pages_.push_back(StringifyInt(xattr_mgr_->mount_point()
                                           ->file_system()
                                           ->io_error_info()
                                           ->timestamp_last()));
}

void UsedFdMagicXattr::FinalizeValue() {
  result_pages_.push_back(
      xattr_mgr_->mount_point()->file_system()->no_open_files()->ToString());
}

void UsedDirPMagicXattr::FinalizeValue() {
  result_pages_.push_back(
      xattr_mgr_->mount_point()->file_system()->no_open_dirs()->ToString());
}

void VersionMagicXattr::FinalizeValue() {
  result_pages_.push_back(std::string(CVMFS_VERSION) + "."
                          + std::string(CVMFS_PATCH_LEVEL));
}

void ExternalURLMagicXattr::FinalizeValue() {
  std::vector<std::string> host_chain;
  std::vector<int> rtt;
  unsigned current_host;
  if (xattr_mgr_->mount_point()->external_download_mgr() != NULL) {
    xattr_mgr_->mount_point()->external_download_mgr()->GetHostInfo(
        &host_chain, &rtt, &current_host);
    if (host_chain.size()) {
      result_pages_.push_back(std::string(host_chain[current_host])
                              + std::string(path_.c_str()));
      return;
    }
  }
  result_pages_.push_back("");
}

bool ExternalURLMagicXattr::PrepareValueFenced() {
  return dirent_->IsRegular() && dirent_->IsExternalFile();
}
