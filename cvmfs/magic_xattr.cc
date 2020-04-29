/**
 * This file is part of the CernVM File System.
 */

#include <cassert>
#include <string>
#include <vector>

#include "catalog_mgr_client.h"
#include "fetch.h"
#include "logging.h"
#include "mountpoint.h"
#include "quota.h"
#include "signature.h"

#include "magic_xattr.h"

MagicXattrManager::MagicXattrManager(MountPoint *mountpoint)
  : mount_point_(mountpoint)
{
  base_xattrs_["user.catalog_counters"] = new CatalogCountersMagicXattr();
  base_xattrs_["user.external_host"] = new ExternalHostMagicXattr();
  base_xattrs_["user.external_timeout"] = new ExternalTimeoutMagicXattr();
  base_xattrs_["user.fqrn"] = new FqrnMagicXattr();
  base_xattrs_["user.host"] = new HostMagicXattr();
  base_xattrs_["user.host_list"] = new HostListMagicXattr();
  base_xattrs_["user.ncleanup24"] = new NCleanup24MagicXattr();
  base_xattrs_["user.nclg"] = new NClgMagicXattr();
  base_xattrs_["user.ndiropen"] = new NDirOpenMagicXattr();
  base_xattrs_["user.ndownload"] = new NDownloadMagicXattr();
  base_xattrs_["user.nioerr"] = new NIOErrMagicXattr();
  base_xattrs_["user.nopen"] = new NOpenMagicXattr();
  base_xattrs_["user.proxy"] = new ProxyMagicXattr();
  base_xattrs_["user.pubkeys"] = new PubkeysMagicXattr();
  base_xattrs_["user.repo_counters"] = new RepoCountersMagicXattr();
  base_xattrs_["user.repo_metainfo"] = new RepoMetainfoMagicXattr();
  base_xattrs_["user.revision"] = new RevisionMagicXattr();
  base_xattrs_["user.root_hash"] = new RootHashMagicXattr();
  base_xattrs_["user.rx"] = new RxMagicXattr();
  base_xattrs_["user.speed"] = new SpeedMagicXattr();
  base_xattrs_["user.tag"] = new TagMagicXattr();
  base_xattrs_["user.timeout"] = new TimeoutMagicXattr();
  base_xattrs_["user.timeout_direct"] = new TimeoutDirectMagicXattr();
  base_xattrs_["user.usedfd"] = new UsedFdMagicXattr();
  base_xattrs_["user.useddirp"] = new UsedDirPMagicXattr();
  base_xattrs_["user.version"] = new VersionMagicXattr();

  withhash_xattrs_["user.hash"] = new HashMagicXattr();
  withhash_xattrs_["user.lhash"] = new LHashMagicXattr();

  regular_xattrs_["user.chunks"] = new ChunksMagicXattr();
  regular_xattrs_["user.compression"] = new CompressionMagicXattr();
  regular_xattrs_["user.external_file"] = new ExternalFileMagicXattr();

  symlink_xattrs_["user.rawlink"] = new RawlinkMagicXattr();
  symlink_xattrs_["xfsroot.rawlink"] = symlink_xattrs_["user.rawlink"];

  authz_xattrs_["user.authz"] = new AuthZMagicXattr();
}

std::string MagicXattrManager::GetListString(catalog::DirectoryEntry *dirent) {
  std::string result;
  std::map<std::string, BaseMagicXattr *>::iterator it = base_xattrs_.begin();
  for (; it != base_xattrs_.end(); ++it) {
    result += (*it).first + std::string("\0", 1);
  }
  if (!dirent->checksum().IsNull()) {
    it = withhash_xattrs_.begin();
    for (; it != withhash_xattrs_.end(); ++it) {
      result += (*it).first + std::string("\0", 1);
    }
  }
  if (dirent->IsRegular()) {
    it = regular_xattrs_.begin();
    for (; it != regular_xattrs_.end(); ++it) {
      result += (*it).first + std::string("\0", 1);
    }
  }
  if (dirent->IsLink()) {
    it = symlink_xattrs_.begin();
    for (; it != symlink_xattrs_.end(); ++it) {
      result += (*it).first + std::string("\0", 1);
    }
  }
  if (mount_point_->has_membership_req()) {
    it = authz_xattrs_.begin();
    for (; it != authz_xattrs_.end(); ++it) {
      result += (*it).first + std::string("\0", 1);
    }
  }
  return result;
}

BaseMagicXattr *MagicXattrManager::Get(const std::string &name,
                                       PathString path,
                                       catalog::DirectoryEntry *d)
{
  BaseMagicXattr *result;
  if (base_xattrs_.count(name) > 0) {
    result = base_xattrs_[name];
  } else if (withhash_xattrs_.count(name) > 0) {
    result = withhash_xattrs_[name];
  } else if (regular_xattrs_.count(name) > 0) {
    result = regular_xattrs_[name];
  } else if (symlink_xattrs_.count(name) > 0) {
    result = symlink_xattrs_[name];
  } else if (authz_xattrs_.count(name) > 0) {
    result = authz_xattrs_[name];
  } else {
    return NULL;
  }

  result->path_ = path;
  result->dirent_ = d;
  result->mount_point_ = mount_point_;
  return result;
}

void MagicXattrManager::Register(const std::string &name,
                                 BaseMagicXattr *magic_xattr)
{
  base_xattrs_[name] = magic_xattr;
}

bool AuthZMagicXattr::PrepareValueFenced() {
  return mount_point_->has_membership_req();
}

std::string AuthZMagicXattr::GetValue() {
  return mount_point_->membership_req();
}

bool CatalogCountersMagicXattr::PrepareValueFenced() {
  counters_ =
    mount_point_->catalog_mgr()->LookupCounters(path_, &subcatalog_path_);
  return true;
}

std::string CatalogCountersMagicXattr::GetValue() {
  std::string res;
  res = "catalog_mountpoint: " + subcatalog_path_ + "\n";
  res += counters_.GetCsvMap();
  return res;
}

bool ChunksMagicXattr::PrepareValueFenced() {
  if (!dirent_->IsRegular()) {
    return false;
  }
  if (dirent_->IsChunkedFile()) {
    FileChunkList chunks;
    if (!mount_point_->catalog_mgr()
                     ->ListFileChunks(path_, dirent_->hash_algorithm(), &chunks)
        || chunks.IsEmpty())
    {
      LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr, "file %s is marked as "
                "'chunked', but no chunks found.", path_.c_str());
      return false;
    } else {
      n_chunks_ = chunks.size();
    }
  } else {
    n_chunks_ = 1;
  }
  return true;
}

std::string ChunksMagicXattr::GetValue() {
  return StringifyUint(n_chunks_);
}

bool CompressionMagicXattr::PrepareValueFenced() {
  return dirent_->IsRegular();
}

std::string CompressionMagicXattr::GetValue() {
  return zlib::AlgorithmName(dirent_->compression_algorithm());
}

bool ExternalFileMagicXattr::PrepareValueFenced() {
  return dirent_->IsRegular();
}

std::string ExternalFileMagicXattr::GetValue() {
  return dirent_->IsExternalFile() ? "1" : "0";
}

bool ExternalHostMagicXattr::PrepareValueFenced() {
  return true;
}

std::string ExternalHostMagicXattr::GetValue() {
  std::vector<string> host_chain;
  std::vector<int> rtt;
  unsigned current_host;
  mount_point_->external_download_mgr()->GetHostInfo(
    &host_chain, &rtt, &current_host);
  if (host_chain.size()) {
    return std::string(host_chain[current_host]);
  } else {
    return "internal error: no hosts defined";
  }
}

bool ExternalTimeoutMagicXattr::PrepareValueFenced() {
  return true;
}

std::string ExternalTimeoutMagicXattr::GetValue() {
  unsigned seconds, seconds_direct;
  mount_point_->download_mgr()->GetTimeout(&seconds, &seconds_direct);
  return StringifyUint(seconds_direct);
}

bool FqrnMagicXattr::PrepareValueFenced() {
  return true;
}

std::string FqrnMagicXattr::GetValue() {
  return mount_point_->fqrn();
}

bool HashMagicXattr::PrepareValueFenced() {
  return !dirent_->checksum().IsNull();
}

std::string HashMagicXattr::GetValue() {
  return dirent_->checksum().ToString();
}

bool HostMagicXattr::PrepareValueFenced() {
  return true;
}

std::string HostMagicXattr::GetValue() {
  std::vector<std::string> host_chain;
  std::vector<int> rtt;
  unsigned current_host;
  mount_point_->download_mgr()->GetHostInfo(&host_chain, &rtt, &current_host);
  if (host_chain.size()) {
    return std::string(host_chain[current_host]);
  } else {
    return "internal error: no hosts defined";
  }
}

bool HostListMagicXattr::PrepareValueFenced() {
  return true;
}

std::string HostListMagicXattr::GetValue() {
  std::string result;
  std::vector<std::string> host_chain;
  std::vector<int> rtt;
  unsigned current_host;
  mount_point_->download_mgr()->GetHostInfo(&host_chain, &rtt, &current_host);
  if (host_chain.size()) {
    result = host_chain[current_host];
    for (unsigned i = 1; i < host_chain.size(); ++i) {
      result +=
        ";" + host_chain[(i+current_host) % host_chain.size()];
    }
  } else {
    result = "internal error: no hosts defined";
  }
  return result;
}

bool LHashMagicXattr::PrepareValueFenced() {
  return !dirent_->checksum().IsNull();
}

std::string LHashMagicXattr::GetValue() {
  string result;
  CacheManager::ObjectInfo object_info;
  object_info.description = path_.ToString();
  if (mount_point_->catalog_mgr()->volatile_flag())
    object_info.type = CacheManager::kTypeVolatile;
  int fd = mount_point_->file_system()->cache_mgr()->Open(
    CacheManager::Bless(dirent_->checksum(), object_info));
  if (fd < 0) {
    result = "Not in cache";
  } else {
    shash::Any hash(dirent_->checksum().algorithm);
    int retval_i =
      mount_point_->file_system()->cache_mgr()->ChecksumFd(fd, &hash);
    if (retval_i != 0)
      result = "I/O error (" + StringifyInt(retval_i) + ")";
    else
      result = hash.ToString();
    mount_point_->file_system()->cache_mgr()->Close(fd);
  }
  return result;
}

bool NCleanup24MagicXattr::PrepareValueFenced() {
  return true;
}

std::string NCleanup24MagicXattr::GetValue() {
  QuotaManager *quota_mgr =
    mount_point_->file_system()->cache_mgr()->quota_mgr();
  if (!quota_mgr->HasCapability(QuotaManager::kCapIntrospectCleanupRate)) {
    return StringifyInt(-1);
  } else {
    const uint64_t period_s = 24 * 60 * 60;
    const uint64_t rate = quota_mgr->GetCleanupRate(period_s);
    return StringifyInt(rate);
  }
}

bool NClgMagicXattr::PrepareValueFenced() {
  n_catalogs_ = mount_point_->catalog_mgr()->GetNumCatalogs();
  return true;
}

std::string NClgMagicXattr::GetValue() {
  return StringifyInt(n_catalogs_);
}

bool NDirOpenMagicXattr::PrepareValueFenced() {
  return true;
}

std::string NDirOpenMagicXattr::GetValue() {
  return mount_point_->file_system()->n_fs_dir_open()->ToString();
}

bool NDownloadMagicXattr::PrepareValueFenced() {
  return true;
}

std::string NDownloadMagicXattr::GetValue() {
  return  mount_point_->statistics()->Lookup("fetch.n_downloads")->Print();
}

bool NIOErrMagicXattr::PrepareValueFenced() {
  n_io_err_ = mount_point_->file_system()->n_io_error()->ToString();
  return true;
}

std::string NIOErrMagicXattr::GetValue() {
  return n_io_err_;
}

bool NOpenMagicXattr::PrepareValueFenced() {
  return true;
}

std::string NOpenMagicXattr::GetValue() {
  return mount_point_->file_system()->n_fs_open()->ToString();
}

bool ProxyMagicXattr::PrepareValueFenced() {
  return true;
}

std::string ProxyMagicXattr::GetValue() {
  vector< vector<download::DownloadManager::ProxyInfo> > proxy_chain;
  unsigned current_group;
  mount_point_->download_mgr()->GetProxyInfo(
    &proxy_chain, &current_group, NULL);
  if (proxy_chain.size()) {
    return proxy_chain[current_group][0].url;
  } else {
    return "DIRECT";
  }
}

bool PubkeysMagicXattr::PrepareValueFenced() {
  pubkeys_ = mount_point_->signature_mgr()->GetActivePubkeys();
  return true;
}

std::string PubkeysMagicXattr::GetValue() {
  return pubkeys_;
}

bool RawlinkMagicXattr::PrepareValueFenced() {
  return dirent_->IsLink();
}

std::string RawlinkMagicXattr::GetValue() {
  return dirent_->symlink().ToString();
}

bool RepoCountersMagicXattr::PrepareValueFenced() {
  counters_ = mount_point_->catalog_mgr()->GetRootCatalog()->GetCounters();
  return true;
}

std::string RepoCountersMagicXattr::GetValue() {
  return counters_.GetCsvMap();
}

uint64_t RepoMetainfoMagicXattr::kMaxMetainfoLength = 65536;

bool RepoMetainfoMagicXattr::PrepareValueFenced() {
  if (!mount_point_->catalog_mgr()->manifest()) {
    error_reason_ = "manifest not available";
    return true;
  }

  metainfo_hash_ = mount_point_->catalog_mgr()->manifest()->meta_info();
  if (metainfo_hash_.IsNull()) {
    error_reason_ = "metainfo not available";
    return true;
  }
  return true;
}

std::string RepoMetainfoMagicXattr::GetValue() {
  if (metainfo_hash_.IsNull()) {
    return error_reason_;
  }

  int fd = mount_point_->fetcher()->
            Fetch(metainfo_hash_, CacheManager::kSizeUnknown,
                  "metainfo (" + metainfo_hash_.ToString() + ")",
                  zlib::kZlibDefault, CacheManager::kTypeRegular, "");
  if (fd < 0) {
    return "Failed to open metadata file";
  }
  uint64_t actual_size = mount_point_->file_system()->cache_mgr()->GetSize(fd);
  if (actual_size > kMaxMetainfoLength) {
    mount_point_->file_system()->cache_mgr()->Close(fd);
    return "Failed to open: metadata file is too big";
  }
  char buffer[actual_size];
  int bytes_read =
    mount_point_->file_system()->cache_mgr()->Pread(fd, buffer, actual_size, 0);
  mount_point_->file_system()->cache_mgr()->Close(fd);
  if (bytes_read < 0) {
    return "Failed to read metadata file";
  }
  return string(buffer, buffer + bytes_read);
}

bool RevisionMagicXattr::PrepareValueFenced() {
  revision_ = mount_point_->catalog_mgr()->GetRevision();
  return true;
}

std::string RevisionMagicXattr::GetValue() {
  return StringifyUint(revision_);
}

bool RootHashMagicXattr::PrepareValueFenced() {
  root_hash_ = mount_point_->catalog_mgr()->GetRootHash();
  return true;
}

std::string RootHashMagicXattr::GetValue() {
  return root_hash_.ToString();
}

bool RxMagicXattr::PrepareValueFenced() {
  return true;
}

std::string RxMagicXattr::GetValue() {
  perf::Statistics *statistics = mount_point_->statistics();
  int64_t rx = statistics->Lookup("download.sz_transferred_bytes")->Get();
  return StringifyInt(rx/1024);
}

bool SpeedMagicXattr::PrepareValueFenced() {
  return true;
}

std::string SpeedMagicXattr::GetValue() {
  perf::Statistics *statistics = mount_point_->statistics();
  int64_t rx = statistics->Lookup("download.sz_transferred_bytes")->Get();
  int64_t time = statistics->Lookup("download.sz_transfer_time")->Get();
  if (time == 0)
    return "n/a";
  else
    return StringifyInt((1000 * (rx/1024))/time);
}

bool TagMagicXattr::PrepareValueFenced() {
  tag_ = mount_point_->repository_tag();
  return true;
}

std::string TagMagicXattr::GetValue() {
  return tag_;
}

bool TimeoutMagicXattr::PrepareValueFenced() {
  return true;
}

std::string TimeoutMagicXattr::GetValue() {
  unsigned seconds, seconds_direct;
  mount_point_->download_mgr()->GetTimeout(&seconds, &seconds_direct);
  return StringifyUint(seconds);
}

bool TimeoutDirectMagicXattr::PrepareValueFenced() {
  return true;
}

std::string TimeoutDirectMagicXattr::GetValue() {
  unsigned seconds, seconds_direct;
  mount_point_->download_mgr()->GetTimeout(&seconds, &seconds_direct);
  return StringifyUint(seconds_direct);
}

bool UsedFdMagicXattr::PrepareValueFenced() {
  n_used_fd_ = mount_point_->file_system()->no_open_files()->ToString();
  return true;
}

std::string UsedFdMagicXattr::GetValue() {
  return n_used_fd_;
}

// TODO: no_open_dirs_ counter is not incremented anywhere

bool UsedDirPMagicXattr::PrepareValueFenced() {
  n_used_dirp_ = mount_point_->file_system()->no_open_dirs()->ToString();
  return true;
}

std::string UsedDirPMagicXattr::GetValue() {
  return n_used_dirp_;
}

bool VersionMagicXattr::PrepareValueFenced() {
  return true;
}

std::string VersionMagicXattr::GetValue() {
  return std::string(VERSION) + "." + std::string(CVMFS_PATCH_LEVEL);
}
