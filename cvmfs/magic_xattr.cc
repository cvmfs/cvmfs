/**
 * This file is part of the CernVM File System.
 */

#include "magic_xattr.h"

#include <cassert>
#include <string>
#include <vector>

#include "catalog_mgr_client.h"
#include "fetch.h"
#include "logging.h"
#include "mountpoint.h"
#include "quota.h"
#include "signature.h"

MagicXattrManager::MagicXattrManager(MountPoint *mountpoint,
                                     bool hide_magic_xattrs)
  : mount_point_(mountpoint),
    hide_magic_xattrs_(hide_magic_xattrs)
{
  xattr_list_["user.catalog_counters"] = new CatalogCountersMagicXattr();
  xattr_list_["user.external_host"] = new ExternalHostMagicXattr();
  xattr_list_["user.external_timeout"] = new ExternalTimeoutMagicXattr();
  xattr_list_["user.fqrn"] = new FqrnMagicXattr();
  xattr_list_["user.host"] = new HostMagicXattr();
  xattr_list_["user.host_list"] = new HostListMagicXattr();
  xattr_list_["user.ncleanup24"] = new NCleanup24MagicXattr();
  xattr_list_["user.nclg"] = new NClgMagicXattr();
  xattr_list_["user.ndiropen"] = new NDirOpenMagicXattr();
  xattr_list_["user.ndownload"] = new NDownloadMagicXattr();
  xattr_list_["user.nioerr"] = new NIOErrMagicXattr();
  xattr_list_["user.nopen"] = new NOpenMagicXattr();
  xattr_list_["user.proxy"] = new ProxyMagicXattr();
  xattr_list_["user.pubkeys"] = new PubkeysMagicXattr();
  xattr_list_["user.repo_counters"] = new RepoCountersMagicXattr();
  xattr_list_["user.repo_metainfo"] = new RepoMetainfoMagicXattr();
  xattr_list_["user.revision"] = new RevisionMagicXattr();
  xattr_list_["user.root_hash"] = new RootHashMagicXattr();
  xattr_list_["user.rx"] = new RxMagicXattr();
  xattr_list_["user.speed"] = new SpeedMagicXattr();
  xattr_list_["user.tag"] = new TagMagicXattr();
  xattr_list_["user.timeout"] = new TimeoutMagicXattr();
  xattr_list_["user.timeout_direct"] = new TimeoutDirectMagicXattr();
  xattr_list_["user.usedfd"] = new UsedFdMagicXattr();
  xattr_list_["user.useddirp"] = new UsedDirPMagicXattr();
  xattr_list_["user.version"] = new VersionMagicXattr();

  xattr_list_["user.hash"] = new HashMagicXattr();
  xattr_list_["user.lhash"] = new LHashMagicXattr();

  xattr_list_["user.chunk_list"] = new ChunkListMagicXattr();
  xattr_list_["user.chunks"] = new ChunksMagicXattr();
  xattr_list_["user.compression"] = new CompressionMagicXattr();
  xattr_list_["user.external_file"] = new ExternalFileMagicXattr();

  xattr_list_["user.rawlink"] = new RawlinkMagicXattr();
  xattr_list_["xfsroot.rawlink"] = new RawlinkMagicXattr();

  xattr_list_["user.authz"] = new AuthzMagicXattr();
}

std::string MagicXattrManager::GetListString(catalog::DirectoryEntry *dirent) {
  if (hide_magic_xattrs()) {
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
        if (dirent->checksum().IsNull()) continue;
        break;
      case kXattrRegular:
        if (!dirent->IsRegular()) continue;
        break;
      case kXattrSymlink:
        if (!dirent->IsLink()) continue;
        break;
      case kXattrAuthz:
        if (!mount_point_->has_membership_req()) continue;
        break;
    }
    result += (*it).first;
    result.push_back('\0');
  }

  return result;
}

MagicXattrRAIIWrapper MagicXattrManager::Get(const std::string &name,
                                             PathString path,
                                             catalog::DirectoryEntry *d)
{
  BaseMagicXattr *result;
  if (xattr_list_.count(name) > 0) {
    result = xattr_list_[name];
  } else {
    return MagicXattrRAIIWrapper(NULL);
  }

  result->mount_point_ = mount_point_;
  result->Lock(path, d);
  return MagicXattrRAIIWrapper(result);
}

void MagicXattrManager::Register(const std::string &name,
                                 BaseMagicXattr *magic_xattr)
{
  if (xattr_list_.count(name) > 0) {
    PANIC(kLogSyslogErr,
          "Magic extended attribute with name %s already registered",
          name.c_str());
  }
  xattr_list_[name] = magic_xattr;
}

bool AuthzMagicXattr::PrepareValueFenced() {
  return mount_point_->has_membership_req();
}

std::string AuthzMagicXattr::GetValue() {
  return mount_point_->membership_req();
}

MagicXattrFlavor AuthzMagicXattr::GetXattrFlavor() {
  return kXattrAuthz;
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

bool ChunkListMagicXattr::PrepareValueFenced() {
  chunk_list_ = "hash,offset,size\n";
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
      for (size_t i = 0; i < chunks.size(); ++i) {
        chunk_list_ += chunks.At(i).content_hash().ToString() + ",";
        chunk_list_ += StringifyInt(chunks.At(i).offset()) + ",";
        chunk_list_ += StringifyUint(chunks.At(i).size()) + "\n";
      }
    }
  } else {
    chunk_list_ += dirent_->checksum().ToString() + ",";
    chunk_list_ += "0,";
    chunk_list_ += StringifyUint(dirent_->size()) + "\n";
  }
  return true;
}

std::string ChunkListMagicXattr::GetValue() {
  return chunk_list_;
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

std::string ExternalTimeoutMagicXattr::GetValue() {
  unsigned seconds, seconds_direct;
  mount_point_->download_mgr()->GetTimeout(&seconds, &seconds_direct);
  return StringifyUint(seconds_direct);
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

std::string NDirOpenMagicXattr::GetValue() {
  return mount_point_->file_system()->n_fs_dir_open()->ToString();
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

std::string NOpenMagicXattr::GetValue() {
  return mount_point_->file_system()->n_fs_open()->ToString();
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

MagicXattrFlavor RawlinkMagicXattr::GetXattrFlavor() {
  return kXattrSymlink;
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
  char buffer[kMaxMetainfoLength];
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

std::string RxMagicXattr::GetValue() {
  perf::Statistics *statistics = mount_point_->statistics();
  int64_t rx = statistics->Lookup("download.sz_transferred_bytes")->Get();
  return StringifyInt(rx/1024);
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

std::string TimeoutMagicXattr::GetValue() {
  unsigned seconds, seconds_direct;
  mount_point_->download_mgr()->GetTimeout(&seconds, &seconds_direct);
  return StringifyUint(seconds);
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

bool UsedDirPMagicXattr::PrepareValueFenced() {
  n_used_dirp_ = mount_point_->file_system()->no_open_dirs()->ToString();
  return true;
}

std::string UsedDirPMagicXattr::GetValue() {
  return n_used_dirp_;
}

std::string VersionMagicXattr::GetValue() {
  return std::string(VERSION) + "." + std::string(CVMFS_PATCH_LEVEL);
}
