/**
 * This file is part of the CernVM File System.
 */

#include "catalog_test_tools.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <sstream>

#include "catalog_rw.h"
#include "compression.h"
#include "directory_entry.h"
#include "hash.h"
#include "options.h"
#include "testutil.h"
#include "util/posix.h"

namespace {

void RemoveLeadingSlash(std::string* path) {
  if ((*path)[0] == '/') {
    path->erase(path->begin());
  }
}
void AddLeadingSlash(std::string* path) {
  if ((*path) != "" && (*path)[0] != '/') {
    path->insert(0, 1, '/');
  }
}

bool ExportDirSpec(const std::string& path,
                   catalog::WritableCatalogManager* mgr, DirSpec* spec) {
  catalog::DirectoryEntryList listing;
  if (!mgr->Listing(path, &listing)) {
    return false;
  }

  for (size_t i = 0u; i < listing.size(); ++i) {
    const catalog::DirectoryEntry& entry = listing[i];
    const std::string entry_full_path = entry.GetFullPath(path);
    XattrList xattrs;
    if (entry.HasXattrs()) {
      mgr->LookupXattrs(PathString(entry_full_path), &xattrs);
    }
    std::string path2 = path;
    RemoveLeadingSlash(&path2);
    spec->AddDirectoryEntry(entry, xattrs, path2);
    if (entry.IsDirectory()) {
      if (!ExportDirSpec(entry_full_path, mgr, spec)) {
        return false;
      }
    }
  }

  return true;
}

}  // namespace

DirSpec::DirSpec() : items_(), dirs_() {
  dirs_.insert("");
}

bool DirSpec::AddFile(const std::string& name, const std::string& parent,
                      const std::string& digest, const size_t size,
                      const XattrList& xattrs, shash::Suffix suffix) {
  shash::Any hash = shash::MkFromHexPtr(shash::HexPtr(digest), suffix);
  if (!HasDir(parent)) {
    return false;
  }

  const catalog::DirectoryEntry entry =
    catalog::DirectoryEntryTestFactory::RegularFile(name, size, hash);
  const std::string full_path = entry.GetFullPath(parent);
  items_.insert(std::make_pair(full_path,
                               DirSpecItem(entry, xattrs, parent)));
  return true;
}

bool DirSpec::LinkFile(const std::string& name, const std::string& parent,
                         const std::string& symlink, const size_t size,
                         const XattrList& xattrs, shash::Suffix suffix) {
  const catalog::DirectoryEntry entry =
    catalog::DirectoryEntryTestFactory::Symlink(name, size, symlink);
  const std::string full_path = entry.GetFullPath(parent);
  items_.insert(std::make_pair(full_path,
                               DirSpecItem(entry, xattrs, parent)));
  return true;
}

bool DirSpec::AddDirectory(const std::string& name, const std::string& parent,
                           const size_t size) {
  if (!HasDir(parent)) {
    return false;
  }

  bool ret = AddDir(name, parent);
  const catalog::DirectoryEntry entry =
    catalog::DirectoryEntryTestFactory::Directory(name, size);
  const std::string full_path = entry.GetFullPath(parent);
  items_.insert(std::make_pair(full_path,
                               DirSpecItem(entry, XattrList(), parent)));
  return ret;
}

bool DirSpec::AddNestedCatalog(const std::string& name) {
  bool ret = AddNC(name);
  if (!ret) return ret;
  nested_catalogs_.push_back(name);
  AddFile(".cvmfscatalog", name, "0000000000000000000000000000000000000001", 0);
  return ret;
}

bool DirSpec::AddDirectoryEntry(const catalog::DirectoryEntry& entry,
                                const XattrList& xattrs,
                                const std::string& parent) {
  if (!HasDir(parent)) {
    return false;
  }

  bool ret = true;
  if (entry.IsDirectory()) {
    ret = AddDir(std::string(entry.name().c_str()), parent);
  }

  const std::string full_path = entry.GetFullPath(parent);
  items_.insert(std::make_pair(full_path, DirSpecItem(entry, xattrs, parent)));
  return ret;
}

void DirSpec::ToString(std::string* out) {
  std::ostringstream ostr;
  for (DirSpec::ItemList::const_iterator it = items_.begin();
       it != items_.end(); ++it) {
    const DirSpecItem& item = it->second;
    if (item.entry_base().IsRegular()) {
      ostr << "F ";
    } else if (item.entry_base().IsDirectory()) {
      ostr << "D ";
    } else if (item.entry_base().IsLink()) {
      ostr << "S ";
    }
    std::string parent = item.parent();
    AddLeadingSlash(&parent);

    ostr << item.entry_base().GetFullPath(parent).c_str();
    if (item.entry_base().IsLink()) {
      ostr << " -> " << item.entry_base().symlink().c_str();
    }

    ostr << std::endl;
  }
  *out = ostr.str();
}

const DirSpecItem* DirSpec::Item(const std::string& full_path) const {
  ItemList::const_iterator it = items_.find(full_path);
  if (it != items_.end()) {
    return &it->second;
  }
  std::string no_slash(full_path);
  RemoveLeadingSlash(&no_slash);
  if (no_slash != full_path) {
    return Item(full_path);
  }
  return NULL;
}

static void RemoveItemHelper(
  const DirSpec& spec,
  const std::string& full_path,
  std::vector<std::string>* acc
) {
  DirSpec::ItemList::const_iterator it = spec.items().find(full_path);
  if (it != spec.items().end()) {
    const DirSpecItem item = it->second;
    acc->push_back(full_path);

    if (item.entry_base().IsDirectory()) {
      std::string rel_full_path(full_path);
      RemoveLeadingSlash(&rel_full_path);
      for (DirSpec::ItemList::const_iterator it = spec.items().begin();
           it != spec.items().end(); ++it) {
        if (it->second.parent() == rel_full_path) {
          const std::string p =
            it->second.entry_base().GetFullPath(rel_full_path);
          RemoveItemHelper(spec, p, acc);
        }
      }
    }
  }
}

void DirSpec::RemoveItemRec(const std::string& full_path) {
  std::string path(full_path);
  RemoveLeadingSlash(&path);
  std::vector<std::string> acc(0);
  RemoveItemHelper(*this, path, &acc);

  for (size_t i = 0u; i < acc.size(); ++i) {
    const DirSpecItem* item = Item(acc[i]);
    if (item->entry_base().IsDirectory()) {
      RmDir(std::string(item->entry_base().name().c_str()), item->parent());
    }
    items_.erase(acc[i]);

    DirSpec::NestedCatalogList::iterator n;
    for (n = nested_catalogs_.begin(); n != nested_catalogs_.end();) {
      if (*n == acc[i]) {
        n = nested_catalogs_.erase(n);
      } else {
        ++n;
      }
    }
  }
}

std::vector<std::string> DirSpec::GetDirs() const {
  std::vector<std::string> out;
  std::copy(dirs_.begin(), dirs_.end(), std::back_inserter(out));

  return out;
}

bool DirSpec::AddDir(const std::string& name, const std::string& parent) {
  std::string full_path = parent + "/" + name;
  RemoveLeadingSlash(&full_path);
  if (HasDir(full_path)) {
    return false;
  }
  dirs_.insert(full_path);
  return true;
}

bool DirSpec::RmDir(const std::string& name, const std::string& parent) {
  std::string full_path = parent + "/" + name;
  RemoveLeadingSlash(&full_path);
  if (!HasDir(full_path)) {
    return false;
  }
  dirs_.erase(full_path);
  return true;
}

bool DirSpec::AddNC(const std::string& name) {
  std::string full_path = name;
  RemoveLeadingSlash(&full_path);
  if (!HasDir(full_path)) {
    return false;
  }
  return true;
}


bool DirSpec::HasDir(const std::string& name) const {
  return dirs_.find(name) != dirs_.end();
}

CatalogTestTool::CatalogTestTool(const std::string& name)
    : name_(name), manifest_(), spooler_(), history_() {}

bool CatalogTestTool::Init() {
  if (!InitDownloadManager(true)) {
    return false;
  }

  const std::string sandbox_root = GetCurrentWorkingDirectory();

  stratum0_ = sandbox_root + "/" + name_;
  MkdirDeep(stratum0_ + "/data", 0777);
  MakeCacheDirectories(stratum0_ + "/data", 0777);
  temp_dir_ = stratum0_ + "/data/txn";

  shash::Any hash_cert(shash::kSha1);
  CreateKeys(stratum0_, &public_key_, &hash_cert);
  CreateWhitelist(stratum0_);

  spooler_ = CreateSpooler("local," + temp_dir_ + "," + stratum0_);
  if (!spooler_.IsValid()) {
    return false;
  }

  manifest_ = CreateRepository(temp_dir_, spooler_);

  if (!manifest_.IsValid()) {
    return false;
  }

  shash::Any history_hash(shash::kSha1);
  CreateHistory(stratum0_, manifest_, &history_hash);

  manifest_->set_certificate(hash_cert);
  manifest_->set_history(history_hash);
  manifest_->set_repository_name("keys.cern.ch");
  manifest_->set_publish_timestamp(time(NULL));
  CreateManifest(stratum0_, manifest_);


  history_.clear();
  history_.push_back(std::make_pair("initial", manifest_->catalog_hash()));

  return true;
}

void CatalogTestTool::UpdateManifest() {
  CreateManifest(stratum0_, manifest_);
}

// Note: we always apply the dir spec to the revision corresponding to the
// original,
//       empty repository.
bool CatalogTestTool::Apply(const std::string& id, const DirSpec& spec) {
  statistics_ = new perf::Statistics();
  catalog_mgr_ =
    CreateCatalogMgr(history_.front().second, "file://" + stratum0_,
                     temp_dir_, spooler_, download_manager(),
                     statistics_.weak_ref());
  if (!catalog_mgr_.IsValid()) {
    return false;
  }

  for (DirSpec::ItemList::const_iterator it = spec.items().begin();
       it != spec.items().end(); ++it) {
    const DirSpecItem& item = it->second;
    if (item.entry_.IsRegular() || item.entry_.IsLink()) {
      catalog_mgr_->AddFile(item.entry_base(), item.xattrs(), item.parent());
    } else if (item.entry_.IsDirectory()) {
      catalog_mgr_->AddDirectory(
        item.entry_base(), item.xattrs(), item.parent());
    }
  }

  DirSpec::NestedCatalogList::const_iterator it;
  for (it = spec.nested_catalogs().begin();
       it != spec.nested_catalogs().end(); ++it) {
    catalog_mgr_->CreateNestedCatalog(*it);
  }


  if (!catalog_mgr_->Commit(false, 0, manifest_)) {
    return false;
  }

  history_.push_back(std::make_pair(id, manifest_->catalog_hash()));

  return true;
}

bool CatalogTestTool::ApplyAtRootHash(
  const shash::Any& root_hash,
  const DirSpec& spec
) {
  statistics_ = new perf::Statistics();
  catalog_mgr_ =
    CreateCatalogMgr(root_hash, "file://" + stratum0_, temp_dir_, spooler_,
                     download_manager(), statistics_.weak_ref());
  if (!catalog_mgr_.IsValid()) {
    return false;
  }

  for (DirSpec::ItemList::const_iterator it = spec.items().begin();
       it != spec.items().end(); ++it) {
    const DirSpecItem& item = it->second;
    if (item.entry_.IsRegular() || item.entry_.IsLink()) {
      catalog_mgr_->AddFile(item.entry_base(), item.xattrs(), item.parent());
    } else if (item.entry_.IsDirectory()) {
      catalog_mgr_->AddDirectory(
        item.entry_base(), item.xattrs(), item.parent());
    }
  }

  DirSpec::NestedCatalogList::const_iterator it;
  for (it = spec.nested_catalogs().begin();
       it != spec.nested_catalogs().end(); ++it) {
    catalog_mgr_->CreateNestedCatalog(*it);
  }

  if (!catalog_mgr_->Commit(false, 0, manifest_)) {
    return false;
  }

  return true;
}

bool CatalogTestTool::LookupNestedCatalogHash(
  const shash::Any& root_hash,
  const std::string& path,
  char **nc_hash
) {
  perf::Statistics stats;
  UniquePtr<catalog::WritableCatalogManager> catalog_mgr(
      CreateCatalogMgr(root_hash, "file://" + stratum0_, temp_dir_, spooler_,
                       download_manager(), &stats));
  if (!catalog_mgr.IsValid()) {
    return false;
  }

  PathString p;
  p.Assign(&path[0], path.length());
  p.Append("/.cvmfscatalog", 14);

  catalog::DirectoryEntry entry;
  // This lookup is used to ensure the needed catalogs are mounted
  catalog_mgr->LookupPath(path, catalog::kLookupSole, &entry);

  p.Assign(&path[0], path.length());
  shash::Any hash = catalog_mgr->GetNestedCatalogHash(p);

  // If the hash is null don't try to strdup
  if (!hash.IsNull()) {
    *nc_hash = strdup(hash.ToString().c_str());
  }
  if (!(*nc_hash)) {
    LogCvmfs(kLogCatalog, kLogStderr,
             "nested catalog for directory '%s' cannot be found",
             path.c_str());
    return false;
  }

  return true;
}

bool CatalogTestTool::FindEntry(
  const shash::Any& root_hash,
  const std::string& path,
  catalog::DirectoryEntry *entry
) {
  perf::Statistics stats;
  UniquePtr<catalog::WritableCatalogManager> catalog_mgr(
      CreateCatalogMgr(root_hash, "file://" + stratum0_, temp_dir_, spooler_,
                       download_manager(), &stats));
  if (!catalog_mgr.IsValid()) {
    return false;
  }

  if (!catalog_mgr->LookupPath(path, catalog::kLookupSole, entry)) {
    LogCvmfs(kLogCatalog, kLogStderr,
             "catalog for directory '%s' cannot be found",
             path.c_str());
    return false;
  }

  return true;
}

bool CatalogTestTool::DirSpecAtRootHash(const shash::Any& root_hash,
                                        DirSpec* spec) {
  perf::Statistics stats;
  UniquePtr<catalog::WritableCatalogManager> catalog_mgr(
      CreateCatalogMgr(root_hash, "file://" + stratum0_, temp_dir_, spooler_,
                       download_manager(), &stats));

  if (!catalog_mgr.IsValid()) {
    return false;
  }

  return ExportDirSpec("", catalog_mgr.weak_ref(), spec);
}

CatalogTestTool::~CatalogTestTool() {}

upload::Spooler* CatalogTestTool::CreateSpooler(const std::string& config) {
  upload::SpoolerDefinition definition(config, shash::kSha1, zlib::kZlibDefault,
                                       false, true, 4194304, 8388608, 16777216,
                                       "dummy_token", "dummy_key");
  return upload::Spooler::Construct(definition);
}

manifest::Manifest* CatalogTestTool::CreateRepository(
    const std::string& dir, upload::Spooler* spooler) {
  manifest::Manifest* manifest =
      catalog::WritableCatalogManager::CreateRepository(dir, false, "",
                                                        spooler);
  if (spooler->GetNumberOfErrors() > 0) {
    return NULL;
  }

  return manifest;
}

catalog::WritableCatalogManager* CatalogTestTool::CreateCatalogMgr(
    const shash::Any& root_hash, const std::string stratum0,
    const std::string& temp_dir, upload::Spooler* spooler,
    download::DownloadManager* dl_mgr, perf::Statistics* stats) {
  catalog::WritableCatalogManager* catalog_mgr =
      new catalog::WritableCatalogManager(root_hash, stratum0, temp_dir,
                                          spooler, dl_mgr, false, 0, 0, 0,
                                          stats, false, 0, 0);
  catalog_mgr->Init();

  return catalog_mgr;
}

void CatalogTestTool::CreateHistory(
  string repo_path_,
  manifest::Manifest *manifest,
  shash::Any *history_hash
) {
  const string history_path = CreateTempPath(repo_path_ + "/history", 0600);
  {
    UniquePtr<history::SqliteHistory> history(
      history::SqliteHistory::Create(history_path,
                                     "keys.cern.ch"));
    ASSERT_TRUE(history.IsValid());
    history::History::Tag tag;
    tag.name = "snapshot";
    tag.root_hash = manifest->catalog_hash();
    tag.timestamp = time(NULL);
    ASSERT_TRUE(history->Insert(tag));
  }
  history_hash->suffix = shash::kSuffixHistory;
  ASSERT_TRUE(zlib::CompressPath2Null(history_path, history_hash));
  ASSERT_TRUE(
    zlib::CompressPath2Path(
      history_path,
      repo_path_ + "/data/" + history_hash->MakePath()));
}


void CatalogTestTool::CreateManifest(
  string repo_path_,
  manifest::Manifest *manifest
) {
  UniquePtr<signature::SignatureManager> signature_mgr(
    new signature::SignatureManager());
  signature_mgr->Init();
  ASSERT_TRUE(
    signature_mgr->LoadCertificatePath(repo_path_ + "/testrepo.crt"));
  ASSERT_TRUE(
    signature_mgr->LoadPrivateKeyPath(repo_path_ + "/testrepo.key", ""));
  ASSERT_TRUE(signature_mgr->KeysMatch());

  string signed_manifest = manifest->ExportString();
  shash::Any published_hash(manifest->GetHashAlgorithm());
  shash::HashMem(
    reinterpret_cast<const unsigned char *>(signed_manifest.data()),
    signed_manifest.length(), &published_hash);
  signed_manifest += "--\n" + published_hash.ToString() + "\n";
  unsigned char *sig;
  unsigned sig_size;
  ASSERT_TRUE(
    signature_mgr->Sign(reinterpret_cast<const unsigned char *>(
                        published_hash.ToString().data()),
                        published_hash.GetHexSize(),
                        &sig, &sig_size));
  signed_manifest += string(reinterpret_cast<char *>(sig), sig_size);
  free(sig);
  ASSERT_TRUE(
    SafeWriteToFile(signed_manifest, repo_path_ + "/.cvmfspublished", 0600));
  signature_mgr->Fini();
}


void CatalogTestTool::CreateWhitelist(string repo_path_) {
  // valid for 128 years as of 2016
  string whitelist_b64 =
    "MjAxNjA2MTcxNjA1MzkKRTIxNDQwNjE3MTYwNTM5Ck5rZXlzLmNlcm4uY2gK"
    "MDA6N0M6RkE6RUU6MUE6MkI6OTg6NzQ6NUQ6MTQ6QTY6MjU6NEU6QzQ6NDA6"
    "QkM6QkQ6NDQ6NDc6QTMKLS0KNWJlYWQ2MzBjMzAxZjdjZTc3MmY2NGJlOWVm"
    "MDFlMjhkNmFhY2E2NgpkM7pmYusuNTNC7XBhQzlAy4wMo1sqrv9EnjcCI6xg"
    "B8YHEvILtHMpB4qK1NyI2We9zuXgRe5MVwDkIEGMvRedCgiStPMD3aCFT730"
    "yv/b5qltYuwlwnjdezOwSAvj6BJ9ITSaW6wT1IA5BtqhBv0I8cloWvV0CfyI"
    "m+pnebb/yyu8hIsOH0SdRhZsFx23Eml50FrzvwaavbDVQHtU46YbqlqgGwFy"
    "QJE0X7lljrbtAjJHOAxurnDyhENnna6tWxwedpMOYEwwEoqF20plHqawSZbL"
    "oDjuHCEu2TrGkj+CguUT/XPSbTLMVjg+yMsi23e+a+P9ipOhOaEL4mk/LqPx";
  string wl;
  ASSERT_TRUE(Debase64(whitelist_b64, &wl));
  EXPECT_TRUE(SafeWriteToFile(wl, repo_path_ + "/.cvmfswhitelist", 0600));
}

void CatalogTestTool::CreateKeys(
  string repo_path_,
  string *public_key,
  shash::Any *hash_cert
) {
  // Key material for a repo named "keys.cern.ch"
  string pubkey = "-----BEGIN PUBLIC KEY-----\n"
    "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA6eJmVLlzDanGoZjqDf/M\n"
    "tcrds7mrHhRSBWLHzqucsPVLi8+zl7WRfjtb+SEe4xvSkd3mdKKPzew4s7tOic5m\n"
    "D9sl9wKpU6AfMpTfuOEZvcWDFh5lsAeNldE+LViHCibHoj2WIEAI+HZkoNAFlg+c\n"
    "ZDyKXxg+Xk2ZPwjLKGX6rwWEvlDebj0q57mI8nZ8tXogu51FFy3fcTndm+DWt+D5\n"
    "7GN1fvFEHbvncrqKSzbgnTVgTwMueoRK5H3I6HuW+DfAUADsQgLbm5PpcgZZNga5\n"
    "qNJQyg+ozaX/3SfWaE2z5sweU1wMNll3fjs3CwIRQPLY0d5g/187z6T1mpiuz6hm\n"
    "IQIDAQAB\n"
    "-----END PUBLIC KEY-----\n";
  string masterkey = "-----BEGIN RSA PRIVATE KEY-----\n"
    "MIIEogIBAAKCAQEA6eJmVLlzDanGoZjqDf/Mtcrds7mrHhRSBWLHzqucsPVLi8+z\n"
    "l7WRfjtb+SEe4xvSkd3mdKKPzew4s7tOic5mD9sl9wKpU6AfMpTfuOEZvcWDFh5l\n"
    "sAeNldE+LViHCibHoj2WIEAI+HZkoNAFlg+cZDyKXxg+Xk2ZPwjLKGX6rwWEvlDe\n"
    "bj0q57mI8nZ8tXogu51FFy3fcTndm+DWt+D57GN1fvFEHbvncrqKSzbgnTVgTwMu\n"
    "eoRK5H3I6HuW+DfAUADsQgLbm5PpcgZZNga5qNJQyg+ozaX/3SfWaE2z5sweU1wM\n"
    "Nll3fjs3CwIRQPLY0d5g/187z6T1mpiuz6hmIQIDAQABAoIBADnFTnmHBUBOu12X\n"
    "I9kpYitVXMXUCsx3QHtMFwaZpS6gqHR0bWv/0VxY1TMIV1TJvo2BPjd5IARBYRAk\n"
    "KBYqAVPRUeNdqO2bE5mu5EQKdg1GCEciYwPEGdjzwmP5BgIf6hfNFpQIvS6CMAD4\n"
    "4Shb2sl3msY6es1YZY4IYgYsimtIfMmVPv0awRX9xJ0cQWZP5Feo09jguY02xPim\n"
    "7JzkGBKazaKFKw7tHoNwfWt312oSdXjmWicUbdDljyrM8olLYwgpoz3ngwYSdKDZ\n"
    "Lcw8b1BXbMj2UERZQMCzIR7j340mp/cUeNSEDqErKwSm+LjPUfTj6XKO1hIvO1kv\n"
    "u1ZS7rECgYEA93BOxUcCa03dYzK+wLrnbVB9SeMXLWOGxJrxF7wid9Ju4WSilWsi\n"
    "BLUzAOSkTVZ7PKIBf2HTnTRYt+B+KuVkb+mnU6/6I/zgp/XsDXY+7sH4bAvBZZ3M\n"
    "Ry/O05lk3sFOKu8rWE1yn3k2XDI1CcDNK5dQ4X0AOvKJynW2c6bIRbsCgYEA8foI\n"
    "y1+gfm9OaUL2MTdXo+16gmtdscUTZzK4oSEN8YRe+PnwV3cS34FjVxy7ZMCX3IWp\n"
    "5gs9KFJof0gxlnu86oAVux2pT7oxiWsvQVpIBWbnGKvJJp20EPPhfX6Ox5fjdcye\n"
    "xVpeyseReL0QH3p0Ej8T4bU5m9OY1d4M+/83d9MCgYBTtyafbjfuUAjQEBIjqNi1\n"
    "zl6lSfTEgYDOMdHSAu/ydDrZfS/Yt8dpqliYO8Mu+0x0pic1jsaG0HgXthdZsgS6\n"
    "LGZVVRufY2YqzXRQ1anTI8NF4vBKzgmYKB+kzagoCWTF9+dFV+ao99yhcscpBpcj\n"
    "4W0W7TDPwNFHs23IUSw/EwKBgCBYA4Trq1A7IIgBY1cAxr4qqA12vHdemFFa/kLL\n"
    "YEnAH9G31uBaEjO938FtHb9B3wqi8yrEpdAV89HPnJE4yO+vXzg7pr35bVWo9hAO\n"
    "OUI/lvQ9Qg3fVopNjv5vRDZ5nvXH/BD1G2aPdmplGxqaC5nExKuOxbyGdA9iNuoY\n"
    "GxnxAoGAPuh+WTXKGsEychIHlP1XlFdG7TB+18WbsS3RRIlAJ3VKka57NVt6xwhb\n"
    "G4CaGObEcJYETvbx+H7qpQIPE9ERIZj1PDTSMN6a8q7JEO8+ECA+RpWqgk43hbQK\n"
    "v1d1Bc3aLfXayoHjTSFYZ9v3X0ZNLDNH9nIosAY388c5C0lIsco=\n"
    "-----END RSA PRIVATE KEY-----\n";
  string certificate = "-----BEGIN CERTIFICATE-----\n"
    "MIIC4DCCAcgCCQCbl8VGkUwDtDANBgkqhkiG9w0BAQsFADAyMTAwLgYDVQQDDCdr\n"
    "ZXlzLmNlcm4uY2ggQ2VyblZNLUZTIFJlbGVhc2UgTWFuYWdlcnMwHhcNMTYwNjE3\n"
    "MTUyMzQwWhcNMTcwNjE3MTUyMzQwWjAyMTAwLgYDVQQDDCdrZXlzLmNlcm4uY2gg\n"
    "Q2VyblZNLUZTIFJlbGVhc2UgTWFuYWdlcnMwggEiMA0GCSqGSIb3DQEBAQUAA4IB\n"
    "DwAwggEKAoIBAQCuSd7oeR4t8KbWCBzGysKks324Dap/gyQKE8KHSrDnnFo8WBTh\n"
    "sIiVZmZbW5pKZE/qpW7Muz0DVrgjRdwhdJUO5DuUGLH7eX7n2a1rNoC76RSd0SR1\n"
    "vejzFwO2+9laQuXFPWbzL1Ja4FHDZmLNrHntqPHKiLUw/7q8fpSMYTHA6kJC98fk\n"
    "Ck6riiTjEA/Gob22tXNqozyM8uKAZ4hSbN85odQb/Zsn5vgj0ZFcXKYV8wtMc4He\n"
    "5Onz0sSTqUbgnRMqIkdA3l67aPAICiAvLCwxfZD1sgwe0dKm/1ou9pakSWbLntZa\n"
    "8YsTE4un3aWZGqJGCp2+b+QAZuQb/5MdzWw9AgMBAAEwDQYJKoZIhvcNAQELBQAD\n"
    "ggEBAFZL6rdEp/89Th68KQgdVVx9USiSjIpJzUYEMN0psBqoLmcF35bd784K8iPg\n"
    "dRwfHKU0LvKAABl1od8nKNSuFuQ70kL2nY0fZQ0cTt14MPcot1PVxARmyL9delzk\n"
    "VVbGVkBn7u3nptIKm4CU8aAft4KBBhbGuPhfXLkRGDGrZv0IG0KCYXPfGnYzl3rF\n"
    "ugCiRjoqZcvUVQg1l2J2yuHhZ12iGaLHPpccmWZvpRVzpaS+XbDjPCAn75DCOaqR\n"
    "dtXFO0AqtWj+4jXvKQ6RoZAU0opX3K7h5qrYeh2lkI9XlyxKD7lBmIZmNf7brXpW\n"
    "nFvQm3OjNT9ZRG9T712hiQ/chdc=\n"
    "-----END CERTIFICATE-----\n";
  string key = "-----BEGIN RSA PRIVATE KEY-----\n"
    "MIIEpgIBAAKCAQEArkne6HkeLfCm1ggcxsrCpLN9uA2qf4MkChPCh0qw55xaPFgU\n"
    "4bCIlWZmW1uaSmRP6qVuzLs9A1a4I0XcIXSVDuQ7lBix+3l+59mtazaAu+kUndEk\n"
    "db3o8xcDtvvZWkLlxT1m8y9SWuBRw2Zizax57ajxyoi1MP+6vH6UjGExwOpCQvfH\n"
    "5ApOq4ok4xAPxqG9trVzaqM8jPLigGeIUmzfOaHUG/2bJ+b4I9GRXFymFfMLTHOB\n"
    "3uTp89LEk6lG4J0TKiJHQN5eu2jwCAogLywsMX2Q9bIMHtHSpv9aLvaWpElmy57W\n"
    "WvGLExOLp92lmRqiRgqdvm/kAGbkG/+THc1sPQIDAQABAoIBAQCQRlBC6vgjmWHS\n"
    "LVb87J2hz3+Tm6R2960etmrCqf61S8WazGNEzGjUG7dBixu210EcgaOt0JVaLTAy\n"
    "6sKl4yb888up9aNoA5QdAyG+bZi1dOV/GsDuwq2ShYuqruKnCFfCJekSCCtJVQX6\n"
    "FchWb59jMAYv3Wj4Tclb/gCkEFUqVqORcEXzwOvqcFXqBagenTKXEqotGcjsSTSY\n"
    "VAbQJFlJdJ7CayOpG9uJ33AxnaCSgbYr2bTWUJ1FqtqqCqO0PZgpg0emEyxGZqE/\n"
    "UlTytFNianHhbGwrzsoXCN1Q3RK5ExN3/RznH8zw7rCXOdfmUNaUiNm9nmUX4sTh\n"
    "3ieO+/7hAoGBANZMwh5lNf3FZluF68g5al3x36gRs/PZDEQfDhPqIyYXV9j8uIuq\n"
    "/Uiv/5xJQIbPFqF25ffroCYqO/mDJpU1FY/hWnW/3cCi1H41xiG9DUOqOx2gqE+6\n"
    "kzBlehTiunNIhmSglK9Ev5LTMXXm7Z/fCE0s1WOTLpO1d/OU1yi/hp4JAoGBANAz\n"
    "+FlKLDvRgNXG/vs7q0pmnkRWtlsvm8Y0WROYdPDSTj7oWk8VBp43RL3zBKGNkPYR\n"
    "3Bz6j89XrSsRsYnwMPkEnYN1OGunaylTmI7zG6gZVyoog8CDxW1RBHITafbjQPl2\n"
    "LKqi8JJL16PJEh17Bl2y99zQiRG155a4qab1BCmVAoGBAMt0HGfXFydTHhaOUofJ\n"
    "Wt7OH9Tk2cAMtMSH50mo5K3pQ5HSfTK8p7M2xKqQMR7LxWSOCU8S+PzC5CXDCgJm\n"
    "X442GTfpbJLTBIK+ctjdL5aqK225dZIcRFmSPhFOIE4K8OzgN8ker/KpZy/Uio1Z\n"
    "pfv/MKhUt8esZbFwAcXB8ABhAoGBALWkIY8EvwKRDK11Jw9YR2BplrpYTE/RgT2y\n"
    "feQyphNT5x/K5r8HwPZXkYmGcwvezhFgE4DUuJJUE6f3j8Sf4JngBOujYM3LChrL\n"
    "69ULE53cPcdyAT/7tkpg3FgJx/C04wLArsdP0EJSGJez3DIMGsm0Ubo71Nm2sY01\n"
    "Hg2ixTbhAoGBAJ6dUGDa5d+CtsQ7CwEgwcaB7+gk8qn7iKCCmeYe7Ja7nYqT97gC\n"
    "WSTDqjRX/AE7vQ3UysvJS9yzRakZiINv03rZXmZv3ft5uDzbqPhBjsX9Nnw3Emua\n"
    "OO+VjREFgdl5q7TLvm1ERXRTHdIWXv9zwC1ybtZUbuGQ42WKWfGwsNHN\n"
    "-----END RSA PRIVATE KEY-----\n";
  ASSERT_TRUE(SafeWriteToFile(pubkey, repo_path_ + "/testrepo.pub", 0600));
  ASSERT_TRUE(SafeWriteToFile(masterkey,
                              repo_path_ + "/testrepo.masterkey", 0600));
  ASSERT_TRUE(SafeWriteToFile(certificate,
                              repo_path_ + "/testrepo.crt", 0600));
  ASSERT_TRUE(SafeWriteToFile(key, repo_path_ + "/testrepo.key", 0600));

  hash_cert->suffix = shash::kSuffixCertificate;
  ASSERT_TRUE(
    zlib::CompressPath2Null(repo_path_ + "/testrepo.crt", hash_cert));
  ASSERT_TRUE(
    zlib::CompressPath2Path(repo_path_ + "/testrepo.crt",
                            repo_path_ + "/data/" + hash_cert->MakePath()));

  *public_key = repo_path_ + string("/testrepo.pub");
}

void CreateMiniRepository(
  SimpleOptionsParser *options_mgr_,
  string *repo_path_
) {
  CatalogTestTool tester(*repo_path_);
  EXPECT_TRUE(tester.Init());
  *repo_path_ = tester.repo_name();
  options_mgr_->SetValue("CVMFS_ROOT_HASH",
                        tester.manifest()->catalog_hash().ToString());
  options_mgr_->SetValue("CVMFS_SERVER_URL", "file://" + *repo_path_);
  options_mgr_->SetValue("CVMFS_HTTP_PROXY", "DIRECT");
  options_mgr_->SetValue("CVMFS_PUBLIC_KEY", tester.public_key());
}
