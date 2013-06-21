/**
 * This file is part of the CernVM File System
 */

#include "cvmfs_config.h"
#include "swissknife_history.h"

#include <ctime>

#include "hash.h"
#include "util.h"
#include "manifest_fetch.h"
#include "download.h"
#include "signature.h"
#include "history.h"
#include "catalog_rw.h"
#include "upload.h"

using namespace std;  // NOLINT

/**
 * Checks if the given path looks like a remote path
 */
static bool IsRemote(const string &repository)
{
  return repository.substr(0, 7) == "http://";
}


/**
 * Retrieves a history db hash from manifest.
 */
static bool GetHistoryDbHash(const string &repository_url,
                             const string &repository_name,
                             const hash::Any &expected_root_hash,
                             hash::Any *historydb_hash)
{
  manifest::ManifestEnsemble manifest_ensemble;
  manifest::Manifest *manifest = NULL;
  if (IsRemote(repository_url)) {
    manifest::Failures retval;
    retval = manifest::Fetch(repository_url, repository_name, 0, NULL,
                             &manifest_ensemble);
    if (retval != manifest::kFailOk) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to fetch repository manifest (%d)",
               retval);
    }
    manifest = manifest_ensemble.manifest;
  } else {
    manifest = manifest::Manifest::LoadFile(repository_url + "/.cvmfspublished");
  }
  if (!manifest) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load repository manifest");
    return false;
  }

  // Compare base hash with hash in manifest (make sure we operate on the)
  // right history file
  if (expected_root_hash != manifest->catalog_hash()) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "wrong manifest, expected catalog %s, found catalog %s",
             expected_root_hash.ToString().c_str(),
             manifest->catalog_hash().ToString().c_str());
    return false;
  }

  *historydb_hash = manifest->history();
  return true;
}


static bool FetchTagList(const string &repository_url,
                         const hash::Any &history_hash,
                         const std::string tmp_path,
                         history::Database *history_db,
                         history::TagList *tag_list)
{
  int retval;
  if (!history_hash.IsNull()) {
    const string url = repository_url + "/data" +
      history_hash.MakePath(1, 2) + "H";
    download::JobInfo download_history(&url, true, false, &tmp_path,
                                       &history_hash);
    retval = download::Fetch(&download_history);
    if (retval != download::kFailOk) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to download history (%d)",
               retval);
      return false;
    }
  }
  retval = history_db->Open(tmp_path, sqlite::kDbOpenReadWrite);
  assert(retval);
  retval = tag_list->Load(history_db);
  assert(retval);
  return true;
}


int swissknife::CommandTag::Main(const swissknife::ArgumentList &args) {
  const string repository_url = MakeCanonicalPath(*args.find('r')->second);
  const string repository_name = *args.find('n')->second;
  const string repository_key_path = *args.find('k')->second;
  const string history_path = *args.find('o')->second;
  const hash::Any base_hash(hash::kSha1, hash::HexPtr(*args.find('b')->second));
  const hash::Any trunk_hash(hash::kSha1, hash::HexPtr(*args.find('t')->second));
  const unsigned trunk_revision = String2Uint64(*args.find('i')->second);
  hash::Any tag_hash = trunk_hash;
  string delete_tag;
  if (args.find('d') != args.end()) {
    delete_tag = *args.find('d')->second;
  }
  if (args.find('h') != args.end()) {
    tag_hash = hash::Any(hash::kSha1, hash::HexPtr(*args.find('h')->second));
  }
  history::Tag new_tag;
  if (args.find('a') != args.end()) {
    vector<string> fields = SplitString(*args.find('a')->second, '@');
    new_tag.name = fields[0];
    new_tag.root_hash = trunk_hash;  // might be changed later
    new_tag.revision = trunk_revision;  // might be changed later
    new_tag.timestamp = time(NULL);
    if (fields.size() > 1) {
      new_tag.channel =
        static_cast<history::UpdateChannel>(String2Uint64(fields[1]));
    }
    if (fields.size() > 2)
      new_tag.description = fields[2];
  }
  bool list_only = false;
  if (args.find('l') != args.end())
    list_only = true;
  hash::Any history_hash;
  history::Database tag_db;
  history::TagList tag_list;
  int retval;

  // Download & verify manifest
  signature::Init();
  retval = signature::LoadPublicRsaKeys(repository_key_path);
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load public repository key %s",
             repository_key_path.c_str());
    return 1;
  }
  download::Init(1, true);
  int result = 1;

  retval = GetHistoryDbHash(repository_url, repository_name, base_hash,
                            &history_hash);
  if (!retval)
    goto tag_fini;

  // Download history database / create new history database
  if (history_hash.IsNull()) {
    if (list_only) {
      LogCvmfs(kLogCvmfs, kLogStdout, "no history");
      result = 0;
      goto tag_fini;
    }
    retval = history::Database::Create(history_path, repository_name);
    if (!retval) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to create history database");
      goto tag_fini;
    }
  }
  retval = FetchTagList(repository_url, history_hash, history_path,
                        &tag_db, &tag_list);
  if (!retval)
    goto tag_fini;

  if (list_only) {
    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "%s",
             tag_list.List().c_str());
    result = 0;
    goto tag_fini;
  }

  // Add / Remove tag to history database
  if (delete_tag != "") {
    LogCvmfs(kLogHistory, kLogStdout, "Removing tag %s", delete_tag.c_str());
    tag_list.Remove(delete_tag);
  }
  if (new_tag.name != "") {
    if (tag_hash != trunk_hash) {
      history::Tag existing_tag;
      bool exists = tag_list.FindHash(tag_hash, &existing_tag);
      if (!exists) {
        LogCvmfs(kLogCvmfs, kLogStderr, "failed to find hash %s in tag list",
                 tag_hash.ToString().c_str());
        goto tag_fini;
      }
      tag_list.Remove(new_tag.name);
      new_tag.root_hash = tag_hash;
      new_tag.revision = existing_tag.revision;
    }
    retval = tag_list.Insert(new_tag);
    assert(retval == history::TagList::kFailOk);
  }

  // Update trunk, trunk-previous tag
  {
    history::Tag trunk_previous;
    bool trunk_found = tag_list.FindTag("trunk", &trunk_previous);
    tag_list.Remove("trunk-previous");
    tag_list.Remove("trunk");
    history::Tag tag_trunk(
      "trunk", trunk_hash, trunk_revision, time(NULL), history::kChannelTrunk,
      "latest published snapshot, automatically updated");
    retval = tag_list.Insert(tag_trunk);
    assert(retval == history::TagList::kFailOk);
    if (trunk_found) {
      trunk_previous.name = "trunk-previous";
      trunk_previous.description =
        "published next to trunk, automatically updated";
      retval = tag_list.Insert(trunk_previous);
      assert(retval == history::TagList::kFailOk);
    }
  }

  //LogCvmfs(kLogCvmfs, kLogStdout, "%s", tag_list.List().c_str());
  retval = tag_list.Store(&tag_db);
  assert(retval);
  result = 0;

 tag_fini:
  signature::Fini();
  download::Fini();
  return result;
}


int swissknife::CommandRollback::Main(const swissknife::ArgumentList &args) {
  const string spooler_definition = *args.find('r')->second;
  const upload::SpoolerDefinition sd(spooler_definition);
  upload::Spooler *spooler = upload::Spooler::Construct(sd);
  assert(spooler);

  const string repository_url = MakeCanonicalPath(*args.find('u')->second);
  const string repository_name = *args.find('n')->second;
  const string repository_key_path = *args.find('k')->second;
  const string history_path = *args.find('o')->second;
  const hash::Any base_hash(hash::kSha1, hash::HexPtr(*args.find('b')->second));
  const string target_tag_name = *args.find('t')->second;
  const string manifest_path = *args.find('m')->second;
  const string temp_dir = *args.find('d')->second;
  hash::Any history_hash;
  history::Database tag_db;
  history::TagList tag_list;
  history::Tag target_tag;
  history::Tag trunk_tag;
  string catalog_path;
  catalog::WritableCatalog *catalog = NULL;
  manifest::Manifest *manifest = NULL;
  hash::Any hash_republished_catalog(hash::kSha1);
  int retval;

  // Download & verify manifest & history database
  signature::Init();
  retval = signature::LoadPublicRsaKeys(repository_key_path);
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load public repository key %s",
             repository_key_path.c_str());
    return 1;
  }
  download::Init(1, true);
  int result = 1;

  retval = GetHistoryDbHash(repository_url, repository_name, base_hash,
                            &history_hash);
  if (!retval)
    goto rollback_fini;

  if (history_hash.IsNull()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "no history");
    goto rollback_fini;
  }
  retval = FetchTagList(repository_url, history_hash, history_path,
                        &tag_db, &tag_list);
  if (!retval)
    goto rollback_fini;

  // Verify rollback tag
  retval = tag_list.FindTag(target_tag_name, &target_tag);
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "tag %s does not exist",
             target_tag_name.c_str());
    goto rollback_fini;
  }
  retval = tag_list.FindTag("trunk", &trunk_tag);
  assert(retval);
  assert(trunk_tag.revision >= target_tag.revision);
  if (target_tag.revision == trunk_tag.revision) {
    LogCvmfs(kLogCvmfs, kLogStderr, "not rolling back to trunk revision (%u)",
             trunk_tag.revision);
    goto rollback_fini;
  }

  {  // Download rollback destination catalog
    FILE *f = CreateTempFile(temp_dir + "/cvmfs", 0600, "w", &catalog_path);
    assert(f);
    fclose(f);
    const string catalog_url = repository_url + "/data" +
      target_tag.root_hash.MakePath(1, 2) + "C";
    download::JobInfo download_catalog(&catalog_url, true, false, &catalog_path,
                                       &target_tag.root_hash);
    retval = download::Fetch(&download_catalog);
    if (retval != download::kFailOk) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to download catalog (%d)",
               retval);
      goto rollback_fini;
    }
  }

  // Update timestamp and revision
  catalog = catalog::AttachFreelyRw("", catalog_path, target_tag.root_hash);
  assert(catalog);
  catalog->UpdateLastModified();
  catalog->SetRevision(trunk_tag.revision + 1);

  // Upload catalog
  if (!zlib::CompressPath2Path(catalog->database_path(),
                               catalog->database_path() + ".compressed",
                               &hash_republished_catalog))
  {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to compress catalog (%d)");
    delete catalog;
    unlink(catalog_path.c_str());
    goto rollback_fini;
  }
  spooler->Upload(catalog->database_path() + ".compressed",
                  "data" + hash_republished_catalog.MakePath(1, 2) + "C");
  spooler->WaitForUpload();
  spooler->WaitForTermination();
  unlink((catalog->database_path() + ".compressed").c_str());
  manifest = new manifest::Manifest(hash_republished_catalog, "");
  manifest->set_ttl(catalog->GetTTL());
  manifest->set_revision(catalog->GetRevision());
  retval = manifest->Export(manifest_path);
  assert(retval);
  delete catalog;
  unlink(catalog_path.c_str());
  delete manifest;

  // Remove all entries including destination catalog from history
  tag_list.Rollback(target_tag.revision);

  // Add new trunk tag / updated named tag to history
  target_tag.revision = trunk_tag.revision + 1;
  target_tag.timestamp = time(NULL);
  target_tag.root_hash = hash_republished_catalog;
  trunk_tag.revision = target_tag.revision;
  trunk_tag.timestamp = target_tag.timestamp;
  trunk_tag.root_hash = target_tag.root_hash;
  if (target_tag.name != "trunk-previous")
    tag_list.Insert(target_tag);
  tag_list.Insert(trunk_tag);

  retval = tag_list.Store(&tag_db);
  assert(retval);
  LogCvmfs(kLogHistory, kLogStdout,
           "Previous trunk was %s, previous history database was %s",
           base_hash.ToString().c_str(), history_hash.ToString().c_str());
  result = 0;

 rollback_fini:
  delete spooler;
  signature::Fini();
  download::Fini();
  return result;
}
