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

using namespace std;  // NOLINT

/**
 * Checks if the given path looks like a remote path
 */
static bool IsRemote(const string &repository) {
  return repository.substr(0, 7) == "http://";
}


int swissknife::CommandTag::Main(const swissknife::ArgumentList &args) {
  const string repository_url = MakeCanonicalPath(*args.find('r')->second);
  const string repository_name = *args.find('n')->second;
  const string repository_key_path = *args.find('k')->second;
  const string history_path = *args.find('o')->second;
  const hash::Any base_hash(hash::kSha1, hash::HexPtr(*args.find('b')->second));
  const hash::Any trunk_hash(hash::kSha1, hash::HexPtr(*args.find('t')->second));
  const unsigned trunk_revision = String2Uint64(*args.find('i')->second);
  string delete_tag;
  if (args.find('d') != args.end()) {
    delete_tag = *args.find('d')->second;
  }
  history::Tag new_tag;
  if (args.find('a') != args.end()) {
    vector<string> fields = SplitString(*args.find('a')->second, '@');
    new_tag.name = fields[0];
    new_tag.root_hash = trunk_hash;
    new_tag.revision = trunk_revision;
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
    goto tag_fini;
  }

  // Compare base hash with hash in manifest (make sure we operate on the)
  // right history file
  if (base_hash != manifest->catalog_hash()) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "wrong manifest, expected catalg %s, found catalog %s",
             base_hash.ToString().c_str(),
             manifest->catalog_hash().ToString().c_str());
    goto tag_fini;
  }

  // Download history database / create new history database
  if (manifest->history().IsNull()) {
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
  } else {
    const string url = repository_url + "/data" +
                       manifest->history().MakePath(1, 2) + "H";
    const hash::Any history_hash = manifest->history();
    download::JobInfo download_history(&url, true, false, &history_path,
                                       &history_hash);
    retval = download::Fetch(&download_history);
    if (retval != download::kFailOk) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to download history (%d)",
               retval);
      goto tag_fini;
    }
  }
  retval = tag_db.Open(history_path, sqlite::kDbOpenReadWrite);
  assert(retval);
  retval = tag_list.Load(&tag_db);
  assert(retval);

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
  // Download & verify manifest

  // Compare base hash with hash in manifest (make sure we operate on the
  // right history file)

  // Download history database

  // Safe highest revision from trunk tag

  // Remove all entries including destination catalog

  // Download rollback destination catalog

  // Update timestamp and revision

  // Commit catalog

  // Add new trunk tag / updated named tag to history
  return 0;
}
