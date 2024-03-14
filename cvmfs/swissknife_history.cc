/**
 * This file is part of the CernVM File System
 */

#include "swissknife_history.h"
#include "cvmfs_config.h"

#include <algorithm>
#include <cassert>
#include <ctime>

#include "catalog_rw.h"
#include "crypto/hash.h"
#include "crypto/signature.h"
#include "manifest_fetch.h"
#include "network/download.h"
#include "upload.h"

using namespace std;         // NOLINT
using namespace swissknife;  // NOLINT

const std::string CommandTag::kHeadTag = "trunk";
const std::string CommandTag::kPreviousHeadTag = "trunk-previous";

const std::string CommandTag::kHeadTagDescription = "current HEAD";
const std::string CommandTag::kPreviousHeadTagDescription =
    "default undo target";

static void InsertCommonParameters(ParameterList *r) {
  r->push_back(Parameter::Mandatory('w', "repository directory / url"));
  r->push_back(Parameter::Mandatory('t', "temporary scratch directory"));
  r->push_back(Parameter::Optional('p', "public key of the repository"));
  r->push_back(Parameter::Optional('f', "fully qualified repository name"));
  r->push_back(Parameter::Optional('r', "spooler definition string"));
  r->push_back(Parameter::Optional('m', "(unsigned) manifest file to edit"));
  r->push_back(Parameter::Optional('b', "mounted repository base hash"));
  r->push_back(
      Parameter::Optional('e', "hash algorithm to use (default SHA1)"));
  r->push_back(Parameter::Switch('L', "follow HTTP redirects"));
  r->push_back(Parameter::Optional('P', "session_token_file"));
  r->push_back(Parameter::Optional('@', "proxy url"));
}

CommandTag::Environment *CommandTag::InitializeEnvironment(
    const ArgumentList &args, const bool read_write) {
  const string repository_url = MakeCanonicalPath(*args.find('w')->second);
  const string tmp_path = MakeCanonicalPath(*args.find('t')->second);
  const string spl_definition =
      (args.find('r') == args.end())
          ? ""
          : MakeCanonicalPath(*args.find('r')->second);
  const string manifest_path = (args.find('m') == args.end())
                                   ? ""
                                   : MakeCanonicalPath(*args.find('m')->second);
  const shash::Algorithms hash_algo =
      (args.find('e') == args.end())
          ? shash::kSha1
          : shash::ParseHashAlgorithm(*args.find('e')->second);
  const string pubkey_path = (args.find('p') == args.end())
                                 ? ""
                                 : MakeCanonicalPath(*args.find('p')->second);
  const shash::Any base_hash =
      (args.find('b') == args.end())
          ? shash::Any()
          : shash::MkFromHexPtr(shash::HexPtr(*args.find('b')->second),
                                shash::kSuffixCatalog);
  const string repo_name =
      (args.find('f') == args.end()) ? "" : *args.find('f')->second;

  string session_token_file;
  if (args.find('P') != args.end()) {
    session_token_file = *args.find('P')->second;
  }

  // Sanity checks
  if (hash_algo == shash::kAny) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to parse hash algorithm to use");
    return NULL;
  }

  if (read_write && spl_definition.empty()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "no upstream storage provided (-r)");
    return NULL;
  }

  if (read_write && manifest_path.empty()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "no (unsigned) manifest provided (-m)");
    return NULL;
  }

  if (!read_write && pubkey_path.empty()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "no public key provided (-p)");
    return NULL;
  }

  if (!read_write && repo_name.empty()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "no repository name provided (-f)");
    return NULL;
  }

  if (HasPrefix(spl_definition, "gw", false)) {
    if (session_token_file.empty()) {
      PrintError(
          "Session token file has to be provided "
          "when upstream type is gw.");
      return NULL;
    }
  }

  // create new environment
  // Note: We use this encapsulation because we cannot be sure that the
  // Command object gets deleted properly. With the Environment object at
  // hand we have full control and can make heavy and safe use of RAII
  UniquePtr<Environment> env(new Environment(repository_url, tmp_path));
  env->manifest_path.Set(manifest_path);
  env->history_path.Set(CreateTempPath(tmp_path + "/history", 0600));

  // initialize the (swissknife global) download manager
  const bool follow_redirects = (args.count('L') > 0);
  const std::string &proxy = (args.count('@') > 0) ?
                              *args.find('@')->second : "";
  if (!this->InitDownloadManager(follow_redirects, proxy)) {
    return NULL;
  }

  // initialize the (swissknife global) signature manager (if possible)
  if (!pubkey_path.empty() && !this->InitSignatureManager(pubkey_path)) {
    return NULL;
  }

  // open the (yet unsigned) manifest file if it is there, otherwise load the
  // latest manifest from the server
  env->manifest =
      (FileExists(env->manifest_path.path()))
          ? OpenLocalManifest(env->manifest_path.path())
          : FetchRemoteManifest(env->repository_url, repo_name, base_hash);

  if (!env->manifest.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load manifest file");
    return NULL;
  }

  // figure out the hash of the history from the previous revision if needed
  if (read_write && env->manifest->history().IsNull() && !base_hash.IsNull()) {
    env->previous_manifest =
        FetchRemoteManifest(env->repository_url, repo_name, base_hash);
    if (!env->previous_manifest.IsValid()) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to load previous manifest");
      return NULL;
    }

    LogCvmfs(kLogCvmfs, kLogDebug,
             "using history database '%s' from previous "
             "manifest (%s) as basis",
             env->previous_manifest->history().ToString().c_str(),
             env->previous_manifest->repository_name().c_str());
    env->manifest->set_history(env->previous_manifest->history());
    env->manifest->set_repository_name(
        env->previous_manifest->repository_name());
  }

  // download the history database referenced in the manifest
  env->history = GetHistory(env->manifest.weak_ref(), env->repository_url,
                            env->history_path.path(), read_write);
  if (!env->history.IsValid()) {
    return NULL;
  }

  // if the using Command is expected to change the history database, we
  // need
  // to initialize the upload spooler for potential later history upload
  if (read_write) {
    const bool use_file_chunking = false;
    const bool generate_legacy_bulk_chunks = false;
    const upload::SpoolerDefinition sd(spl_definition, hash_algo,
                                       zlib::kZlibDefault,
                                       generate_legacy_bulk_chunks,
                                       use_file_chunking, 0, 0, 0,
                                       session_token_file);
    env->spooler = upload::Spooler::Construct(sd);
    if (!env->spooler.IsValid()) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to initialize upload spooler");
      return NULL;
    }
  }

  // return the pointer of the Environment (passing the ownership along)
  return env.Release();
}

bool CommandTag::CloseAndPublishHistory(Environment *env) {
  assert(env->spooler.IsValid());

  // set the previous revision pointer of the history database
  env->history->SetPreviousRevision(env->manifest->history());

  // close the history database
  history::History *weak_history = env->history.Release();
  delete weak_history;

  // compress and upload the new history database
  Future<shash::Any> history_hash;
  upload::Spooler::CallbackPtr callback = env->spooler->RegisterListener(
      &CommandTag::UploadClosure, this, &history_hash);
  env->spooler->ProcessHistory(env->history_path.path());
  env->spooler->WaitForUpload();
  const shash::Any new_history_hash = history_hash.Get();
  env->spooler->UnregisterListener(callback);

  // retrieve the (async) uploader result
  if (new_history_hash.IsNull()) {
    return false;
  }

  // update the (yet unsigned) manifest file
  env->manifest->set_history(new_history_hash);
  if (!env->manifest->Export(env->manifest_path.path())) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to export the new manifest '%s'",
             env->manifest_path.path().c_str());
    return false;
  }

  // disable the unlink guard in order to keep the newly exported manifest file
  env->manifest_path.Disable();
  LogCvmfs(kLogCvmfs, kLogVerboseMsg,
           "exported manifest (%" PRIu64 ") with new history '%s'",
           env->manifest->revision(), new_history_hash.ToString().c_str());

  return true;
}


bool CommandTag::UploadCatalogAndUpdateManifest(
    CommandTag::Environment *env, catalog::WritableCatalog *catalog) {
  assert(env->spooler.IsValid());

  // gather information about catalog to be uploaded and update manifest
  UniquePtr<catalog::WritableCatalog> wr_catalog(catalog);
  const std::string catalog_path = wr_catalog->database_path();
  env->manifest->set_ttl(wr_catalog->GetTTL());
  env->manifest->set_revision(wr_catalog->GetRevision());
  env->manifest->set_publish_timestamp(wr_catalog->GetLastModified());

  // close the catalog
  catalog::WritableCatalog *weak_catalog = wr_catalog.Release();
  delete weak_catalog;

  // upload the catalog
  Future<shash::Any> catalog_hash;
  upload::Spooler::CallbackPtr callback = env->spooler->RegisterListener(
      &CommandTag::UploadClosure, this, &catalog_hash);
  env->spooler->ProcessCatalog(catalog_path);
  env->spooler->WaitForUpload();
  const shash::Any new_catalog_hash = catalog_hash.Get();
  env->spooler->UnregisterListener(callback);

  // check if the upload succeeded
  if (new_catalog_hash.IsNull()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to upload catalog '%s'",
             catalog_path.c_str());
    return false;
  }

  // update the catalog size and hash in the manifest
  const size_t catalog_size = GetFileSize(catalog_path);
  env->manifest->set_catalog_size(catalog_size);
  env->manifest->set_catalog_hash(new_catalog_hash);

  LogCvmfs(kLogCvmfs, kLogVerboseMsg, "uploaded new catalog (%lu bytes) '%s'",
           catalog_size, new_catalog_hash.ToString().c_str());

  return true;
}

void CommandTag::UploadClosure(const upload::SpoolerResult &result,
                               Future<shash::Any> *hash) {
  assert(!result.IsChunked());
  if (result.return_code != 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to upload history database (%d)",
             result.return_code);
    hash->Set(shash::Any());
  } else {
    hash->Set(result.content_hash);
  }
}

bool CommandTag::UpdateUndoTags(
    Environment *env, const history::History::Tag &current_head_template,
    const bool undo_rollback) {
  assert(env->history.IsValid());

  history::History::Tag current_head;
  history::History::Tag current_old_head;

  // remove previous HEAD tag
  if (!env->history->Remove(CommandTag::kPreviousHeadTag)) {
    LogCvmfs(kLogCvmfs, kLogVerboseMsg, "didn't find a previous HEAD tag");
  }

  // check if we have a current HEAD tag that needs to renamed to previous
  // HEAD
  if (env->history->GetByName(CommandTag::kHeadTag, &current_head)) {
    // remove current HEAD tag
    if (!env->history->Remove(CommandTag::kHeadTag)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to remove current HEAD tag");
      return false;
    }

    // set previous HEAD tag where current HEAD used to be
    if (!undo_rollback) {
      current_old_head = current_head;
      current_old_head.name = CommandTag::kPreviousHeadTag;
      current_old_head.description = CommandTag::kPreviousHeadTagDescription;
      if (!env->history->Insert(current_old_head)) {
        LogCvmfs(kLogCvmfs, kLogStderr, "failed to set previous HEAD tag");
        return false;
      }
    }
  }

  // set the current HEAD to the catalog provided by the template HEAD
  current_head = current_head_template;
  current_head.name = CommandTag::kHeadTag;
  current_head.description = CommandTag::kHeadTagDescription;
  if (!env->history->Insert(current_head)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to set new current HEAD");
    return false;
  }

  return true;
}

bool CommandTag::FetchObject(const std::string &repository_url,
                             const shash::Any &object_hash,
                             const std::string &destination_path) const {
  assert(!object_hash.IsNull());

  download::Failures dl_retval;
  const std::string url = repository_url + "/data/" + object_hash.MakePath();

  cvmfs::PathSink pathsink(destination_path);
  download::JobInfo download_object(&url, true, false, &object_hash, &pathsink);
  dl_retval = download_manager()->Fetch(&download_object);

  if (dl_retval != download::kFailOk) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to download object '%s' (%d - %s)",
             object_hash.ToStringWithSuffix().c_str(), dl_retval,
             download::Code2Ascii(dl_retval));
    return false;
  }

  return true;
}

history::History *CommandTag::GetHistory(const manifest::Manifest *manifest,
                                         const std::string &repository_url,
                                         const std::string &history_path,
                                         const bool read_write) const {
  const shash::Any history_hash = manifest->history();
  history::History *history;

  if (history_hash.IsNull()) {
    history = history::SqliteHistory::Create(history_path,
                                             manifest->repository_name());
    if (NULL == history) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to create history database");
      return NULL;
    }
  } else {
    if (!FetchObject(repository_url, history_hash, history_path)) {
      return NULL;
    }

    history = (read_write) ? history::SqliteHistory::OpenWritable(history_path)
                           : history::SqliteHistory::Open(history_path);
    if (NULL == history) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to open history database (%s)",
               history_path.c_str());
      unlink(history_path.c_str());
      return NULL;
    }

    assert(history->fqrn() == manifest->repository_name());
  }

  return history;
}

catalog::Catalog *CommandTag::GetCatalog(const std::string &repository_url,
                                         const shash::Any &catalog_hash,
                                         const std::string catalog_path,
                                         const bool read_write) const {
  assert(shash::kSuffixCatalog == catalog_hash.suffix);
  if (!FetchObject(repository_url, catalog_hash, catalog_path)) {
    return NULL;
  }

  const std::string catalog_root_path = "";
  return (read_write) ? catalog::WritableCatalog::AttachFreely(
                            catalog_root_path, catalog_path, catalog_hash)
                      : catalog::Catalog::AttachFreely(
                            catalog_root_path, catalog_path, catalog_hash);
}

void CommandTag::PrintTagMachineReadable(
    const history::History::Tag &tag) const {
  LogCvmfs(kLogCvmfs, kLogStdout, "%s %s %" PRIu64 " %" PRIu64 " %ld %s %s",
           tag.name.c_str(),
           tag.root_hash.ToString().c_str(), tag.size, tag.revision,
           tag.timestamp,
           (tag.branch == "") ? "(default)" : tag.branch.c_str(),
           tag.description.c_str());
}

std::string CommandTag::AddPadding(const std::string &str, const size_t padding,
                                   const bool align_right,
                                   const std::string &fill_char) const {
  assert(str.size() <= padding);
  std::string result(str);
  result.resize(padding);
  const size_t pos = (align_right) ? 0 : str.size();
  const size_t padding_width = padding - str.size();
  for (size_t i = 0; i < padding_width; ++i) result.insert(pos, fill_char);
  return result;
}

bool CommandTag::IsUndoTagName(const std::string &tag_name) const {
  return tag_name == CommandTag::kHeadTag ||
         tag_name == CommandTag::kPreviousHeadTag;
}

//------------------------------------------------------------------------------

ParameterList CommandEditTag::GetParams() const {
  ParameterList r;
  InsertCommonParameters(&r);

  r.push_back(Parameter::Optional('d', "space separated tags to be deleted"));
  r.push_back(Parameter::Optional('a', "name of the new tag"));
  r.push_back(Parameter::Optional('D', "description of the tag"));
  r.push_back(Parameter::Optional('B', "branch of the new tag"));
  r.push_back(Parameter::Optional('P', "predecessor branch"));
  r.push_back(Parameter::Optional('h', "root hash of the new tag"));
  r.push_back(Parameter::Switch('x', "maintain undo tags"));
  return r;
}

int CommandEditTag::Main(const ArgumentList &args) {
  if ((args.find('d') == args.end()) && (args.find('a') == args.end()) &&
      (args.find('x') == args.end())) {
    LogCvmfs(kLogCvmfs, kLogStderr, "nothing to do");
    return 1;
  }

  // initialize the Environment (taking ownership)
  const bool history_read_write = true;
  UniquePtr<Environment> env(InitializeEnvironment(args, history_read_write));
  if (!env.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to init environment");
    return 1;
  }

  int retval;
  if (args.find('d') != args.end()) {
    retval = RemoveTags(args, env.weak_ref());
    if (retval != 0) return retval;
  }
  if ((args.find('a') != args.end()) || (args.find('x') != args.end())) {
    retval = AddNewTag(args, env.weak_ref());
    if (retval != 0) return retval;
  }

  // finalize processing and upload new history database
  if (!CloseAndPublishHistory(env.weak_ref())) {
    return 1;
  }
  return 0;
}

int CommandEditTag::AddNewTag(const ArgumentList &args, Environment *env) {
  const std::string tag_name =
      (args.find('a') != args.end()) ? *args.find('a')->second : "";
  const std::string tag_description =
      (args.find('D') != args.end()) ? *args.find('D')->second : "";
  const bool undo_tags = (args.find('x') != args.end());
  const std::string root_hash_string =
      (args.find('h') != args.end()) ? *args.find('h')->second : "";
  const std::string branch_name =
    (args.find('B') != args.end()) ? *args.find('B')->second : "";
  const std::string previous_branch_name =
    (args.find('P') != args.end()) ? *args.find('P')->second : "";

  if (tag_name.find(" ") != std::string::npos) {
    LogCvmfs(kLogCvmfs, kLogStderr, "tag names must not contain spaces");
    return 1;
  }

  assert(!tag_name.empty() || undo_tags);

  if (IsUndoTagName(tag_name)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "undo tags are managed internally");
    return 1;
  }

  // set the root hash to be tagged to the current HEAD if no other hash was
  // given by the user
  shash::Any root_hash = GetTagRootHash(env, root_hash_string);
  if (root_hash.IsNull()) {
    return 1;
  }

  // open the catalog to be tagged (to check for existence and for meta info)
  const UnlinkGuard catalog_path(
      CreateTempPath(env->tmp_path + "/catalog", 0600));
  const bool catalog_read_write = false;
  const UniquePtr<catalog::Catalog> catalog(GetCatalog(
      env->repository_url, root_hash, catalog_path.path(), catalog_read_write));
  if (!catalog.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "catalog with hash '%s' does not exist",
             root_hash.ToString().c_str());
    return 1;
  }

  // check if the catalog is a root catalog
  if (!catalog->root_prefix().IsEmpty()) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "cannot tag catalog '%s' that is not a "
             "root catalog.",
             root_hash.ToString().c_str());
    return 1;
  }

  // create a template for the new tag to be created, moved or used as undo tag
  history::History::Tag tag_template;
  tag_template.name = "<template>";
  tag_template.root_hash = root_hash;
  tag_template.size = GetFileSize(catalog_path.path());
  tag_template.revision = catalog->GetRevision();
  tag_template.timestamp = catalog->GetLastModified();
  tag_template.branch = branch_name;
  tag_template.description = tag_description;

  // manipulate the tag database by creating a new tag or moving an existing one
  if (!tag_name.empty()) {
    tag_template.name = tag_name;
    const bool user_provided_hash = (!root_hash_string.empty());

    if (!env->history->ExistsBranch(tag_template.branch)) {
      history::History::Branch branch(
        tag_template.branch,
        previous_branch_name,
        tag_template.revision);
      if (!env->history->InsertBranch(branch)) {
        LogCvmfs(kLogCvmfs, kLogStderr, "cannot insert branch '%s'",
                 tag_template.branch.c_str());
        return 1;
      }
    }

    if (!ManipulateTag(env, tag_template, user_provided_hash)) {
      return 1;
    }
  }

  // handle undo tags ('trunk' and 'trunk-previous') if necessary
  if (undo_tags && !UpdateUndoTags(env, tag_template)) {
    return 1;
  }

  return 0;
}

shash::Any CommandEditTag::GetTagRootHash(
    Environment *env, const std::string &root_hash_string) const {
  shash::Any root_hash;

  if (root_hash_string.empty()) {
    LogCvmfs(kLogCvmfs, kLogVerboseMsg,
             "no catalog hash provided, using hash"
             "of current HEAD catalog (%s)",
             env->manifest->catalog_hash().ToString().c_str());
    root_hash = env->manifest->catalog_hash();
  } else {
    root_hash = shash::MkFromHexPtr(shash::HexPtr(root_hash_string),
                                    shash::kSuffixCatalog);
    if (root_hash.IsNull()) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "failed to read provided catalog hash '%s'",
               root_hash_string.c_str());
    }
  }

  return root_hash;
}

bool CommandEditTag::ManipulateTag(Environment *env,
                                   const history::History::Tag &tag_template,
                                   const bool user_provided_hash) {
  const std::string &tag_name = tag_template.name;

  // check if the tag already exists, otherwise create it and return
  if (!env->history->Exists(tag_name)) {
    return CreateTag(env, tag_template);
  }

  // tag does exist already, now we need to see if we can move it
  if (!user_provided_hash) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "a tag with the name '%s' already exists. Do you want to move it? "
             "(-h <root hash>)",
             tag_name.c_str());
    return false;
  }

  // move the already existing tag and return
  return MoveTag(env, tag_template);
}

bool CommandEditTag::MoveTag(Environment *env,
                             const history::History::Tag &tag_template)
{
  const std::string &tag_name = tag_template.name;
  history::History::Tag new_tag = tag_template;

  // get the already existent tag
  history::History::Tag old_tag;
  if (!env->history->GetByName(tag_name, &old_tag)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to retrieve tag '%s' for moving",
             tag_name.c_str());
    return false;
  }

  // check if we would move the tag to the same hash
  if (old_tag.root_hash == new_tag.root_hash) {
    LogCvmfs(kLogCvmfs, kLogStderr, "tag '%s' already points to '%s'",
             tag_name.c_str(), old_tag.root_hash.ToString().c_str());
    return false;
  }

  // copy over old description if no new description was given
  if (new_tag.description.empty()) {
    new_tag.description = old_tag.description;
  }
  new_tag.branch = old_tag.branch;

  // remove the old tag from the database
  if (!env->history->Remove(tag_name)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "removing old tag '%s' before move failed",
             tag_name.c_str());
    return false;
  }
  if (!env->history->PruneBranches()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "could not prune unused branches");
    return false;
  }
  bool retval = env->history->Vacuum();
  assert(retval);

  LogCvmfs(kLogCvmfs, kLogStdout, "moving tag '%s' from '%s' to '%s'",
           tag_name.c_str(), old_tag.root_hash.ToString().c_str(),
           tag_template.root_hash.ToString().c_str());

  // re-create the moved tag
  return CreateTag(env, new_tag);
}

bool CommandEditTag::CreateTag(Environment *env,
                               const history::History::Tag &new_tag) {
  if (!env->history->Insert(new_tag)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to insert new tag '%s'",
             new_tag.name.c_str());
    return false;
  }

  return true;
}

int CommandEditTag::RemoveTags(const ArgumentList &args, Environment *env) {
  typedef std::vector<std::string> TagNames;
  const std::string tags_to_delete = *args.find('d')->second;

  const TagNames condemned_tags = SplitString(tags_to_delete, ' ');

  // check if user tries to remove a magic undo tag
  TagNames::const_iterator i = condemned_tags.begin();
  const TagNames::const_iterator iend = condemned_tags.end();
  for (; i != iend; ++i) {
    if (IsUndoTagName(*i)) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "undo tags are handled internally and cannot be deleted");
      return 1;
    }
  }

  LogCvmfs(kLogCvmfs, kLogDebug, "proceeding to delete %lu tags",
           condemned_tags.size());

  // check if the tags to be deleted exist
  bool all_exist = true;
  for (i = condemned_tags.begin(); i != iend; ++i) {
    if (!env->history->Exists(*i)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "tag '%s' does not exist", i->c_str());
      all_exist = false;
    }
  }
  if (!all_exist) {
    return 1;
  }

  // delete the tags from the tag database and print their root hashes
  i = condemned_tags.begin();
  env->history->BeginTransaction();
  for (; i != iend; ++i) {
    // print some information about the tag to be deleted
    history::History::Tag condemned_tag;
    const bool found_tag = env->history->GetByName(*i, &condemned_tag);
    assert(found_tag);
    LogCvmfs(kLogCvmfs, kLogStdout, "deleting '%s' (%s)",
             condemned_tag.name.c_str(),
             condemned_tag.root_hash.ToString().c_str());

    // remove the tag
    if (!env->history->Remove(*i)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to remove tag '%s' from history",
               i->c_str());
      return 1;
    }
  }
  bool retval = env->history->PruneBranches();
  if (!retval) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "failed to prune unused branches from history");
    return 1;
  }
  env->history->CommitTransaction();
  retval = env->history->Vacuum();
  assert(retval);

  return 0;
}

//------------------------------------------------------------------------------


ParameterList CommandListTags::GetParams() const {
  ParameterList r;
  InsertCommonParameters(&r);
  r.push_back(Parameter::Switch('x', "machine readable output"));
  r.push_back(Parameter::Switch('B', "print branch hierarchy"));
  return r;
}

void CommandListTags::PrintHumanReadableTagList(
    const CommandListTags::TagList &tags) const {
  // go through the list of tags and figure out the column widths
  const std::string name_label = "Name";
  const std::string rev_label = "Revision";
  const std::string time_label = "Timestamp";
  const std::string branch_label = "Branch";
  const std::string desc_label = "Description";

  // figure out the maximal lengths of the fields in the lists
  TagList::const_reverse_iterator i = tags.rbegin();
  const TagList::const_reverse_iterator iend = tags.rend();
  size_t max_name_len = name_label.size();
  size_t max_rev_len = rev_label.size();
  size_t max_time_len = desc_label.size();
  size_t max_branch_len = branch_label.size();
  for (; i != iend; ++i) {
    max_name_len = std::max(max_name_len, i->name.size());
    max_rev_len = std::max(max_rev_len, StringifyInt(i->revision).size());
    max_time_len =
        std::max(max_time_len, StringifyTime(i->timestamp, true).size());
    max_branch_len = std::max(max_branch_len, i->branch.size());
  }

  // print the list header
  LogCvmfs(kLogCvmfs, kLogStdout,
           "%s \u2502 %s \u2502 %s \u2502 %s \u2502 %s",
           AddPadding(name_label, max_name_len).c_str(),
           AddPadding(rev_label, max_rev_len).c_str(),
           AddPadding(time_label, max_time_len).c_str(),
           AddPadding(branch_label, max_branch_len).c_str(),
           desc_label.c_str());
  LogCvmfs(kLogCvmfs, kLogStdout,
           "%s\u2500\u253C\u2500%s\u2500\u253C\u2500%s"
           "\u2500\u253C\u2500%s\u2500\u253C\u2500%s",
           AddPadding("", max_name_len, false, "\u2500").c_str(),
           AddPadding("", max_rev_len, false, "\u2500").c_str(),
           AddPadding("", max_time_len, false, "\u2500").c_str(),
           AddPadding("", max_branch_len, false, "\u2500").c_str(),
           AddPadding("", desc_label.size() + 1, false, "\u2500").c_str());

  // print the rows of the list
  i = tags.rbegin();
  for (; i != iend; ++i) {
    LogCvmfs(
        kLogCvmfs, kLogStdout,
        "%s \u2502 %s \u2502 %s \u2502 %s \u2502 %s",
        AddPadding(i->name, max_name_len).c_str(),
        AddPadding(StringifyInt(i->revision), max_rev_len, true).c_str(),
        AddPadding(StringifyTime(i->timestamp, true), max_time_len).c_str(),
        AddPadding(i->branch, max_branch_len).c_str(),
        i->description.c_str());
  }

  // print the list footer
  LogCvmfs(kLogCvmfs, kLogStdout,
           "%s\u2500\u2534\u2500%s\u2500\u2534\u2500%s"
           "\u2500\u2534\u2500%s\u2500\u2534\u2500%s",
           AddPadding("", max_name_len, false, "\u2500").c_str(),
           AddPadding("", max_rev_len, false, "\u2500").c_str(),
           AddPadding("", max_time_len, false, "\u2500").c_str(),
           AddPadding("", max_branch_len, false, "\u2500").c_str(),
           AddPadding("", desc_label.size() + 1, false, "\u2500").c_str());

  // print the number of tags listed
  LogCvmfs(kLogCvmfs, kLogStdout, "listing contains %lu tags", tags.size());
}

void CommandListTags::PrintMachineReadableTagList(const TagList &tags) const {
  TagList::const_iterator i = tags.begin();
  const TagList::const_iterator iend = tags.end();
  for (; i != iend; ++i) {
    PrintTagMachineReadable(*i);
  }
}


void CommandListTags::PrintHumanReadableBranchList(
  const BranchHierarchy &branches) const
{
  unsigned N = branches.size();
  for (unsigned i = 0; i < N; ++i) {
    for (unsigned l = 0; l < branches[i].level; ++l) {
      LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "%s",
               ((l + 1) == branches[i].level) ? "\u251c " : "\u2502 ");
    }
    LogCvmfs(kLogCvmfs, kLogStdout, "%s @%" PRIu64,
             branches[i].branch.branch.c_str(),
             branches[i].branch.initial_revision);
  }
}


void CommandListTags::PrintMachineReadableBranchList(
  const BranchHierarchy &branches) const
{
  unsigned N = branches.size();
  for (unsigned i = 0; i < N; ++i) {
    LogCvmfs(kLogCvmfs, kLogStdout, "[%u] %s%s @%" PRIu64,
             branches[i].level,
             AddPadding("", branches[i].level, false, " ").c_str(),
             branches[i].branch.branch.c_str(),
             branches[i].branch.initial_revision);
  }
}


void CommandListTags::SortBranchesRecursively(
  unsigned level,
  const string &parent_branch,
  const BranchList &branches,
  BranchHierarchy *hierarchy) const
{
  // For large numbers of branches, this should be turned into the O(n) version
  // using a linked list
  unsigned N = branches.size();
  for (unsigned i = 0; i < N; ++i) {
    if (branches[i].branch == "")
      continue;
    if (branches[i].parent == parent_branch) {
      hierarchy->push_back(BranchLevel(branches[i], level));
      SortBranchesRecursively(
          level + 1, branches[i].branch, branches, hierarchy);
    }
  }
}


CommandListTags::BranchHierarchy CommandListTags::SortBranches(
  const BranchList &branches) const
{
  BranchHierarchy hierarchy;
  hierarchy.push_back(
    BranchLevel(history::History::Branch("(default)", "", 0), 0));
  SortBranchesRecursively(1, "", branches, &hierarchy);
  return hierarchy;
}


int CommandListTags::Main(const ArgumentList &args) {
  const bool machine_readable = (args.find('x') != args.end());
  const bool branch_hierarchy = (args.find('B') != args.end());

  // initialize the Environment (taking ownership)
  const bool history_read_write = false;
  UniquePtr<Environment> env(InitializeEnvironment(args, history_read_write));
  if (!env.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to init environment");
    return 1;
  }

  if (branch_hierarchy) {
    BranchList branch_list;
    if (!env->history->ListBranches(&branch_list)) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "failed to list branches in history database");
      return 1;
    }
    BranchHierarchy branch_hierarchy = SortBranches(branch_list);

    if (machine_readable) {
      PrintMachineReadableBranchList(branch_hierarchy);
    } else {
      PrintHumanReadableBranchList(branch_hierarchy);
    }
  } else {
    // obtain a full list of all tags
    TagList tags;
    if (!env->history->List(&tags)) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "failed to list tags in history database");
      return 1;
    }

    if (machine_readable) {
      PrintMachineReadableTagList(tags);
    } else {
      PrintHumanReadableTagList(tags);
    }
  }

  return 0;
}

//------------------------------------------------------------------------------

ParameterList CommandInfoTag::GetParams() const {
  ParameterList r;
  InsertCommonParameters(&r);

  r.push_back(Parameter::Mandatory('n', "name of the tag to be inspected"));
  r.push_back(Parameter::Switch('x', "machine readable output"));
  return r;
}

std::string CommandInfoTag::HumanReadableFilesize(const size_t filesize) const {
  const size_t kiB = 1024;
  const size_t MiB = kiB * 1024;
  const size_t GiB = MiB * 1024;

  if (filesize > GiB) {
    return StringifyDouble(static_cast<double>(filesize) / GiB) + " GiB";
  } else if (filesize > MiB) {
    return StringifyDouble(static_cast<double>(filesize) / MiB) + " MiB";
  } else if (filesize > kiB) {
    return StringifyDouble(static_cast<double>(filesize) / kiB) + " kiB";
  } else {
    return StringifyInt(filesize) + " Byte";
  }
}

void CommandInfoTag::PrintHumanReadableInfo(
    const history::History::Tag &tag) const {
  LogCvmfs(kLogCvmfs, kLogStdout,
           "Name:         %s\n"
           "Revision:     %" PRIu64 "\n"
           "Timestamp:    %s\n"
           "Branch:       %s\n"
           "Root Hash:    %s\n"
           "Catalog Size: %s\n"
           "%s",
           tag.name.c_str(), tag.revision,
           StringifyTime(tag.timestamp, true /* utc */).c_str(),
           tag.branch.c_str(),
           tag.root_hash.ToString().c_str(),
           HumanReadableFilesize(tag.size).c_str(),
           tag.description.c_str());
}

int CommandInfoTag::Main(const ArgumentList &args) {
  const std::string tag_name = *args.find('n')->second;
  const bool machine_readable = (args.find('x') != args.end());

  // initialize the Environment (taking ownership)
  const bool history_read_write = false;
  UniquePtr<Environment> env(InitializeEnvironment(args, history_read_write));
  if (!env.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to init environment");
    return 1;
  }

  history::History::Tag tag;
  const bool found = env->history->GetByName(tag_name, &tag);
  if (!found) {
    LogCvmfs(kLogCvmfs, kLogStderr, "tag '%s' does not exist",
             tag_name.c_str());
    return 1;
  }

  if (machine_readable) {
    PrintTagMachineReadable(tag);
  } else {
    PrintHumanReadableInfo(tag);
  }

  return 0;
}

//------------------------------------------------------------------------------

ParameterList CommandRollbackTag::GetParams() const {
  ParameterList r;
  InsertCommonParameters(&r);

  r.push_back(Parameter::Optional('n', "name of the tag to be republished"));
  return r;
}

int CommandRollbackTag::Main(const ArgumentList &args) {
  const bool undo_rollback = (args.find('n') == args.end());
  const std::string tag_name =
      (!undo_rollback) ? *args.find('n')->second : CommandTag::kPreviousHeadTag;

  // initialize the Environment (taking ownership)
  const bool history_read_write = true;
  UniquePtr<Environment> env(InitializeEnvironment(args, history_read_write));
  if (!env.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to init environment");
    return 1;
  }

  // find tag to be rolled back to
  history::History::Tag target_tag;
  const bool found = env->history->GetByName(tag_name, &target_tag);
  if (!found) {
    if (undo_rollback) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "only one anonymous rollback supported - "
               "perhaps you want to provide a tag name?");
    } else {
      LogCvmfs(kLogCvmfs, kLogStderr, "tag '%s' does not exist",
               tag_name.c_str());
    }
    return 1;
  }
  if (target_tag.branch != "") {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "rollback is only supported on the default branch");
    return 1;
  }

  // list the tags that will be deleted
  TagList affected_tags;
  if (!env->history->ListTagsAffectedByRollback(tag_name, &affected_tags)) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "failed to list condemned tags prior to rollback to '%s'",
             tag_name.c_str());
    return 1;
  }

  // check if tag is valid to be rolled back to
  const uint64_t current_revision = env->manifest->revision();
  assert(target_tag.revision <= current_revision);
  if (target_tag.revision == current_revision) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "not rolling back to current head (%" PRIu64 ")",
             current_revision);
    return 1;
  }

  // open the catalog to be rolled back to
  const UnlinkGuard catalog_path(
      CreateTempPath(env->tmp_path + "/catalog", 0600));
  const bool catalog_read_write = true;
  UniquePtr<catalog::WritableCatalog> catalog(
      dynamic_cast<catalog::WritableCatalog *>(
          GetCatalog(env->repository_url, target_tag.root_hash,
                     catalog_path.path(), catalog_read_write)));
  if (!catalog.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to open catalog with hash '%s'",
             target_tag.root_hash.ToString().c_str());
    return 1;
  }

  // check if the catalog has a supported schema version
  if (catalog->schema() < catalog::CatalogDatabase::kLatestSupportedSchema -
                              catalog::CatalogDatabase::kSchemaEpsilon) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "not rolling back to outdated and "
             "incompatible catalog schema (%.1f < %.1f)",
             catalog->schema(),
             catalog::CatalogDatabase::kLatestSupportedSchema);
    return 1;
  }

  // update the catalog to be republished
  catalog->Transaction();
  catalog->UpdateLastModified();
  catalog->SetRevision(current_revision + 1);
  catalog->SetPreviousRevision(env->manifest->catalog_hash());
  catalog->Commit();

  // Upload catalog (handing over ownership of catalog pointer)
  if (!UploadCatalogAndUpdateManifest(env.weak_ref(), catalog.Release())) {
    LogCvmfs(kLogCvmfs, kLogStderr, "catalog upload failed");
    return 1;
  }

  // update target tag with newly published root catalog information
  history::History::Tag updated_target_tag(target_tag);
  updated_target_tag.root_hash = env->manifest->catalog_hash();
  updated_target_tag.size = env->manifest->catalog_size();
  updated_target_tag.revision = env->manifest->revision();
  updated_target_tag.timestamp = env->manifest->publish_timestamp();
  if (!env->history->Rollback(updated_target_tag)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to rollback history to '%s'",
             updated_target_tag.name.c_str());
    return 1;
  }
  bool retval = env->history->Vacuum();
  assert(retval);

  // set the magic undo tags
  if (!UpdateUndoTags(env.weak_ref(), updated_target_tag, undo_rollback)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to update magic undo tags");
    return 1;
  }

  // finalize the history and upload it
  if (!CloseAndPublishHistory(env.weak_ref())) {
    return 1;
  }

  // print the tags that have been removed by the rollback
  PrintDeletedTagList(affected_tags);

  return 0;
}

void CommandRollbackTag::PrintDeletedTagList(const TagList &tags) const {
  size_t longest_name = 0;
  TagList::const_iterator i = tags.begin();
  const TagList::const_iterator iend = tags.end();
  for (; i != iend; ++i) {
    longest_name = std::max(i->name.size(), longest_name);
  }

  i = tags.begin();
  for (; i != iend; ++i) {
    LogCvmfs(kLogCvmfs, kLogStdout, "removed tag %s (%s)",
             AddPadding(i->name, longest_name).c_str(),
             i->root_hash.ToString().c_str());
  }
}

//------------------------------------------------------------------------------

ParameterList CommandEmptyRecycleBin::GetParams() const {
  ParameterList r;
  InsertCommonParameters(&r);
  return r;
}

int CommandEmptyRecycleBin::Main(const ArgumentList &args) {
  // initialize the Environment (taking ownership)
  const bool history_read_write = true;
  UniquePtr<Environment> env(InitializeEnvironment(args, history_read_write));
  if (!env.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to init environment");
    return 1;
  }

  if (!env->history->EmptyRecycleBin()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to empty recycle bin");
    return 1;
  }

  // finalize the history and upload it
  if (!CloseAndPublishHistory(env.weak_ref())) {
    return 1;
  }

  return 0;
}
