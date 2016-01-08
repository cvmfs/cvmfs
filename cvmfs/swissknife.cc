/**
 * This file is part of the CernVM File System
 *
 * This tool acts as an entry point for all the server-related
 * cvmfs tasks, such as uploading files and checking the sanity of
 * a repository.
 */

#include "cvmfs_config.h"
#include "swissknife.h"

#include <cassert>
#include <unistd.h>

#include <vector>

#include "logging.h"
#include "manifest.h"
#include "manifest_fetch.h"
#include "swissknife_check.h"
#include "swissknife_gc.h"
#include "swissknife_graft.h"
#include "swissknife_hash.h"
#include "swissknife_history.h"
#include "swissknife_info.h"
#include "swissknife_letter.h"
#include "swissknife_lsrepo.h"
#include "swissknife_migrate.h"
#include "swissknife_pull.h"
#include "swissknife_scrub.h"
#include "swissknife_sign.h"
#include "swissknife_sync.h"
#include "swissknife_zpipe.h"

using namespace std;  // NOLINT

namespace swissknife {
vector<Command *> command_list;
download::DownloadManager *g_download_manager;
signature::SignatureManager *g_signature_manager;
perf::Statistics *g_statistics;

void Usage() {
  LogCvmfs(kLogCvmfs, kLogStdout,
    "CernVM-FS repository storage management commands\n"
    "Version %s\n"
    "Usage (normally called from cvmfs_server):\n"
    "  cvmfs_swissknife <command> [options]\n",
    VERSION);

  for (unsigned i = 0; i < command_list.size(); ++i) {
    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "\n"
             "Command %s\n"
             "--------", command_list[i]->GetName().c_str());
    for (unsigned j = 0; j < command_list[i]->GetName().length(); ++j) {
      LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "-");
    }
    LogCvmfs(kLogCvmfs, kLogStdout, "");
    LogCvmfs(kLogCvmfs, kLogStdout, "%s",
             command_list[i]->GetDescription().c_str());
    swissknife::ParameterList params = command_list[i]->GetParams();
    if (!params.empty()) {
      LogCvmfs(kLogCvmfs, kLogStdout, "Options:");
      for (unsigned j = 0; j < params.size(); ++j) {
        LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "  -%c    %s",
                 params[j].key(), params[j].description().c_str());
        if (params[j].optional())
          LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, " (optional)");
        LogCvmfs(kLogCvmfs, kLogStdout, "");
      }
    }  // Parameter list
  }  // Command list

  LogCvmfs(kLogCvmfs, kLogStdout, "");
}


Command::~Command() {
  if (download_manager_.IsValid()) {
    download_manager_->Fini();
  }

  if (signature_manager_.IsValid()) {
    signature_manager_->Fini();
  }
}


bool Command::InitDownloadManager(const bool     follow_redirects,
                                  const unsigned max_pool_handles,
                                  const bool     use_system_proxy) {
  if (download_manager_.IsValid()) {
    return true;
  }

  download_manager_ = new download::DownloadManager();
  assert(download_manager_);
  download_manager_->Init(max_pool_handles, use_system_proxy, g_statistics);

  if (follow_redirects) {
    download_manager_->EnableRedirects();
  }

  return true;
}

bool Command::InitSignatureManager(const std::string pubkey_path,
                                   const std::string trusted_certs) {
  if (signature_manager_.IsValid()) {
    return true;
  }

  signature_manager_ = new signature::SignatureManager();
  assert(signature_manager_);
  signature_manager_->Init();

  if (!signature_manager_->LoadPublicRsaKeys(pubkey_path)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load public repo key '%s'",
             pubkey_path.c_str());
    return false;
  }

  if (!trusted_certs.empty() &&
      !signature_manager_->LoadTrustedCaCrl(trusted_certs)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load trusted certificates");
    return false;
  }

  return true;
}


download::DownloadManager* Command::download_manager()  const {
  assert(download_manager_.IsValid());
  return download_manager_.weak_ref();
}

signature::SignatureManager* Command::signature_manager() const {
  assert(signature_manager_.IsValid());
  return signature_manager_.weak_ref();
}


manifest::Manifest* Command::OpenLocalManifest(const std::string path) const {
  return manifest::Manifest::LoadFile(path);
}


manifest::Failures Command::FetchRemoteManifestEnsemble(
                             const std::string &repository_url,
                             const std::string &repository_name,
                                   manifest::ManifestEnsemble *ensemble) const {
  const uint64_t    minimum_timestamp = 0;
  const shash::Any *base_catalog      = NULL;
  return manifest::Fetch(repository_url,
                         repository_name,
                         minimum_timestamp,
                         base_catalog,
                         signature_manager(),
                         download_manager(),
                         ensemble);
}


manifest::Manifest* Command::FetchRemoteManifest(
                                          const std::string &repository_url,
                                          const std::string &repository_name,
                                          const shash::Any  &base_hash) const {
  manifest::ManifestEnsemble manifest_ensemble;
  UniquePtr<manifest::Manifest> manifest;

  // fetch (and verify) the manifest
  const manifest::Failures retval =
                              FetchRemoteManifestEnsemble(repository_url,
                                                          repository_name,
                                                          &manifest_ensemble);

  if (retval != manifest::kFailOk) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to fetch repository manifest "
                                    "(%d - %s)",
             retval, manifest::Code2Ascii(retval));
    return NULL;
  } else {
    // copy-construct a fresh manifest object because ManifestEnsemble will
    // free manifest_ensemble.manifest when it goes out of scope
    manifest = new manifest::Manifest(*manifest_ensemble.manifest);
  }

  // check if manifest fetching was successful
  if (!manifest) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load repository manifest");
    return NULL;
  }

  // check the provided base hash of the repository if provided
  if (!base_hash.IsNull() && manifest->catalog_hash() != base_hash) {
    LogCvmfs(kLogCvmfs, kLogStderr, "base hash does not match manifest "
                                    "(found: %s expected: %s)",
             manifest->catalog_hash().ToString().c_str(),
             base_hash.ToString().c_str());
    return NULL;
  }

  // return the fetched manifest (releasing pointer ownership)
  return manifest.Release();
}

}  // namespace swissknife


int main(int argc, char **argv) {
  swissknife::g_download_manager = new download::DownloadManager();
  swissknife::g_signature_manager = new signature::SignatureManager();
  swissknife::g_statistics = new perf::Statistics();

  swissknife::command_list.push_back(new swissknife::CommandCreate());
  swissknife::command_list.push_back(new swissknife::CommandUpload());
  swissknife::command_list.push_back(new swissknife::CommandRemove());
  swissknife::command_list.push_back(new swissknife::CommandPeek());
  swissknife::command_list.push_back(new swissknife::CommandSync());
  swissknife::command_list.push_back(new swissknife::CommandApplyDirtab());
  swissknife::command_list.push_back(new swissknife::CommandCreateTag());
  swissknife::command_list.push_back(new swissknife::CommandRemoveTag());
  swissknife::command_list.push_back(new swissknife::CommandListTags());
  swissknife::command_list.push_back(new swissknife::CommandInfoTag());
  swissknife::command_list.push_back(new swissknife::CommandRollbackTag());
  swissknife::command_list.push_back(new swissknife::CommandEmptyRecycleBin());
  swissknife::command_list.push_back(new swissknife::CommandSign());
  swissknife::command_list.push_back(new swissknife::CommandLetter());
  swissknife::command_list.push_back(new swissknife::CommandCheck());
  swissknife::command_list.push_back(new swissknife::CommandListCatalogs());
  swissknife::command_list.push_back(new swissknife::CommandPull());
  swissknife::command_list.push_back(new swissknife::CommandZpipe());
  swissknife::command_list.push_back(new swissknife::CommandGraft());
  swissknife::command_list.push_back(new swissknife::CommandHash());
  swissknife::command_list.push_back(new swissknife::CommandInfo());
  swissknife::command_list.push_back(new swissknife::CommandVersion());
  swissknife::command_list.push_back(new swissknife::CommandMigrate());
  swissknife::command_list.push_back(new swissknife::CommandScrub());
  swissknife::command_list.push_back(new swissknife::CommandGc());

  if (argc < 2) {
    swissknife::Usage();
    return 1;
  }
  if ((string(argv[1]) == "--help")) {
    swissknife::Usage();
    return 0;
  }
  if ((string(argv[1]) == "--version")) {
    swissknife::CommandVersion().Main(swissknife::ArgumentList());
    return 0;
  }

  // find the command to be run
  swissknife::Command *command = NULL;
  for (unsigned i = 0; i < swissknife::command_list.size(); ++i) {
    if (swissknife::command_list[i]->GetName() == string(argv[1])) {
      command = swissknife::command_list[i];
      break;
    }
  }

  if (NULL == command) {
    swissknife::Usage();
    return 1;
  }

  // parse the command line arguments for the Command
  swissknife::ArgumentList args;
  optind = 1;
  string option_string = "";
  swissknife::ParameterList params = command->GetParams();
  for (unsigned j = 0; j < params.size(); ++j) {
    option_string.push_back(params[j].key());
    if (!params[j].switch_only())
      option_string.push_back(':');
  }
  int c;
  while ((c = getopt(argc, argv, option_string.c_str())) != -1) {
    bool valid_option = false;
    for (unsigned j = 0; j < params.size(); ++j) {
      if (c == params[j].key()) {
        valid_option = true;
        string *argument = NULL;
        if (!params[j].switch_only()) {
          argument = new string(optarg);
        }
        args[c] = argument;
        break;
      }
    }
    if (!valid_option) {
      swissknife::Usage();
      return 1;
    }
  }
  for (unsigned j = 0; j < params.size(); ++j) {
    if (!params[j].optional()) {
      if (args.find(params[j].key()) == args.end()) {
        LogCvmfs(kLogCvmfs, kLogStderr, "parameter -%c missing",
                 params[j].key());
        return 1;
      }
    }
  }

  // run the command
  const int retval = command->Main(args);

  // delete the command list
        vector<swissknife::Command *>::const_iterator i    =
                                               swissknife::command_list.begin();
  const vector<swissknife::Command *>::const_iterator iend =
                                               swissknife::command_list.end();
  for (; i != iend; ++i) { // TODO(rmeusel): C++11 range based for loop :-(
    delete *i;
  }
  swissknife::command_list.clear();

  delete swissknife::g_signature_manager;
  delete swissknife::g_download_manager;
  delete swissknife::g_statistics;

  return retval;
}
