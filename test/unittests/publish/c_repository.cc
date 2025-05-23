/**
 * This file is part of the CernVM File System.
 */

#include "c_repository.h"

#include <string>

#include "publish/settings.h"
#include "util/posix.h"

publish::Publisher *GetTestPublisher() {
  std::string base_dir = CreateTempDir(GetCurrentWorkingDirectory() + "/repo");
  std::string srv_dir = base_dir + "/srv";
  std::string spool_dir = base_dir + "/spool";
  std::string keys_dir = base_dir + "/keys";

  publish::SettingsPublisher settings("test.cvmfs.io");
  settings.SetIsSilent(true);
  settings.SetOwner(GetUserName());
  settings.GetStorage()->MakeLocal(srv_dir);
  settings.GetTransaction()->GetSpoolArea()->SetSpoolArea(spool_dir);
  settings.GetTransaction()->GetSpoolArea()->SetUnionMount(spool_dir
                                                           + "/union");
  settings.GetKeychain()->SetKeychainDir(keys_dir);
  settings.SetUrl("file://" + srv_dir);
  settings.GetTransaction()->SetLayoutRevision(
      publish::Publisher::kRequiredLayoutRevision);

  return publish::Publisher::Create(settings);
}


publish::Repository *GetRepositoryFromPublisher(
    const publish::Publisher &publisher) {
  publish::SettingsRepository settings(publisher.settings().fqrn());
  settings.SetUrl(publisher.settings().url());
  settings.SetTmpDir(GetCurrentWorkingDirectory());
  settings.GetKeychain()->SetKeychainDir(
      publisher.settings().keychain().keychain_dir());
  return new publish::Repository(settings);
}
