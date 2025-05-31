/**
 * This file is part of the CernVM File System.
 */

#include "cmd_pub.h"

#include <fcntl.h>

#include <string>
#include <vector>

#include "manifest.h"
#include "network/download.h"
#include "notify/messages.h"
#include "notify/publisher_http.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/string.h"

namespace {

const LogFacilities &kLogInfo = DefaultLogging::info;
const LogFacilities &kLogError = DefaultLogging::error;

const int kMaxPoolHandles = 1;
const unsigned kDownloadTimeout = 60;  // 1 minute
const unsigned kDownloadRetries = 1;   // 2 attempts in total

}  // namespace

namespace notify {

int DoPublish(const std::string &server_url, const std::string &repository_url,
              bool verbose) {
  const std::string repo_url = MakeCanonicalPath(repository_url);

  if (verbose) {
    LogCvmfs(kLogCvmfs, kLogInfo, "Parameters: ");
    LogCvmfs(kLogCvmfs, kLogInfo, "  CVMFS repository URL: %s",
             repo_url.c_str());
    LogCvmfs(kLogCvmfs, kLogInfo, "  Notification server URL: %s",
             server_url.c_str());
  }

  // Download repository manifest
  std::string manifest_contents;
  const std::string manifest_url = repo_url + "/.cvmfspublished";
  if (IsHttpUrl(repo_url)) {
    perf::Statistics stats;
    const UniquePtr<download::DownloadManager> download_manager(
        new download::DownloadManager(
            kMaxPoolHandles, perf::StatisticsTemplate("download", &stats)));
    assert(download_manager.IsValid());

    download_manager->SetTimeout(kDownloadTimeout, kDownloadTimeout);
    download_manager->SetRetryParameters(kDownloadRetries, 500, 2000);

    cvmfs::MemSink manifest_memsink;
    download::JobInfo download_manifest(&manifest_url, false, false, NULL,
                                        &manifest_memsink);
    const download::Failures retval =
        download_manager->Fetch(&download_manifest);
    if (retval != download::kFailOk) {
      LogCvmfs(kLogCvmfs, kLogError, "Failed to download manifest (%d - %s)",
               retval, download::Code2Ascii(retval));
      return 6;
    }
    manifest_contents = std::string(
        reinterpret_cast<char *>(manifest_memsink.data()),
        manifest_memsink.pos());
  } else {
    const int fd = open(manifest_url.c_str(), O_RDONLY);
    if (fd == -1) {
      LogCvmfs(kLogCvmfs, kLogError, "Could not open manifest file");
      return 7;
    }
    if (!SafeReadToString(fd, &manifest_contents)) {
      LogCvmfs(kLogCvmfs, kLogError, "Could not read manifest file");
      close(fd);
      return 8;
    }
    close(fd);
  }

  const UniquePtr<manifest::Manifest> manifest(manifest::Manifest::LoadMem(
      reinterpret_cast<const unsigned char *>(manifest_contents.data()),
      manifest_contents.size()));

  if (verbose) {
    LogCvmfs(kLogCvmfs, kLogInfo, "Current repository manifest:\n%s",
             manifest->ExportString().c_str());
  }

  const std::string repository_name = manifest->repository_name();

  // Publish message
  const UniquePtr<notify::Publisher> publisher(
      new notify::PublisherHTTP(server_url));

  std::string msg_text;
  notify::msg::Activity msg;
  msg.version_ = 1;
  msg.timestamp_ = StringifyTime(std::time(NULL), true);
  msg.repository_ = repository_name;
  msg.manifest_ = manifest_contents;
  msg.ToJSONString(&msg_text);

  if (!publisher->Publish(msg_text, repository_name)) {
    LogCvmfs(kLogCvmfs, kLogError, "Could not publish notification");
    return 9;
  }

  assert(publisher->Finalize());

  return 0;
}

}  // namespace notify
