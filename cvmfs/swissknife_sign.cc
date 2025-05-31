/**
 * This file is part of the CernVM File System
 *
 * This tool signs a CernVM-FS manifest with an X.509 certificate.
 */

#include "swissknife_sign.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <termios.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <set>
#include <string>
#include <vector>

#include "compression/compression.h"
#include "crypto/hash.h"
#include "crypto/signature.h"
#include "manifest.h"
#include "object_fetcher.h"
#include "reflog.h"
#include "signing_tool.h"
#include "upload.h"
#include "util/logging.h"
#include "util/posix.h"
#include "util/smalloc.h"

using namespace std;  // NOLINT

typedef HttpObjectFetcher<> ObjectFetcher;

int swissknife::CommandSign::Main(const swissknife::ArgumentList &args) {
  const string manifest_path = *args.find('m')->second;
  const string repo_url = *args.find('u')->second;
  const string spooler_definition = *args.find('r')->second;
  const string temp_dir = *args.find('t')->second;

  string certificate = "";
  if (args.find('c') != args.end())
    certificate = *args.find('c')->second;
  string priv_key = "";
  if (args.find('k') != args.end())
    priv_key = *args.find('k')->second;
  string repo_name = "";
  if (args.find('n') != args.end())
    repo_name = *args.find('n')->second;
  string pwd = "";
  if (args.find('s') != args.end())
    pwd = *args.find('s')->second;
  string meta_info = "";
  if (args.find('M') != args.end())
    meta_info = *args.find('M')->second;
  string proxy = "";
  if (args.find('@') != args.end())
    proxy = *args.find('@')->second;
  const bool garbage_collectable = (args.count('g') > 0);
  const bool bootstrap_shortcuts = (args.count('A') > 0);
  const bool return_early = (args.count('e') > 0);

  string reflog_chksum_path;
  const shash::Any reflog_hash;
  if (args.find('R') != args.end()) {
    reflog_chksum_path = *args.find('R')->second;
  }

  SigningTool signing_tool(this);
  return signing_tool.Run(manifest_path, repo_url, spooler_definition, temp_dir,
                          certificate, priv_key, repo_name, pwd, meta_info,
                          reflog_chksum_path, proxy, garbage_collectable,
                          bootstrap_shortcuts, return_early);
}
