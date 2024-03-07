#include "swissknife_ingestsql.h"

#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <pwd.h>
#include <grp.h>

#include <csignal>
#include <fstream>

#include "acl/libacl.h"
#include "catalog_mgr_rw.h"
#include "curl/curl.h"
#include "gateway_util.h"
#include "swissknife_lease_curl.h"
#include "swissknife_lease_json.h"

#define CHECK_SQLITE_ERROR(ret, expected) do {                                                          \
                                            if (ret!=expected) {                                        \
                                              LogCvmfs(kLogCvmfs, kLogStderr, "SQLite error: %d", ret); \
                                              assert(0);                                                \
                                            }                                                           \
                                          } while (0)

// bring these in from SyncParameters, as we're not using that class now
static const unsigned kDefaultMaxWeight = 100000;
static const unsigned kDefaultMinWeight = 1000;
static const size_t kDefaultMinFileChunkSize = 4 * 1024 * 1024;
static const size_t kDefaultAvgFileChunkSize = 8 * 1024 * 1024;
static const size_t kDefaultMaxFileChunkSize = 16 * 1024 * 1024;
static const unsigned kDefaultNestedKcatalogLimit = 500;
static const unsigned kDefaultRootKcatalogLimit = 200;
static const unsigned kDefaultFileMbyteLimit = 1024;

static const unsigned kChunkSize = 24 * 1024 * 1024;
static const unsigned kLeaseBusyRetryInterval = 10;

static string kConfigDir("/etc/cvmfs/gateway-client/");

static bool g_lease_acquired = false;
static string g_gateway_url;
static string g_gateway_key_id;
static string g_gateway_secret;
static string g_session_token;
static string g_session_token_file;

static string get_lease_from_paths(vector<string> paths);
static vector<string> get_all_paths_from_sqlite(vector<string>& sqlite_db_vec,
                                                bool include_additions,
                                                bool include_deletions);
static string get_parent(string& path);
static string get_basename(string& path);
static XattrList marshal_xattrs(const char *acl);
static string sanitise_name(const char *name_cstr, bool allow_leading_slash);
static string sanitise_symlink_target(const char *name_cstr);
static void on_signal(int sig);
static string acquire_lease(string& key_id, string& secret, string lease_path,
                            string& repo_service_url, bool force_cancel_lease);
static void cancel_lease();
static vector<string> get_file_list(string& path);
static int check_hash(const char *hash) ;
static string merge_databases(vector<string>& sqlite_db_vec, string& tmpdir);
static void recursively_delete_directory(PathString& path, catalog::WritableCatalogManager &catalog_manager);
static void create_empty_database( string& filename );
static void relax_db_locking(sqlite3 *db);
 
static string sanitise_symlink_target(const char *name_cstr) {
  string name = string(name_cstr);
  bool ok = true;
#if 0
  for (int i = strlen(name_cstr) - 1; i >= 0; i--) {
    // prohibit control codes and whitespace
    if (name_cstr[i] <= 33) {
      ok = false;
    }
  }
#endif
  if (name.find("//") != string::npos) {
    ok = false;
  }
  if (name == "") {
    ok = false;
  }
  if (!ok) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Name [%s] is invalid", name.c_str());
    assert(0);
  }
  return string(name);
}

static string sanitise_name(const char *name_cstr,
                            bool allow_leading_slash = false) {
  int reason = 0;
  const char *c = name_cstr;
  while(*c == '/' && *c != '\0' ) {c++;} // strip any leading slashes
  string name = string(c);
  bool ok = true;
#if 0
  for (int i = strlen(name_cstr) - 1; i >= 0; i--) {
    // prohibit control codes and whitespace
    if (name_cstr[i] <= 33) {
      ok = false;
    }
  }
#endif

  if (!allow_leading_slash && HasPrefix(name, "/", true)) {
    reason=1;
    ok = false;
  }
  if (HasSuffix(name, "/", true)) {
    if (!(allow_leading_slash &&
          name.size() == 1)) {  // account for the case where name=="/"
      reason=2;
      ok = false;
    }
  }
  if (name.find("//") != string::npos) {
    reason=3;
    ok = false;
  }
  if (HasPrefix(name, "./", true) || HasPrefix(name, "../", true)) {
    reason=4;
    ok = false;
  }
  if (HasSuffix(name, "/.", true) || HasSuffix(name, "/..", true)) {
    reason=5;
    ok = false;
  }
  if (name.find("/./") != string::npos || name.find("/../") != string::npos) {
    reason=6;
    ok = false;
  }
  if (name == "") {
    reason=7;
    ok = false;
  }
  if (!ok) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Name [%s] is invalid (reason %d)", name.c_str(), reason);
    assert(0);
  }
  return string(name);
}

static string get_parent(string& path) {
  size_t found = path.find_last_of("/");
  if (found == string::npos) {
    return string("");
  }
  return path.substr(0, found);
}

static string get_basename(string& path) {
  size_t found = path.find_last_of("/");
  if (found == string::npos) {
    return path;
  }
  return path.substr(found + 1);
}

static string acquire_lease(string& key_id, string& secret, string lease_path,
                            string& repo_service_url, bool force_cancel_lease) {
  CURLcode ret = curl_global_init(CURL_GLOBAL_ALL);
  assert(ret == CURLE_OK);
  bool acquired = false;

  while (!acquired) {
    CurlBuffer buffer;
    if (MakeAcquireRequest(key_id, secret, lease_path, repo_service_url,
                           &buffer)) {
      string session_token;

      LeaseReply rep = ParseAcquireReply(buffer, &session_token);
      switch (rep) {
        case kLeaseReplySuccess:
          acquired = true;
          g_lease_acquired = true;
          return session_token;
          break;
        case kLeaseReplyBusy:
          if( force_cancel_lease ) {
            LogCvmfs(kLogCvmfs, kLogStderr, "Lease busy, forcing cancellation (TODO");
          }
          LogCvmfs(kLogCvmfs, kLogStderr, "Lease busy, retrying in %d sec",
                   kLeaseBusyRetryInterval);
          sleep(kLeaseBusyRetryInterval);
          break;
        default:
          LogCvmfs(kLogCvmfs, kLogStderr, "Error acquiring lease: %s. Retrying in %d sec",
                   buffer.data.c_str(), kLeaseBusyRetryInterval);
          sleep(kLeaseBusyRetryInterval);
      }
    } else {
      LogCvmfs(kLogCvmfs, kLogStderr, "Error making lease acquisition request. Retrying in %d sec", kLeaseBusyRetryInterval);
      sleep(kLeaseBusyRetryInterval);
    }
  }
  assert(false);
  return "";
}

static void cancel_lease() {
  CurlBuffer buffer;
  if (MakeEndRequest("DELETE", g_gateway_key_id, g_gateway_secret,
                     g_session_token, g_gateway_url, "", &buffer)) {
    int ret = ParseDropReply(buffer);
    if (kLeaseReplySuccess == ret) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Lease cancelled");
    } else {
      LogCvmfs(kLogCvmfs, kLogStderr, "Lease cancellation failed: %d", ret);
    }
  } else {
    LogCvmfs(kLogCvmfs, kLogStderr, "Lease cancellation request failed");
  }
}

static void on_signal(int sig) {
  signal(sig, SIG_DFL);
  if (g_lease_acquired) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Cancelling lease");
    cancel_lease();
    unlink(g_session_token_file.c_str());
  }
  if (sig == SIGINT || sig == SIGTERM) exit(1);
}

static vector<string> get_all_paths_from_sqlite(vector<string>& sqlite_db_vec,
                                                bool include_additions,
                                                bool include_deletions) {
  int ret;
  vector<string> paths;

  string prefix = "";

  for (vector<string>::iterator it = sqlite_db_vec.begin();
       it != sqlite_db_vec.end(); it++) {
    sqlite3 *db;
    ret = sqlite3_open_v2((*it).c_str(), &db, SQLITE_OPEN_READONLY, NULL);
    CHECK_SQLITE_ERROR(ret, SQLITE_OK);
    relax_db_locking(db);

    vector<string> tables;
    if (include_additions) {
      tables.push_back("dirs");
      tables.push_back("links");
      tables.push_back("files");
    }
    if (include_deletions) {
      tables.push_back("deletions");
    }
    map<string,bool> pathmap;

    // get all the paths from the DB
    for (vector<string>::iterator it = tables.begin(); it != tables.end();
         it++) {
      sqlite3_stmt *stmt;
      string query = "SELECT name FROM " + *it;
      int ret = sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, NULL);
      CHECK_SQLITE_ERROR(ret, SQLITE_OK);
      while (sqlite3_step(stmt) == SQLITE_ROW) {
        char *name = (char *)sqlite3_column_text(stmt, 0);
        string names = sanitise_name(name);
        paths.push_back(names);
        if (*it != "deletions" ) {
          assert(pathmap.find(names) == pathmap.end());
          pathmap[names]=true;
        }
      }
      ret = sqlite3_finalize(stmt);
      CHECK_SQLITE_ERROR(ret, SQLITE_OK);
    }
    ret = sqlite3_close_v2(db);
    CHECK_SQLITE_ERROR(ret, SQLITE_OK);
  }
  return paths;
}

static string get_lease_from_paths(vector<string> paths) {
  string prefix = "";
  bool mismatch = false;
  // first get longest path
  string ref = "";
  for (vector<string>::iterator p = paths.begin(); p != paths.end(); p++) {
    if ((*p).size() > ref.size()) {
      ref = *p;
    }
  }

  for (int i = 1; (size_t)i < ref.size() && !mismatch; i++) {
    string candidate = ref.substr(0, i);
    for (vector<string>::iterator p = paths.begin(); p != paths.end(); p++) {
      if ((*p).rfind(candidate, 0) != 0) {
        mismatch = true;
        break;
      }
    }
    if (!mismatch) {
      prefix = candidate;
    }
  }
  prefix = get_parent(prefix);
  prefix = string("/") + prefix;
  LogCvmfs(kLogCvmfs, kLogStderr, "Longest prefix is %s", prefix.c_str());
  return prefix;
}

// Convert the human-readable ACL into the binary format used to store it in an
// xattr.
static XattrList marshal_xattrs(const char *acl_string) {
  XattrList aclobj;

  if (acl_string == NULL || strlen(acl_string) == 0) {
    return aclobj;
  }

  acl_t acl = acl_from_text(acl_string);  // Convert ACL string to acl_t object
  assert(acl != NULL);
  assert( 0 == acl_valid(acl));

  size_t binary_size;

  char *binary_acl = (char *)acl_to_xattr(acl, &binary_size);

  aclobj.Set("system.posix_acl_access", string(binary_acl, binary_size));
  acl_free(acl);
  free(binary_acl);

  return aclobj;
}

string load_config(const string config_file, const string parameter) {
  ifstream input(config_file.c_str());
  assert(!input.fail());
  vector<string> lines;
  for (string line; getline(input, line);) {
    lines.push_back(line);
  }
  for (vector<string>::iterator it = lines.begin(); it != lines.end(); it++) {
    string l = *it;
    size_t p = l.find("=", 0);
    if (p != string::npos) {
      string key = l.substr(0, p);
      string val = l.substr(p + 1);
      if (key == parameter) {
        return val;
      }
    }
  }
  LogCvmfs(kLogCvmfs, kLogStderr, "Parameter %s not found in config file %s",
           parameter.c_str(), config_file.c_str());
  assert(0);
}

static vector<string> get_file_list(string& path) {
  vector<string> paths;
  const char *cpath = path.c_str();
  struct stat st;
  int ret = stat(cpath, &st);
  assert(ret == 0);

  if (S_ISDIR(st.st_mode)) {
    DIR *d;
    struct dirent *dir;
    d = opendir(cpath);
    if (d) {
      while ((dir = readdir(d)) != NULL) {
        const char *t = strrchr(dir->d_name, '.');
        if (t && !strcmp(t, ".db")) {
          paths.push_back(path + "/" + dir->d_name);
        }
      }
      closedir(d);
    }
  } else {
    paths.push_back(path);
  }
  return paths;
}

int swissknife::IngestSQL::Main(const swissknife::ArgumentList &args) {

  // the catalog code uses assert() liberally.
  // install ABRT signal handler to catch an abort and cancel lease
  signal(SIGABRT, &on_signal);
  signal(SIGINT, &on_signal);
  signal(SIGTERM, &on_signal);

  if (args.find('n') != args.end()) {
    create_empty_database( *args.find('n')->second);
    exit(0);
  }

  string dir_temp = "";
  if (args.find('t') != args.end()) {
    dir_temp = MakeCanonicalPath(*args.find('t')->second);
  } else if (getenv("TMPDIR")) {
    dir_temp = MakeCanonicalPath(getenv("TMPDIR"));
  } else {
    LogCvmfs(kLogCvmfs, kLogStderr, "-t or TMPDIR required");
    return 1;
  }

  if (args.find('C') != args.end()) {
    kConfigDir = MakeCanonicalPath(*args.find('C')->second);
     kConfigDir += "/";
    LogCvmfs(kLogCvmfs, kLogStderr, "Overriding configuration dir prefix to %s", kConfigDir.c_str() );
  } 

  // mandatory arguments
  string repo_name = *args.find('N')->second;
  string sqlite_db_path = *args.find('D')->second;

  vector<string> sqlite_db_vec = get_file_list(sqlite_db_path);

  // optional arguments
  bool allow_deletions = (args.find('d') != args.end());
  bool force_cancel_lease = (args.find('x') != args.end());
  bool allow_additions = !allow_deletions || (args.find('a') != args.end());

  bool enable_corefiles = (args.find('c') != args.end());

  string config_file = kConfigDir + repo_name + "/config";
  string stratum0;
  string proxy;

  if( !enable_corefiles ) {
    struct rlimit rlim;
    rlim.rlim_cur = rlim.rlim_max = 0;
    setrlimit( RLIMIT_CORE, &rlim );
  }

  if (args.find('g') != args.end()) {
    g_gateway_url = *args.find('g')->second;
  } else {
    g_gateway_url = load_config(config_file, "CVMFS_GATEWAY");
  }
  if (args.find('w') != args.end()) {
    stratum0 = *args.find('w')->second;
  } else {
    stratum0 = load_config(config_file, "CVMFS_STRATUM0");
  }

  if (args.find('@') != args.end()) {
    proxy = *args.find('@')->second;
  } else {
    proxy = load_config(config_file, "CVMFS_HTTP_PROXY");
  }

  string lease_path = "/";
  if (args.find('l') != args.end()) {
    lease_path = *args.find('l')->second;
    lease_path = sanitise_name(lease_path.c_str(), true);
    if (!HasPrefix(lease_path, "/", true)) {
      lease_path = string("/") + lease_path;
    }
  } else {
    // lease path wasn't specified, so try to autodetect it
    vector<string> paths = get_all_paths_from_sqlite(
        sqlite_db_vec, allow_additions, allow_deletions);
    if (paths.size() == 0) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Database is empty, nothing to do");
      return 0;  // treat it as a success
    }
    lease_path = get_lease_from_paths(paths);
  }

  if(sqlite_db_vec.size()>1) {
    sqlite_db_vec.insert(sqlite_db_vec.begin(), merge_databases(sqlite_db_vec, dir_temp));
  }

  string public_keys = kConfigDir + repo_name + "/pubkey";
  string key_file = kConfigDir + repo_name + "/gatewaykey";
  
  if( args.find('k') != args.end()) {
     public_keys = *args.find('k')->second;
  }
  if( args.find('s') != args.end()) {
     key_file = *args.find('s')->second;
  }

  assert( access( public_keys.c_str(), R_OK )==0 );
  assert( access( key_file.c_str(), R_OK )==0 );

  string spooler_definition_string = string("gw,,") + g_gateway_url;

  // load gateway lease
  if (!gateway::ReadKeys(key_file, &g_gateway_key_id, &g_gateway_secret)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "gateway::ReadKeys failed");
    return 1;
  }

  // acquire lease and save token to a file in the tmpdir
  LogCvmfs(kLogCvmfs, kLogStderr, "Acquiring gateway lease on %s",
           lease_path.c_str());
  g_session_token = acquire_lease(g_gateway_key_id, g_gateway_secret,
                                  repo_name + lease_path, g_gateway_url, force_cancel_lease);

  char *_tmpfile = strdup( (dir_temp + "/session_token_XXXXXXX").c_str() );
  _tmpfile = mktemp(_tmpfile);

  g_session_token_file = string(_tmpfile);
  free(_tmpfile);

  FILE *fout=fopen( g_session_token_file.c_str(), "wb"); 
  assert(fout!=NULL);
  fputs( g_session_token.c_str(), fout);
  fclose(fout);

  // now initialise the various bits we need

  upload::SpoolerDefinition spooler_definition(
      spooler_definition_string, shash::kSha1, zlib::kZlibDefault, false, true,
      kDefaultMinFileChunkSize, kDefaultAvgFileChunkSize,
      kDefaultMaxFileChunkSize, g_session_token_file, key_file);

  if (args.find('q') != args.end()) {
    spooler_definition.number_of_concurrent_uploads =
        String2Uint64(*args.find('q')->second);
  }

  upload::SpoolerDefinition spooler_definition_catalogs(
      spooler_definition.Dup2DefaultCompression());
  UniquePtr<upload::Spooler> spooler(
      upload::Spooler::Construct(spooler_definition, NULL));
  UniquePtr<upload::Spooler> spooler_catalogs(
      upload::Spooler::Construct(spooler_definition_catalogs, NULL));

  if (!spooler.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "spooler invalid");
    return 1;
  }
  if (!spooler_catalogs.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "spooler_catalogs invalid");
    return 1;
  }
  if (!InitDownloadManager(true, proxy)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "downlaod manager init failed");
    return 1;
  }
  if (!InitVerifyingSignatureManager(public_keys, "")) {
    LogCvmfs(kLogCvmfs, kLogStderr, "signature manager init failed");
    return 1;
  }

  UniquePtr<manifest::Manifest> manifest;

  manifest = FetchRemoteManifest(stratum0, repo_name, shash::Any());

  if (!manifest.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "manifest invalid");
    return 1;
  }

  // get hash of current root catalog, remove terminal "C", encode it
  string old_root_hash = manifest->catalog_hash().ToString(true);
  string hash = old_root_hash.substr(0, old_root_hash.length() - 1);
  shash::Any base_hash =
      shash::MkFromHexPtr(shash::HexPtr(hash), shash::kSuffixCatalog);
  LogCvmfs(kLogCvmfs, kLogStderr, "old_root_hash: %s", old_root_hash.c_str());

  bool is_balanced = false;

  catalog::WritableCatalogManager catalog_manager(
      base_hash, stratum0, dir_temp, spooler_catalogs.weak_ref(),
      download_manager(), false, kDefaultNestedKcatalogLimit,
      kDefaultRootKcatalogLimit, kDefaultFileMbyteLimit, statistics(),
      is_balanced, kDefaultMaxWeight, kDefaultMinWeight);

  catalog_manager.Init();

  // check to see if the lease path is valid - ie it already exists in the repo
#if 0 
  catalog::DirectoryEntry dir;
  if (lease_path != "/" &&
      !catalog_manager.LookupPath(lease_path, catalog::kLookupDefault, &dir)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Lease path %s does not exist. Aborting",
             lease_path.c_str());
    cancel_lease();
    return 1;
  }
#endif 

  // now graft the contents of the DB
  process_sqlite(sqlite_db_vec, catalog_manager, lease_path, allow_additions,
                 allow_deletions);

  // commit changes
  LogCvmfs(kLogCvmfs, kLogStderr, "Committing changes...");
  if (!catalog_manager.Commit(false, false, manifest.weak_ref())) {
    LogCvmfs(kLogCvmfs, kLogStderr, "something went wrong during sync");
    return 1;
  }

  // finalize the spooler
  LogCvmfs(kLogCvmfs, kLogStderr, "Waiting for all uploads to finish...");
  spooler->WaitForUpload();
  spooler_catalogs->WaitForUpload();
  spooler->FinalizeSession(false);

  LogCvmfs(kLogCvmfs, kLogStderr, "Exporting repository manifest");

  // We call FinalizeSession(true) this time, to also trigger the commit
  // operation on the gateway machine (if the upstream is of type "gw").

  // Get the path of the new root catalog
  const string new_root_hash = manifest->catalog_hash().ToString(true);

  if (!spooler_catalogs->FinalizeSession(true, old_root_hash, new_root_hash,
                                         RepositoryTag())) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to commit the transaction");
    // lease is only released on success
    cancel_lease();
    return 1;
  }
  LogCvmfs(kLogCvmfs, kLogStderr, "new_root_hash: %s", new_root_hash.c_str());

  unlink( g_session_token_file.c_str() );
  return 0;
}

void swissknife::IngestSQL::process_sqlite(
    vector<string> sqlite_db_vec,
    catalog::WritableCatalogManager &catalog_manager, string lease,
    bool allow_additions, bool allow_deletions) {
  int ret;
  sqlite3 *db;
  int pass = 0;
  bool has_merged_db = sqlite_db_vec.size() > 1;

  // perform all deletions first
  if (allow_deletions) {
    for (vector<string>::iterator it = sqlite_db_vec.begin(); it != sqlite_db_vec.end(); it++, pass++) {
      if (!has_merged_db || 0 != pass) {
        ret = sqlite3_open_v2(it->c_str(), &db, SQLITE_OPEN_READONLY, NULL);
        CHECK_SQLITE_ERROR(ret, SQLITE_OK);
        relax_db_locking(db);

        LogCvmfs(kLogCvmfs, kLogStderr, "Procesing deletions from %s...", it->c_str());
        ret = do_deletions(db, catalog_manager);
        CHECK_SQLITE_ERROR(ret, SQLITE_OK);

        ret = sqlite3_close_v2(db);
        CHECK_SQLITE_ERROR(ret, SQLITE_OK);
      }
    }
  }

  if (allow_additions) {
    pass = 0;
    for (vector<string>::iterator it = sqlite_db_vec.begin(); it != sqlite_db_vec.end(); it++, pass++) {
      ret = sqlite3_open_v2(it->c_str(), &db, SQLITE_OPEN_READONLY, NULL);
      CHECK_SQLITE_ERROR(ret, SQLITE_OK);
      relax_db_locking(db);

      LogCvmfs(kLogCvmfs, kLogStderr, "Procesing additions from %s...", it->c_str());
      // all directories are guaranteed to be present in the first DB
      if (0 == pass) { 
        ret = add_directories(db, catalog_manager);
        CHECK_SQLITE_ERROR(ret, SQLITE_OK);
      }
      // only attempt to process symlinks/files if this is not the merged
      // DB as that DB only has directories
      if (!has_merged_db || 0 != pass) {
        // symlinks
        ret = add_symlinks(db, catalog_manager);
        CHECK_SQLITE_ERROR(ret, SQLITE_OK);
        // files
        ret = add_files(db, catalog_manager);
        CHECK_SQLITE_ERROR(ret, SQLITE_OK);
      }

      ret = sqlite3_close_v2(db);
      CHECK_SQLITE_ERROR(ret, SQLITE_OK);
    }
  }
}

int swissknife::IngestSQL::add_directories(
    sqlite3 *db, catalog::WritableCatalogManager &catalog_manager) {
  sqlite3_stmt *stmt;
  int ret = sqlite3_prepare_v2(db,
                               "SELECT name, mode, mtime, owner, grp, acl FROM "
                               "dirs ORDER BY length(name) ASC",
                               -1, &stmt, NULL);
  CHECK_SQLITE_ERROR(ret, SQLITE_OK);
  while (sqlite3_step(stmt) == SQLITE_ROW) {
    char *name = (char *)sqlite3_column_text(stmt, 0);
    int64_t mode = sqlite3_column_int64(stmt, 1);
    int64_t mtime = sqlite3_column_int64(stmt, 2);
    int64_t owner = sqlite3_column_int64(stmt, 3);
    int64_t grp = sqlite3_column_int64(stmt, 4);
    char *acl = (char *)sqlite3_column_text(stmt, 5);

    string names = sanitise_name(name);
    catalog::DirectoryEntry dir;
    XattrList xattr = marshal_xattrs(acl);

    bool exists = false;
    exists = catalog_manager.LookupPath(string("/") + names,
                                        catalog::kLookupDefault, &dir);

    if (exists && !S_ISDIR(dir.mode_)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Refusing to replace existing file/symlink at %s with a directory", names.c_str());
      assert(0);
    }

    dir.name_ = NameString(get_basename(names));
    dir.mtime_ = mtime / 1000000000;
    dir.mode_ = mode | S_IFDIR;
    dir.mode_ &= (S_IFDIR | 0777);
    dir.uid_ = owner;
    dir.gid_ = grp;
    dir.has_xattrs_ = (acl || strlen(acl) > 0);

    if (exists) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Touching directory %s", names.c_str());
      catalog_manager.TouchDirectory(dir, xattr, names);
    } else {
      LogCvmfs(kLogCvmfs, kLogStderr, "Adding directory [%s]", names.c_str());
      catalog_manager.AddDirectory(dir, xattr, get_parent(names));
      // now add a .cvmfscatalog file
      // so that manual changes wont remove the nested catalog
      LogCvmfs(kLogCvmfs, kLogStderr, "Placing .cvmfscatalog file in [%s]", names.c_str());
      catalog::DirectoryEntryBase dir2;
      dir2.name_ = NameString(".cvmfscatalog");
      dir2.mtime_ = mtime / 1000000000;
      dir2.mode_ = (S_IFREG | 0666);
      dir2.uid_ = 0;
      dir2.gid_ = 0;
      dir2.has_xattrs_ = 0;
      dir2.checksum_ = shash::MkFromHexPtr(
          shash::HexPtr("da39a3ee5e6b4b0d3255bfef95601890afd80709"),
          shash::kSuffixNone);  // hash of ""
      XattrList xattr2;
      catalog_manager.AddFile(dir2, xattr2, names);

      LogCvmfs(kLogCvmfs, kLogStderr, "Creating Nested Catalog [%s]", names.c_str());
      catalog_manager.CreateNestedCatalog(names);
    }
  }
  ret = sqlite3_finalize(stmt);
  return ret;
}

int swissknife::IngestSQL::add_symlinks(
    sqlite3 *db, catalog::WritableCatalogManager &catalog_manager) {
  sqlite3_stmt *stmt;
  int ret = sqlite3_prepare_v2(
      db, "SELECT name, target, mtime, owner, grp, skip_if_file_or_dir FROM links", -1, &stmt, NULL);
  CHECK_SQLITE_ERROR(ret, SQLITE_OK);

  while (sqlite3_step(stmt) == SQLITE_ROW) {
    char *name_cstr = (char *)sqlite3_column_text(stmt, 0);
    char *target_cstr = (char *)sqlite3_column_text(stmt, 1);
    int64_t mtime = sqlite3_column_int64(stmt, 2);
    int64_t owner = sqlite3_column_int64(stmt, 3);
    int64_t grp = sqlite3_column_int64(stmt, 4);
    int64_t skip_if_file_or_dir = sqlite3_column_int64(stmt, 5);

    string names = sanitise_name(name_cstr);
    string target= sanitise_symlink_target(target_cstr);

    catalog::DirectoryEntry dir;
    catalog::DirectoryEntryBase dir2;
    XattrList xattr;
    bool exists = false;
    exists = catalog_manager.LookupPath(string("/") + names,
                                        catalog::kLookupDefault, &dir);

    dir2.name_ = NameString(get_basename(names));
    dir2.mtime_ = mtime / 1000000000;
    dir2.uid_ = owner;
    dir2.gid_ = grp;
    dir2.has_xattrs_ = false;
    dir2.symlink_ = LinkString(target);
    dir2.mode_ = S_IFLNK | 0777;

    int noop=false;

    if (exists) {
      if(skip_if_file_or_dir) {
        if(S_ISDIR(dir.mode_) || S_ISREG(dir.mode_)) {
          LogCvmfs(kLogCvmfs, kLogStderr, "File or directory for symlink [%s] exists, skipping symlink creation", names.c_str() );
          noop = true;
        } else if (S_ISLNK(dir.mode_)) {
           LogCvmfs(kLogCvmfs, kLogStderr, "Removing existing symlink [%s]", 
               names.c_str());
           catalog_manager.RemoveFile(names);
        } else {
          assert( "Unknown mode for dirent" == NULL );
        }
      } else {
        if (S_ISDIR(dir.mode_)) {
          LogCvmfs(kLogCvmfs, kLogStderr, "Not removing directory [%s]",
                 names.c_str());
          assert(0);
        }
        LogCvmfs(kLogCvmfs, kLogStderr, "Removing existing file [%s]",
               names.c_str());
        catalog_manager.RemoveFile(names);
      }
    }
    if(!noop) {
      string parent = get_parent(names);
      LogCvmfs(kLogCvmfs, kLogStderr, "Adding symlink %s", names.c_str());
      catalog_manager.AddFile(dir2, xattr, parent);
    }
  }
  ret = sqlite3_finalize(stmt);
  return ret;
}

static int check_hash( const char*hash) {
  if (strlen(hash)!=40) { return 1;}
  for(int i=0; i<40; i++ ) {
   // < '0' || > 'f' || ( > '9' && < 'a' )
   if( hash[i]<0x30 || hash[i]>0x66 || ( hash[i]>0x39 && hash[i] <0x61 )) {
    return 1;
   }
  }
  return 0;
}

int swissknife::IngestSQL::add_files(
    sqlite3 *db, catalog::WritableCatalogManager &catalog_manager) {
  sqlite3_stmt *stmt;
  int ret = sqlite3_prepare_v2(
      db, "SELECT name, mode, mtime, owner, grp, size, hashes, internal FROM files", -1,
      &stmt, NULL);
  CHECK_SQLITE_ERROR(ret, SQLITE_OK);
  while (sqlite3_step(stmt) == SQLITE_ROW) {
    char *name = (char *)sqlite3_column_text(stmt, 0);
    int64_t mode = sqlite3_column_int64(stmt, 1);
    int64_t mtime = sqlite3_column_int64(stmt, 2);
    int64_t owner = sqlite3_column_int64(stmt, 3);
    int64_t grp = sqlite3_column_int64(stmt, 4);
    int64_t size = sqlite3_column_int64(stmt, 5);
    char *hashes_cstr = (char *)sqlite3_column_text(stmt, 6);
    int64_t internal = sqlite3_column_int64(stmt, 7);

    string names = sanitise_name(name);
    catalog::DirectoryEntry dir;
    XattrList xattr;
    bool exists = false;
    exists = catalog_manager.LookupPath(string("/") + names,
                                        catalog::kLookupDefault, &dir);

    dir.name_ = NameString(get_basename(names));
    dir.mtime_ = mtime / 1000000000;
    dir.mode_ = mode | S_IFREG;
    dir.mode_ &= (S_IFREG | 0777);
    dir.uid_ = owner;
    dir.gid_ = grp;
    dir.size_ = size;
    dir.has_xattrs_ = false;
    FileChunkList file_chunks;
    dir.is_external_file_ = !internal;
    dir.set_is_chunked_file(true);
    dir.checksum_ = shash::MkFromHexPtr(
        shash::HexPtr("0000000000000000000000000000000000000000"),
        shash::kSuffixNone);
    dir.compression_algorithm_ = zlib::kNoCompression;
    // tokenize hashes
    char *ref;
    char *tok;
    tok = strtok_r(hashes_cstr, ",", &ref);
    vector<off_t> offsets;
    vector<size_t> sizes;
    vector<shash::Any> hashes;
    off_t offset = 0;

    assert( size>=0 );
    while (tok) {
      offsets.push_back(offset);
      // TODO: check the hash format
      assert( check_hash(tok)==0 );
      hashes.push_back(
          shash::MkFromHexPtr(shash::HexPtr(tok), shash::kSuffixNone));
      tok = strtok_r(NULL, ",", &ref);
      offset += kChunkSize;  // TODO: in the future we might want variable chunk
                             // sizes specified in the DB
    }
    size_t expected_num_chunks = size/kChunkSize;
    if (expected_num_chunks * (size_t)kChunkSize < (size_t) size || size==0 ) { expected_num_chunks++; }
    assert(offsets.size() == expected_num_chunks );
    for (long int i = 0; i < (long int) offsets.size() - 1; i++) {  
      sizes.push_back(size_t(offsets[i + 1] - offsets[i]));
    }

    sizes.push_back(size_t(dir.size_ - offsets[offsets.size() - 1]));
    for (long unsigned int i = 0; i < offsets.size(); i++) {
      FileChunk chunk = FileChunk(hashes[i], offsets[i], sizes[i]);
      file_chunks.PushBack(chunk);
    }

    if (exists) {
      if (S_ISDIR(dir.mode_) || S_ISLNK(dir.mode_)) {
        LogCvmfs(kLogCvmfs, kLogStderr, "Refusing to replace existing dir/symlink at %s with a file",
                 names.c_str());
        assert(0);
      }
      LogCvmfs(kLogCvmfs, kLogStderr, "Removing existing file %s",
               names.c_str());
      catalog_manager.RemoveFile(names);
    }
    string parent = get_parent(names);
    LogCvmfs(kLogCvmfs, kLogStderr, "Adding chunked file %s", names.c_str());
    catalog_manager.AddChunkedFile(dir, xattr, parent, file_chunks);
  }
  ret = sqlite3_finalize(stmt);
  return ret;
}

int swissknife::IngestSQL::do_deletions(
    sqlite3 *db, catalog::WritableCatalogManager &catalog_manager) {
  sqlite3_stmt *stmt;
  int ret = sqlite3_prepare_v2(
      db, "SELECT name, directory, file, link FROM deletions ORDER BY length(name) DESC",
      -1, &stmt, NULL);
  CHECK_SQLITE_ERROR(ret, SQLITE_OK);
  while (sqlite3_step(stmt) == SQLITE_ROW) {
    char *name = (char *)sqlite3_column_text(stmt, 0);
    int64_t isdir  = sqlite3_column_int64(stmt, 1);
    int64_t isfile = sqlite3_column_int64(stmt, 2);
    int64_t islink = sqlite3_column_int64(stmt, 3);

    string names = sanitise_name(name);

    catalog::DirectoryEntry dirent;
    bool exists = false;
    exists = catalog_manager.LookupPath(string("/") + names,
                                        catalog::kLookupDefault, &dirent);
    if(exists) {
      if(    (isdir  && S_ISDIR(dirent.mode_) )
          || (islink && S_ISLNK(dirent.mode_) )
          || (isfile && S_ISREG(dirent.mode_) ) ) {
        if (S_ISDIR(dirent.mode_)) {
          PathString names_path(names);
          recursively_delete_directory(names_path, catalog_manager);
        } else {
          LogCvmfs(kLogCvmfs, kLogStderr, "Removing link/file %s", names.c_str());
          catalog_manager.RemoveFile(names);
        }
      } else {
        LogCvmfs(kLogCvmfs, kLogStderr, "Mismatch in deletion type, not deleting: %s dir %d/%d , link %d/%d, file %d/%d", names.c_str(), isdir,  S_ISDIR(dirent.mode_), islink, S_ISLNK(dirent.mode_), isfile, S_ISREG(dirent.mode_) );
      }
    } else {
      LogCvmfs(kLogCvmfs, kLogStderr, "Not Removing non-existent %s",
               names.c_str());
    }
  }
  ret = sqlite3_finalize(stmt);
  return ret;
}



const char*schema[] = {
 "PRAGMA journal_mode=WAL;",

 "CREATE TABLE IF NOT EXISTS dirs ( \
        name  TEXT    PRIMARY KEY, \
        mode  INTEGER NOT NULL DEFAULT 493,\
        mtime INTEGER NOT NULL DEFAULT 0,\
        owner INTEGER NOT NULL DEFAULT 0, \
        grp   INTEGER NOT NULL DEFAULT 0, \
        acl   TEXT    NOT NULL DEFAULT '');",

 "CREATE TABLE IF NOT EXISTS files ( \
        name   TEXT    PRIMARY KEY, \
        mode   INTEGER NOT NULL DEFAULT 420, \
        mtime  INTEGER NOT NULL DEFAULT 0,\
        owner  INTEGER NOT NULL DEFAULT 0,\
        grp    INTEGER NOT NULL DEFAULT 0,\
        size   INTEGER NOT NULL DEFAULT 0,\
        hashes TEXT    NOT NULL DEFAULT '',\
        internal INTEGER NOT NULL DEFAULT 0\
  );"

 "CREATE TABLE IF NOT EXISTS links (\
        name   TEXT    PRIMARY KEY,\
        target TEXT    NOT NULL DEFAULT '',\
        mtime  INTEGER NOT NULL DEFAULT 0,\
        owner  INTEGER NOT NULL DEFAULT 0,\
        grp    INTEGER NOT NULL DEFAULT 0,\
        skip_if_file_or_dir INTEGER NOT NULL DEFAULT 0\
  );",

 "CREATE TABLE IF NOT EXISTS deletions (\
        name      TEXT PRIMARY KEY,\
        directory INTEGER NOT NULL DEFAULT 0,\
        file      INTEGER NOT NULL DEFAULT 0,\
        link      INTEGER NOT NULL DEFAULT 0\
   );",
  NULL
};

static void create_empty_database( string& filename ) {
  sqlite3 *db_out;
  int ret = sqlite3_open_v2(filename.c_str(), &db_out, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
  CHECK_SQLITE_ERROR(ret, SQLITE_OK);
  relax_db_locking(db_out);
  
  const char **ptr = schema;
  while (*ptr!=NULL) {
    ret = sqlite3_exec(db_out, *ptr, NULL, NULL, NULL);
    CHECK_SQLITE_ERROR(ret, SQLITE_OK);
    ptr++;
  } 
  sqlite3_close(db_out);
}

static string merge_databases(vector<string>& sqlite_db_vec, string& tmpdir) {
  // create a new database file
  char *tmpfile = strdup((tmpdir + "/merged_db_XXXXXXX").c_str());
  mktemp(tmpfile);

  LogCvmfs(kLogCvmfs, kLogStderr, "Merging databases to %s...", tmpfile ); 
  sqlite3 *db_out;
  int ret = sqlite3_open_v2(tmpfile, &db_out, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
  CHECK_SQLITE_ERROR(ret, SQLITE_OK);
  relax_db_locking(db_out);
  
  const char **ptr = schema;
  while (*ptr!=NULL) {
    ret = sqlite3_exec(db_out, *ptr, NULL, NULL, NULL);
    CHECK_SQLITE_ERROR(ret, SQLITE_OK);
    ptr++;
  } 

  ret = sqlite3_exec(db_out, "PRAGMA synchronous=OFF", NULL, NULL, NULL);
  CHECK_SQLITE_ERROR(ret, SQLITE_OK);
  ret = sqlite3_exec(db_out, "PRAGMA journal_mode=MEMORY", NULL, NULL, NULL);
  CHECK_SQLITE_ERROR(ret, SQLITE_OK);
  ret = sqlite3_exec(db_out, "PRAGMA temp_store=MEMORY", NULL, NULL, NULL);
  CHECK_SQLITE_ERROR(ret, SQLITE_OK);
  ret = sqlite3_exec(db_out, "BEGIN TRANSACTION", NULL, NULL, NULL);
  CHECK_SQLITE_ERROR(ret, SQLITE_OK);

  sqlite3_stmt *stmt_out;
  ret = sqlite3_prepare_v2(db_out, 
                               "INSERT OR IGNORE INTO dirs ( name, mode, mtime, owner, grp, acl ) VALUES ( ?, ?, ?, ?, ?, ? )", -1, &stmt_out, NULL );
  CHECK_SQLITE_ERROR(ret, SQLITE_OK); 

  for (vector<string>::iterator it = sqlite_db_vec.begin();
       it != sqlite_db_vec.end(); it++) {
    sqlite3 *db;
    sqlite3_stmt *stmt_in;
    LogCvmfs(kLogCvmfs, kLogStderr, "Merging from %s", (*it).c_str() );
    ret = sqlite3_open_v2((*it).c_str(), &db, SQLITE_OPEN_READONLY, NULL);
    CHECK_SQLITE_ERROR(ret, SQLITE_OK);
    relax_db_locking(db);

    int ret = sqlite3_prepare_v2(db,
                               "SELECT name, mode, mtime, owner, grp, acl FROM "
                               "dirs ORDER BY length(name) ASC",
                               -1, &stmt_in, NULL);
    CHECK_SQLITE_ERROR(ret, SQLITE_OK); 

    while (sqlite3_step(stmt_in) == SQLITE_ROW) {
      char *name = (char *)sqlite3_column_text(stmt_in, 0);
      int64_t mode = sqlite3_column_int64(stmt_in, 1);
      int64_t mtime = sqlite3_column_int64(stmt_in, 2);
      int64_t owner = sqlite3_column_int64(stmt_in, 3);
      int64_t grp = sqlite3_column_int64(stmt_in, 4);
      char *acl = (char *)sqlite3_column_text(stmt_in, 5);

      ret = sqlite3_reset(stmt_out);
      CHECK_SQLITE_ERROR(ret, SQLITE_OK);
      ret = sqlite3_bind_text (stmt_out, 1, name, -1, NULL); 
      CHECK_SQLITE_ERROR(ret, SQLITE_OK);
      ret = sqlite3_bind_int64(stmt_out, 2, mode);
      CHECK_SQLITE_ERROR(ret, SQLITE_OK);
      ret = sqlite3_bind_int64(stmt_out, 3, mtime);
      CHECK_SQLITE_ERROR(ret, SQLITE_OK);
      ret = sqlite3_bind_int64(stmt_out, 4, owner);
      CHECK_SQLITE_ERROR(ret, SQLITE_OK);
      ret = sqlite3_bind_int64(stmt_out, 5, grp);
      CHECK_SQLITE_ERROR(ret, SQLITE_OK);
      ret = sqlite3_bind_text (stmt_out, 6, acl, -1, NULL); 
      CHECK_SQLITE_ERROR(ret, SQLITE_OK);
      ret = sqlite3_step(stmt_out);
      CHECK_SQLITE_ERROR(ret, SQLITE_DONE);
    }
    sqlite3_finalize(stmt_in);
    sqlite3_close(db);
  }

  ret = sqlite3_exec(db_out, "COMMIT TRANSACTION", NULL, NULL, NULL );
  CHECK_SQLITE_ERROR(ret, SQLITE_OK); 
  sqlite3_close(db_out);
  string tmpfile_s = string(tmpfile);
  free(tmpfile);
  return tmpfile_s;

}


static void recursively_delete_directory(PathString& path, catalog::WritableCatalogManager &catalog_manager) {
   catalog::DirectoryEntryList listing;

  // Add all names
   catalog::StatEntryList listing_from_catalog;
   bool retval = catalog_manager.ListingStat(PathString( "/" +  path.ToString()), &listing_from_catalog);

   assert(retval);

  if(!catalog_manager.IsTransitionPoint(path.ToString())) {

   for (unsigned i = 0; i < listing_from_catalog.size(); ++i) {
    PathString entry_path;
    entry_path.Assign(path);
    entry_path.Append("/", 1);
    entry_path.Append(listing_from_catalog.AtPtr(i)->name.GetChars(),
                      listing_from_catalog.AtPtr(i)->name.GetLength());

    if( S_ISDIR( listing_from_catalog.AtPtr(i)->info.st_mode) ) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Recursing into %s/", entry_path.ToString().c_str());
      recursively_delete_directory(entry_path, catalog_manager);


    } else {
      LogCvmfs(kLogCvmfs, kLogStderr, " Recursively removing %s", entry_path.ToString().c_str());
      catalog_manager.RemoveFile(entry_path.ToString());
    }

   }
  } else { 
      LogCvmfs(kLogCvmfs, kLogStderr, "Removing nested catalog %s", path.ToString().c_str());
      catalog_manager.RemoveNestedCatalog(path.ToString(), false);
  }
  LogCvmfs(kLogCvmfs, kLogStderr, "Removing directory %s", path.ToString().c_str());
     catalog_manager.RemoveDirectory(path.ToString());

}


static void relax_db_locking(sqlite3 *db) {
    int ret=0;
    ret = sqlite3_exec(db, "PRAGMA temp_store=2", NULL, NULL, NULL);
    CHECK_SQLITE_ERROR(ret, SQLITE_OK);
    ret = sqlite3_exec(db, "PRAGMA synchronous=OFF", NULL, NULL, NULL);
    CHECK_SQLITE_ERROR(ret, SQLITE_OK);
}
