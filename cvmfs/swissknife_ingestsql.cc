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
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <stack>

#include "acl.h"
#include "catalog_mgr_rw.h"
#include "curl/curl.h"
#include "gateway_util.h"
#include "swissknife_lease_curl.h"
#include "swissknife_lease_json.h"
#include "swissknife_sync.h"
#include "upload.h"
#include "util/logging.h"
#include "catalog_downloader.h"
#include "shortstring.h"

#define CHECK_SQLITE_ERROR(ret, expected) do {                                                            \
                                            if ((ret)!=expected) {                                        \
                                              LogCvmfs(kLogCvmfs, kLogStderr, "SQLite error: %d", (ret)); \
                                              assert(0);                                                  \
                                            }                                                             \
                                          } while (0)

#define CUSTOM_ASSERT(check, msg, ...) do {                                                     \
                                         if (!(check)) {                                        \
                                           LogCvmfs(kLogCvmfs, kLogStderr, msg, ##__VA_ARGS__); \
                                           assert(0);                                           \
                                         }                                                      \
                                       } while (0)

#define SHOW_PROGRESS(item, freq, curr, total) do {                                                                            \
                                                 if ((curr) % freq == 0 || (curr) == total) {                                  \
                                                   LogCvmfs(kLogCvmfs, kLogStdout, "Processed %d/%d %s", (curr), total, item); \
                                                 }                                                                             \
                                               } while (0)



static const unsigned kExternalChunkSize = 24 * 1024 * 1024;
static const unsigned kInternalChunkSize = 6  * 1024 * 1024;
static const unsigned kDefaultLeaseBusyRetryInterval = 10;
static const unsigned kLeaseRefreshInterval = 90; // seconds


static bool g_lease_acquired = false;
static string g_gateway_url;
static string g_gateway_key_id;
static string g_gateway_secret;
static string g_session_token;
static string g_session_token_file;
static string g_s3_file;
static time_t g_last_lease_refresh=0;
static bool   g_stop_refresh = false;
static int64_t g_priority=0;
static bool   g_add_missing_catalogs = false;
static string get_lease_from_paths(vector<string> paths);
static vector<string> get_all_dirs_from_sqlite(vector<string>& sqlite_db_vec,
                                                bool include_additions,
                                                bool include_deletions);
static string get_parent(const string& path);
static string get_basename(const string& path);

static XattrList marshal_xattrs(const char *acl);
static string sanitise_name(const char *name_cstr, bool allow_leading_slash);
static void on_signal(int sig);
static string acquire_lease(const string& key_id, const string& secret, const string& lease_path,
                            const string& repo_service_url, bool force_cancel_lease, uint64_t *current_revision, string &current_root_hash,
                            unsigned int refresh_interval);
static void cancel_lease();
static void refresh_lease();
static vector<string> get_file_list(string& path);
static int check_hash(const char *hash) ;
static void recursively_delete_directory(PathString& path, catalog::WritableCatalogManager &catalog_manager);
static void create_empty_database( string& filename );
static void relax_db_locking(sqlite3 *db);
static bool check_prefix(const std::string &path , const std::string &prefix); 

static bool isDatabaseMarkedComplete(const char *dbfile);
static void setDatabaseMarkedComplete(const char *dbfile);

extern "C" void* lease_refresh_thread(void *payload);
 
static string sanitise_name(const char *name_cstr,
                            bool allow_leading_slash = false) {
  int reason = 0;
  const char *c = name_cstr;
  while(*c == '/') {c++;} // strip any leading slashes
  string const name = string(c);
  bool ok = true;

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
  CUSTOM_ASSERT(ok, "Name [%s] is invalid (reason %d)", name.c_str(), reason);
  return string(name);
}

static string get_parent(const string& path) {
  size_t const found = path.find_last_of('/');
  if (found == string::npos) {
    return string("");
  }
  return path.substr(0, found);
}

static string get_basename(const string& path) {
  const size_t found = path.find_last_of('/');
  if (found == string::npos) {
    return path;
  }
  return path.substr(found + 1);
}

// this is copied from MakeRelativePath
static string MakeCatalogPath(const std::string &relative_path) {
  return (relative_path == "") ? "" : "/" + relative_path;
}

static string acquire_lease(const string& key_id, const string& secret, const string& lease_path,
                            const string& repo_service_url, bool force_cancel_lease, uint64_t *current_revision, string &current_root_hash,
                            unsigned int refresh_interval) {
  const CURLcode ret = curl_global_init(CURL_GLOBAL_ALL);
  CUSTOM_ASSERT(ret == CURLE_OK, "failed to init curl");

  string gateway_metadata_str;
  char *gateway_metadata = getenv("CVMFS_GATEWAY_METADATA");
  if (gateway_metadata != NULL) gateway_metadata_str = gateway_metadata;

  while (true) {
    CurlBuffer buffer;
    if (MakeAcquireRequest(key_id, secret, lease_path, repo_service_url,
                           &buffer, gateway_metadata_str)) {
      string session_token;

      const LeaseReply rep = ParseAcquireReplyWithRevision(buffer, &session_token, current_revision, current_root_hash);
      switch (rep) {
        case kLeaseReplySuccess:
          g_lease_acquired = true;
          g_last_lease_refresh = time(NULL);
          return session_token;
          break;
        case kLeaseReplyBusy:
          if( force_cancel_lease ) {
            LogCvmfs(kLogCvmfs, kLogStderr, "Lease busy, forcing cancellation (TODO");
          }
          LogCvmfs(kLogCvmfs, kLogStderr, "Lease busy, retrying in %d sec",
                   refresh_interval);
          sleep(refresh_interval);
          break;
        default:
          LogCvmfs(kLogCvmfs, kLogStderr, "Error acquiring lease: %s. Retrying in %d sec",
                   buffer.data.c_str(), refresh_interval);
          sleep(refresh_interval);
      }
    } else {
      LogCvmfs(kLogCvmfs, kLogStderr, "Error making lease acquisition request. Retrying in %d sec", refresh_interval);
      sleep(refresh_interval);
    }
  }
  assert(false);
  return "";
}

static uint64_t make_commit_on_gateway( const std::string &old_root_hash, const std::string &new_root_hash, int64_t priority) {
  CurlBuffer buffer;
  char priorityStr[100];
  (void)sprintf(priorityStr, "%" PRId64, priority); // skipping return value check; no way such large buffer will overflow
  buffer.data="";

  const std::string payload = "{\n\"old_root_hash\": \"" + old_root_hash + "\",\n\"new_root_hash\": \""+new_root_hash+"\",\n\"priority\": "+priorityStr+"}";

  return MakeEndRequest("POST", g_gateway_key_id, g_gateway_secret,
                     g_session_token, g_gateway_url, payload,  &buffer, true);
}

static void refresh_lease() {
  CurlBuffer buffer;
  buffer.data="";
  if ( (time(NULL)- g_last_lease_refresh )< kLeaseRefreshInterval ){ return; }

  if (MakeEndRequest("PATCH", g_gateway_key_id, g_gateway_secret,
                     g_session_token, g_gateway_url, "", &buffer,false)) {
    const int ret = ParseDropReply(buffer);
    if (kLeaseReplySuccess == ret) {
      LogCvmfs(kLogCvmfs, kLogVerboseMsg, "Lease refreshed");
      g_last_lease_refresh=time(NULL);
    } else {
      LogCvmfs(kLogCvmfs, kLogStderr, "Lease refresh failed: %d", ret);
    }
  } else {
    LogCvmfs(kLogCvmfs, kLogStderr, "Lease refresh request failed");
    if(buffer.data == "Method Not Allowed\n") {
      g_last_lease_refresh=time(NULL);
      LogCvmfs(kLogCvmfs, kLogStderr, "This gateway does not support lease refresh");
    } 
    
  }
}


static void cancel_lease() {
  CurlBuffer buffer;
  if (MakeEndRequest("DELETE", g_gateway_key_id, g_gateway_secret,
                     g_session_token, g_gateway_url, "", &buffer, false)) {
    const int ret = ParseDropReply(buffer);
    if (kLeaseReplySuccess == ret) {
      LogCvmfs(kLogCvmfs, kLogStdout, "Lease cancelled");
    } else {
      LogCvmfs(kLogCvmfs, kLogStderr, "Lease cancellation failed: %d", ret);
    }
  } else {
    LogCvmfs(kLogCvmfs, kLogStderr, "Lease cancellation request failed");
  }
  g_stop_refresh=true;
}

static void on_signal(int sig) {
  (void)signal(sig, SIG_DFL);
  if (g_lease_acquired) {
    LogCvmfs(kLogCvmfs, kLogStdout, "Cancelling lease");
    cancel_lease();
    unlink(g_session_token_file.c_str());
  }
  if (sig == SIGINT || sig == SIGTERM) exit(1);
}

static vector<string> get_all_dirs_from_sqlite(vector<string>& sqlite_db_vec,
                                                bool include_additions,
                                                bool include_deletions) {
  int ret;
  vector<string> paths;

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

    // get all the paths from the DB
    for (vector<string>::iterator it = tables.begin(); it != tables.end();
         it++) {
      sqlite3_stmt *stmt;
      const string query = "SELECT name FROM " + *it;
      ret = sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, NULL);
      CHECK_SQLITE_ERROR(ret, SQLITE_OK);
      while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *name = reinterpret_cast<const char *>(sqlite3_column_text(stmt, 0));
        const string names = sanitise_name(name);
        if (*it=="dirs") { 
          paths.push_back(names);
        } else {
          paths.push_back(get_parent(names));
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

static int get_db_schema_revision(sqlite3 *db, const std::string &db_name = "") {
  sqlite3_stmt *stmt;
  std::ostringstream stmt_str;
  stmt_str << "SELECT value FROM " << db_name << "properties WHERE key = 'schema_revision'";
  int ret = sqlite3_prepare_v2(db, stmt_str.str().c_str(), -1, &stmt, NULL);
  CHECK_SQLITE_ERROR(ret, SQLITE_OK);

  ret = sqlite3_step(stmt);
  // if table exists, we require that it must have a schema_revision row
  CHECK_SQLITE_ERROR(ret, SQLITE_ROW);
  const std::string schema_revision_str(reinterpret_cast<const char *>(sqlite3_column_text(stmt, 0)));
  CHECK_SQLITE_ERROR(sqlite3_finalize(stmt), SQLITE_OK);
  return std::stoi(schema_revision_str);
}

static int get_row_count(sqlite3 *db, const std::string &table_name) {
  sqlite3_stmt *stmt;
  std::ostringstream stmt_str;
  stmt_str << "SELECT COUNT(*) FROM " << table_name;
  int ret = sqlite3_prepare_v2(db, stmt_str.str().c_str(), -1, &stmt, NULL);
  CHECK_SQLITE_ERROR(ret, SQLITE_OK);

  ret = sqlite3_step(stmt);
  CHECK_SQLITE_ERROR(ret, SQLITE_ROW);
  const std::string count_str(reinterpret_cast<const char *>(sqlite3_column_text(stmt, 0)));
  CHECK_SQLITE_ERROR(sqlite3_finalize(stmt), SQLITE_OK);
  return std::stoi(count_str);
}

static int calculate_print_frequency(int total) {
  int base = 1000;
  while (base * 50 < total) base *= 10;
  return base;
}

// compute a common path among for paths for use as the lease path
static string get_lease_from_paths(vector<string> paths) {
  CUSTOM_ASSERT(!paths.empty(), "no paths are provided");

  // we'd have to ensure path is relative
  // (probably best to check this elsewhere as it is not just a requirement for this function)
  auto lease = PathString(paths.at(0));
  for (auto it = paths.begin() + 1; it != paths.end(); ++it) {
    auto path = PathString(*it);
    // shrink the lease path until it is a parent of "path"
    while (!IsSubPath(lease, path)) {
        auto i = lease.GetLength() - 1;
        for (; i >= 0; --i) {
            if (lease.GetChars()[i] == '/' || i == 0) {
                lease.Truncate(i);
                break;
            }
        }
    }
    if (lease.IsEmpty()) break; // early stop if lease is already at the root
  }

  auto prefix = "/" + lease.ToString();

  LogCvmfs(kLogCvmfs, kLogStdout, "Longest prefix is %s", prefix.c_str());
  return prefix;
}

static XattrList marshal_xattrs(const char *acl_string) {
  XattrList aclobj;

  if (acl_string == NULL || acl_string[0] == '\0') {
    return aclobj;
  }

  bool equiv_mode;
  size_t binary_size;
  char *binary_acl;
  const int ret = acl_from_text_to_xattr_value(string(acl_string), binary_acl, binary_size, equiv_mode);
  if (ret) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failure of acl_from_text_to_xattr_value(%s)", acl_string);
    assert(0); // TODO(vavolkl): incorporate error handling other than asserting
    return aclobj;
  }
  if (!equiv_mode) {
    CUSTOM_ASSERT(aclobj.Set("system.posix_acl_access", string(binary_acl, binary_size)), "failed to set system.posix_acl_access (ACL size %ld)", binary_size);
    free(binary_acl);
  }

  return aclobj;
}

std::unordered_map<string, string> load_config(const string& config_file) {
  std::unordered_map<string, string> config_map;
  ifstream input(config_file);
  if (!input) {
    LogCvmfs(kLogCvmfs, kLogStderr, "could not open config file %s", config_file.c_str());
    return config_map;
  }
  vector<string> lines;
  for (string line; getline(input, line);) {
    lines.push_back(line);
  }

  for (auto it = lines.begin(); it != lines.end(); it++) {
    const string l = *it;
    const size_t p = l.find('=', 0);
    if (p != string::npos) {
      const string key = l.substr(0, p);
      string val = l.substr(p + 1);
      // trim any double quotes
      if (val.front() == '"') {
        val = val.substr(1, val.length() - 2);
      }
      config_map[key] = val;
    }
  }

  return config_map;
}

string retrieve_config(std::unordered_map<string, string> &config_map, const string& key) {
  auto kv = config_map.find(key);
  CUSTOM_ASSERT(kv != config_map.end(), "Parameter %s not found in config", key.c_str());
  return kv->second;
}

static vector<string> get_file_list(string& path) {
  vector<string> paths;
  const char *cpath = path.c_str();
  struct stat st;
  const int ret = stat(cpath, &st);
  CUSTOM_ASSERT(ret == 0, "failed to stat file %s", cpath);

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

extern bool g_log_with_time;

int swissknife::IngestSQL::Main(const swissknife::ArgumentList &args) {

  // the catalog code uses assert() liberally.
  // install ABRT signal handler to catch an abort and cancel lease
  if (
    signal(SIGABRT, &on_signal) == SIG_ERR
    || signal(SIGINT, &on_signal) == SIG_ERR
    || signal(SIGTERM, &on_signal) == SIG_ERR
  ) {
    LogCvmfs(kLogCvmfs, kLogStdout, "Setting signal handlers failed");
    exit(1);
  }

  const bool enable_corefiles = (args.find('c') != args.end());
  if( !enable_corefiles ) {
    struct rlimit rlim;
    rlim.rlim_cur = rlim.rlim_max = 0;
    setrlimit( RLIMIT_CORE, &rlim );
  }


  if (args.find('n') != args.end()) {
    create_empty_database( *args.find('n')->second);
    exit(0);
  }

  //TODO(@vvolkl): add 'B' option to wait_for_update
  //TODO(@vvolkl): add 'T' option for ttl


  if (args.find('P') != args.end()) {
    const char *arg = (*args.find('P')->second).c_str();
    char* at_null_terminator_if_number;
    g_priority = strtoll(arg, &at_null_terminator_if_number, 10);
    if (*at_null_terminator_if_number != '\0') {
      LogCvmfs(kLogCvmfs, kLogStderr, "Priority parameter value '%s' parsing failed", arg);
      return 1;
    }
  } else {
    g_priority = -time(NULL);
  }


  unsigned int lease_busy_retry_interval = kDefaultLeaseBusyRetryInterval;
  if (args.find('r') != args.end()) {
    lease_busy_retry_interval = atoi((*args.find('r')->second).c_str());
  }

  string dir_temp = "";
  const char *env_tmpdir;
  if (args.find('t') != args.end()) {
    dir_temp = MakeCanonicalPath(*args.find('t')->second);
  } else if (env_tmpdir = getenv("TMPDIR")) {
    dir_temp = MakeCanonicalPath(env_tmpdir);
  } else {
    LogCvmfs(kLogCvmfs, kLogStderr, "-t or TMPDIR required");
    return 1;
  }

  string kConfigDir("/etc/cvmfs/gateway-client/");
  if (args.find('C') != args.end()) {
    kConfigDir = MakeCanonicalPath(*args.find('C')->second);
     kConfigDir += "/";
    LogCvmfs(kLogCvmfs, kLogStdout, "Overriding configuration dir prefix to %s", kConfigDir.c_str() );
  } 

  // mandatory arguments
  string const repo_name = *args.find('N')->second;
  string sqlite_db_path = *args.find('D')->second;

  vector<string> sqlite_db_vec = get_file_list(sqlite_db_path);

  // optional arguments
  bool const allow_deletions = (args.find('d') != args.end());
  bool const force_cancel_lease = (args.find('x') != args.end());
  bool const allow_additions = !allow_deletions || (args.find('a') != args.end());
  g_add_missing_catalogs = ( args.find('z') != args.end());
  bool const check_completed_graft_property = ( args.find('Z') != args.end());
  if (args.find('v') != args.end()) {
    SetLogVerbosity(kLogVerbose);
  }

  if(check_completed_graft_property) {
    if(sqlite_db_vec.size()!=1) {
      LogCvmfs(kLogCvmfs, kLogStderr, "-Z requires a single DB file");
      exit(1);
    }
    if(isDatabaseMarkedComplete(sqlite_db_vec[0].c_str())) {
      LogCvmfs(kLogCvmfs, kLogStderr, "DB file is already marked as completed_graft");
      exit(0);
    } else {
      LogCvmfs(kLogCvmfs, kLogStderr, "DB file is not marked as completed_graft");
    }
  }

  string const config_file = kConfigDir + repo_name + "/config";
  string stratum0;
  string proxy;

  string additional_prefix="";
  bool   has_additional_prefix=false;
  if (args.find('p') != args.end()) {
      additional_prefix=*args.find('p')->second;
      additional_prefix = sanitise_name(additional_prefix.c_str(), true);
      if( additional_prefix.back() != '/' ) {
        additional_prefix += "/";
      }
      has_additional_prefix= true;
      LogCvmfs(kLogCvmfs, kLogStdout, "Adding additional prefix %s to lease and all paths", additional_prefix.c_str() );
      // now we are confident that any additional prefix has no leading / and does have a tailing /
  }
  auto config_map = load_config(config_file);

  if (args.find('g') != args.end()) {
    g_gateway_url = *args.find('g')->second;
  } else {
    g_gateway_url = retrieve_config(config_map, "CVMFS_GATEWAY");
  }
  if (args.find('w') != args.end()) {
    stratum0 = *args.find('w')->second;
  } else {
    stratum0 = retrieve_config(config_map, "CVMFS_STRATUM0");
  }

  if (args.find('@') != args.end()) {
    proxy = *args.find('@')->second;
  } else {
    proxy = retrieve_config(config_map, "CVMFS_HTTP_PROXY");
  }

  string lease_path="";
  //bool lease_autodetected = false;
  if (args.find('l') != args.end()) {
    lease_path = *args.find('l')->second;
  } else {
    // lease path wasn't specified, so try to autodetect it
    vector<string> const paths = get_all_dirs_from_sqlite(
        sqlite_db_vec, allow_additions, allow_deletions);
    if (paths.size() == 0) {
      LogCvmfs(kLogCvmfs, kLogStdout, "Database is empty, nothing to do");
      return 0;  // treat it as a success
    }
    lease_path = get_lease_from_paths(paths);
    //lease_autodetected = true;
  }

  if (has_additional_prefix) {
    if (lease_path == "/" ) { lease_path = "/" + additional_prefix; }
    else { 
      if ( lease_path.substr(0,1)=="/" ) {
        lease_path = "/" + additional_prefix + lease_path.substr(1, lease_path.size()-1);
      } else {
        lease_path = "/" + additional_prefix + lease_path;   // prefix is certain to have a trailing /
      }
    }
  }
  if (lease_path.substr(0,1)!="/") { lease_path = "/" + lease_path; }
  LogCvmfs(kLogCvmfs, kLogStdout, "Lease path is %s", lease_path.c_str());


  string public_keys = kConfigDir + repo_name + "/pubkey";
  string key_file    = kConfigDir + repo_name + "/gatewaykey";
  string s3_file     = kConfigDir + repo_name + "/s3.conf";
  
  if( args.find('k') != args.end()) {
     public_keys = *args.find('k')->second;
  }
  if( args.find('s') != args.end()) {
     key_file = *args.find('s')->second;
  }
  if( args.find('3') != args.end()) {
     s3_file = *args.find('3')->second;
  }

  CUSTOM_ASSERT(access(public_keys.c_str(), R_OK) == 0, "%s is not readable", public_keys.c_str());
  CUSTOM_ASSERT(access(key_file.c_str(), R_OK) == 0, "%s is not readable", key_file.c_str());

//  string spooler_definition_string = string("gw,,") + g_gateway_url;
  // create a spooler that will upload to S3
  string const spooler_definition_string = string("S3,") + dir_temp + "," + repo_name + "@" + s3_file;

  // load gateway lease
  if (!gateway::ReadKeys(key_file, &g_gateway_key_id, &g_gateway_secret)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "gateway::ReadKeys failed");
    return 1;
  }

  uint64_t current_revision=0;
  std::string current_root_hash="";

  // acquire lease and save token to a file in the tmpdir
  LogCvmfs(kLogCvmfs, kLogStdout, "Acquiring gateway lease on %s",
           lease_path.c_str());
  g_session_token = acquire_lease(g_gateway_key_id, g_gateway_secret,
                                  repo_name + lease_path, g_gateway_url, force_cancel_lease,
                                  &current_revision, current_root_hash, lease_busy_retry_interval);

 
  char *_tmpfile = strdup( (dir_temp + "/gateway_session_token_XXXXXX").c_str() );
  int const temp_fd = mkstemp(_tmpfile);
  g_session_token_file = string(_tmpfile);
  free(_tmpfile);

  FILE *fout=fdopen(temp_fd, "wb"); 
  CUSTOM_ASSERT(fout!=NULL, "failed to open session token file %s for writing", g_session_token_file.c_str());
  fputs(g_session_token.c_str(), fout);
  fclose(fout);

  // now start the lease refresh thread
  pthread_t lease_thread;
  if ( 0 != pthread_create( &lease_thread, NULL, lease_refresh_thread, NULL ) ) {
     LogCvmfs(kLogCvmfs, kLogStderr, "Unable to start lease refresh thread");
     cancel_lease();
     return 1;
  }

  // now initialise the various bits we need

  upload::SpoolerDefinition spooler_definition(
      spooler_definition_string, shash::kSha1, zlib::kZlibDefault, false, true,
      SyncParameters::kDefaultMinFileChunkSize, SyncParameters::kDefaultAvgFileChunkSize,
      SyncParameters::kDefaultMaxFileChunkSize, g_session_token_file, key_file);

  if (args.find('q') != args.end()) {
    spooler_definition.number_of_concurrent_uploads =
        String2Uint64(*args.find('q')->second);
  }

  upload::SpoolerDefinition const spooler_definition_catalogs(
      spooler_definition.Dup2DefaultCompression());

  UniquePtr<upload::Spooler> const spooler_catalogs(
      upload::Spooler::Construct(spooler_definition_catalogs, nullptr));

  if (!spooler_catalogs.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "spooler_catalogs invalid");
    cancel_lease();
    return 1;
  }
  if (!InitDownloadManager(true, proxy, kCatalogDownloadMultiplier)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "download manager init failed");
    cancel_lease();
    return 1;
  }
  if (!InitSignatureManager(public_keys, "")) {
    LogCvmfs(kLogCvmfs, kLogStderr, "signature manager init failed");
    cancel_lease();
    return 1;
  }

  UniquePtr<manifest::Manifest> manifest;

  manifest = FetchRemoteManifest(stratum0, repo_name, shash::Any());

  if (!manifest.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "manifest invalid");
    cancel_lease();
    return 1;
  }

  if(current_revision > 0 ) {
  if( current_revision == manifest->revision() ) {
    if (current_root_hash != manifest->catalog_hash().ToString() ) {
     LogCvmfs(kLogCvmfs, kLogStderr, "Mismatch between cvmfspublished and gateway hash for revision %lu (%s!=%s)", current_revision, current_root_hash.c_str(), manifest->catalog_hash().ToString().c_str() );
     cancel_lease();
     return 1;
    } else {
     LogCvmfs(kLogCvmfs, kLogStdout, "Gateway and .cvmfspublished agree on repo version %lu", current_revision );
    }
  }
  if( current_revision > manifest->revision() ) {
     LogCvmfs(kLogCvmfs, kLogStdout, "Gateway has supplied a newer revision than the current .cvmfspublished %lu > %lu", current_revision, manifest->revision() );
     manifest->set_revision(current_revision);
     manifest->set_catalog_hash( shash::MkFromHexPtr(shash::HexPtr(current_root_hash), shash::kSuffixCatalog));
  } else if (current_revision < manifest->revision() ) {
     LogCvmfs(kLogCvmfs, kLogStdout, "Gateway has supplied an older revision than the current .cvmfspublished %lu < %lu", current_revision, manifest->revision() );
  } 
  } else {
     LogCvmfs(kLogCvmfs, kLogStdout, "Gateway has not supplied a revision. Using .cvmfspublished" );
  }


  // get hash of current root catalog, remove terminal "C", encode it
  string const old_root_hash = manifest->catalog_hash().ToString(true);
  string const hash = old_root_hash.substr(0, old_root_hash.length() - 1);
  shash::Any const base_hash =
      shash::MkFromHexPtr(shash::HexPtr(hash), shash::kSuffixCatalog);
  LogCvmfs(kLogCvmfs, kLogStdout, "old_root_hash: %s", old_root_hash.c_str());

  bool const is_balanced = false;

  catalog::WritableCatalogManager catalog_manager(
      base_hash, stratum0, dir_temp, spooler_catalogs.weak_ref(),
      download_manager(), false, SyncParameters::kDefaultNestedKcatalogLimit,
      SyncParameters::kDefaultRootKcatalogLimit, SyncParameters::kDefaultFileMbyteLimit, statistics(),
      is_balanced, SyncParameters::kDefaultMaxWeight, SyncParameters::kDefaultMinWeight, dir_temp /* dir_cache */);

  catalog_manager.Init();


  // now graft the contents of the DB
  vector<sqlite3*> open_dbs;
  for (auto&& db_file : sqlite_db_vec) {
    sqlite3 *db;
    CHECK_SQLITE_ERROR(sqlite3_open_v2(db_file.c_str(), &db, SQLITE_OPEN_READONLY, NULL), SQLITE_OK);
    relax_db_locking(db);
    open_dbs.push_back(db);
  }
  process_sqlite(open_dbs, catalog_manager, allow_additions,
                 allow_deletions, lease_path.substr(1), additional_prefix);
  for (auto&& db : open_dbs) {
    CHECK_SQLITE_ERROR(sqlite3_close_v2(db), SQLITE_OK);
  }

  // commit changes
  LogCvmfs(kLogCvmfs, kLogStdout, "Committing changes...");
  if (!catalog_manager.Commit(false, false, manifest.weak_ref())) {
    LogCvmfs(kLogCvmfs, kLogStderr, "something went wrong during sync");
    cancel_lease();
    return 1;
  }

  // finalize the spooler
  LogCvmfs(kLogCvmfs, kLogStdout, "Waiting for all uploads to finish...");
  spooler_catalogs->WaitForUpload();

  LogCvmfs(kLogCvmfs, kLogStdout, "Exporting repository manifest");

  // We call FinalizeSession(true) this time, to also trigger the commit
  // operation on the gateway machine (if the upstream is of type "gw").

  // Get the path of the new root catalog
  const string new_root_hash = manifest->catalog_hash().ToString(true);

//  if (!spooler_catalogs->FinalizeSession(true, old_root_hash, new_root_hash,
//                                         RepositoryTag())) {
//    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to commit the transaction");
//    // lease is only released on success
//    cancel_lease();
//    return 1;
//  }

  LogCvmfs(kLogCvmfs, kLogStdout, "Committing with priority %" PRId64, g_priority);

  bool const ok = make_commit_on_gateway( old_root_hash, new_root_hash, g_priority );
  if(!ok) {
   LogCvmfs(kLogCvmfs, kLogStderr, "something went wrong during commit on gateway");
   cancel_lease(); 
   exit(1);
  }


  unlink( g_session_token_file.c_str() );

  g_stop_refresh=true;


  if(check_completed_graft_property) {
    setDatabaseMarkedComplete(sqlite_db_vec[0].c_str()); 
  }



  return 0;
}

size_t writeFunction(void *ptr, size_t size, size_t nmemb, std::string* data) {
    return size * nmemb;
}

void replaceAllSubstrings(std::string& str, const std::string& from, const std::string& to) {
    if (from.empty()) {
        return; // Avoid infinite loop if 'from' is an empty string.
    }
    size_t startPos = 0;
    while ((startPos = str.find(from, startPos)) != std::string::npos) {
        str.replace(startPos, from.length(), to);
        startPos += to.length(); // Advance startPos to avoid replacing the substring just inserted.
    }
}


void swissknife::IngestSQL::process_sqlite(
    const std::vector<sqlite3*> &dbs,
    catalog::WritableCatalogManager &catalog_manager,
    bool allow_additions, bool allow_deletions, const std::string &lease_path, const std::string &additional_prefix)
{
  std::map<std::string, Directory> all_dirs;
  std::map<std::string, std::vector<File>> all_files;
  std::map<std::string, std::vector<Symlink>> all_symlinks;

  for (auto&& db : dbs) {
    load_dirs(db, lease_path, additional_prefix, all_dirs);
  }

  // put in a nested scope so we can free up memory of `dir_names`
  {
    LogCvmfs(kLogCvmfs, kLogStdout, "Precaching existing directories (starting from %s)", lease_path.c_str());
    std::unordered_set<std::string> dir_names;
    std::transform(all_dirs.begin(), all_dirs.end(), std::inserter(dir_names, dir_names.end()),
      [](const std::pair<std::string, Directory>& pair) {
        return MakeCatalogPath(pair.first);
      });
    catalog_manager.LoadCatalogs(MakeCatalogPath(lease_path), dir_names);
  }

  for (auto&& db : dbs) {
    load_files(db, lease_path, additional_prefix, all_files);
    load_symlinks(db, lease_path, additional_prefix, all_symlinks);
  }

  // perform all deletions first
  if (allow_deletions) {
    LogCvmfs(kLogCvmfs, kLogStdout, "Processing deletions...");
    for (auto&& db : dbs) {
      CHECK_SQLITE_ERROR(do_deletions(db, catalog_manager, lease_path, additional_prefix), SQLITE_OK);
    }
  }

  if (allow_additions) {
    LogCvmfs(kLogCvmfs, kLogStdout, "Processing additions...");
    // first ensure all directories are present and create missing ones
    do_additions(all_dirs, all_files, all_symlinks, lease_path, catalog_manager);
  }
}

void add_dir_to_tree(std::string path, std::unordered_map<std::string, std::set<std::string>> &tree, const std::string &lease_path) {
  tree[path];
  std::string parent_path = get_parent(path);
  // recursively create any missing parents in the tree
  // avoid creating a loop when we insert the root path
  while (path != parent_path && path != lease_path && !tree[parent_path].count(path)) {
    tree[parent_path].insert(path);
    path = parent_path;
    parent_path = get_parent(path);
  }
}

int swissknife::IngestSQL::do_additions(
  const DirMap &all_dirs, const FileMap &all_files, const SymlinkMap &all_symlinks, const std::string &lease_path, catalog::WritableCatalogManager &catalog_manager) {
  // STEP 1:
  // - collect all the dirs/symlinks/files we need to process from the DB
  // - build a tree of paths for DFS traversal
  //   - note the tree will contain all parent dirs of symlinks/files even if those are not
  //     explicitly added to the dirs table
  std::unordered_map<std::string, std::set<std::string>> tree;
  for (auto&& p : all_dirs) {
    add_dir_to_tree(p.first, tree, lease_path);
  }
  for (auto&& p : all_files) {
    add_dir_to_tree(p.first, tree, lease_path);
  }
  for (auto&& p : all_symlinks) {
    add_dir_to_tree(p.first, tree, lease_path);
  }
  int const row_count = static_cast<int>(tree.size());
  int const print_every = calculate_print_frequency(row_count);
  int curr_row = 0;
  LogCvmfs(kLogCvmfs, kLogStdout, "Changeset: %ld dirs, %ld files, %ld symlinks", tree.size(), all_files.size(), all_symlinks.size());

  // STEP 2:
  // - process all the changes with DFS traversal
  //   - make directories in pre-order
  //   - add files/symlinks and schedule upload in post-order
  catalog_manager.SetupSingleCatalogUploadCallback();
  std::stack<string> dfs_stack;
  for (auto&& p : tree) {
    // figure out the starting point by checking whose parent is missing from the tree
    if (p.first == "" || !tree.count(get_parent(p.first))) {
      CUSTOM_ASSERT(dfs_stack.empty(), "provided DB input forms more than one path trees");
      dfs_stack.push(p.first);
    }
  }
  std::set<string> visited;
  while (!dfs_stack.empty()) {
    string const curr_dir = dfs_stack.top();
    // add content for the dir in post-order traversal
    if (visited.count(curr_dir)) {
      curr_row++;
      if (all_symlinks.count(curr_dir)) {
        add_symlinks(catalog_manager, all_symlinks.at(curr_dir));
      }
      if (all_files.count(curr_dir)) {
        add_files(catalog_manager, all_files.at(curr_dir));
      }
      // snapshot the dir (if it's a nested catalog mountpoint)
      catalog::DirectoryEntry dir_entry;
      bool exists = false;
      exists = catalog_manager.LookupDirEntry(MakeCatalogPath(curr_dir),
                                          catalog::kLookupDefault, &dir_entry);
      assert(exists); // the dir must exist at this point
      if (dir_entry.IsNestedCatalogMountpoint() || dir_entry.IsNestedCatalogRoot()) {
        catalog_manager.AddCatalogToQueue(curr_dir);
        catalog_manager.ScheduleReadyCatalogs();
      }
      dfs_stack.pop();
      SHOW_PROGRESS("directories", print_every, curr_row, row_count);
    } else {
      visited.insert(curr_dir);
      // push children to the stack
      auto it = tree.find(curr_dir);
      if (it != tree.end()) {
        for (auto&& child : it->second) {
          dfs_stack.push(child);
        }
        tree.erase(it);
      }
      if (!all_dirs.count(curr_dir)) continue;

      // create the dir first in pre-order traversal
      const IngestSQL::Directory& dir = all_dirs.at(curr_dir);
      catalog::DirectoryEntry dir_entry;

      bool exists = false;
      exists = catalog_manager.LookupDirEntry(MakeCatalogPath(curr_dir),
                                          catalog::kLookupDefault, &dir_entry);
      CUSTOM_ASSERT(!(exists && !S_ISDIR(dir_entry.mode_)), "Refusing to replace existing file/symlink at %s with a directory", dir.name.c_str());

      dir_entry.name_ = NameString(get_basename(dir.name));
      dir_entry.mtime_ = dir.mtime / 1000000000;
      dir_entry.mode_ = dir.mode | S_IFDIR;
      dir_entry.mode_ &= (S_IFDIR | 0777);
      dir_entry.uid_ = dir.owner;
      dir_entry.gid_ = dir.grp;
      dir_entry.has_xattrs_ = !dir.xattr.IsEmpty();

      bool add_nested_catalog=false;

      if (exists) {
        catalog_manager.TouchDirectory(dir_entry, dir.xattr, dir.name);
        if((!dir_entry.IsNestedCatalogMountpoint() && !dir_entry.IsNestedCatalogRoot()) && ( g_add_missing_catalogs || dir.nested ) ) {
          add_nested_catalog=true;
          LogCvmfs(kLogCvmfs, kLogVerboseMsg, "Touching existing directory %s and adding nested catalog", dir.name.c_str());
        } else {
          LogCvmfs(kLogCvmfs, kLogVerboseMsg, "Touching existing directory %s", dir.name.c_str());
        }
      } else {
        LogCvmfs(kLogCvmfs, kLogVerboseMsg, "Adding directory [%s]", dir.name.c_str());
        catalog_manager.AddDirectory(dir_entry, dir.xattr, get_parent(dir.name));
        if(dir.nested) {add_nested_catalog=true;}
      }
      if (add_nested_catalog) {
        // now add a .cvmfscatalog file
        // so that manual changes won't remove the nested catalog
        LogCvmfs(kLogCvmfs, kLogVerboseMsg, "Placing .cvmfscatalog file in [%s]", dir.name.c_str());
        catalog::DirectoryEntryBase dir2;
        dir2.name_ = NameString(".cvmfscatalog");
        dir2.mtime_ = dir.mtime / 1000000000;
        dir2.mode_ = (S_IFREG | 0666);
        dir2.uid_ = 0;
        dir2.gid_ = 0;
        dir2.has_xattrs_ = 0;
        dir2.checksum_ = shash::MkFromHexPtr(
            shash::HexPtr("da39a3ee5e6b4b0d3255bfef95601890afd80709"),
            shash::kSuffixNone);  // hash of ""
        XattrList const xattr2;
        catalog_manager.AddFile(dir2, xattr2, dir.name);

        LogCvmfs(kLogCvmfs, kLogVerboseMsg, "Creating Nested Catalog [%s]", dir.name.c_str());
        catalog_manager.CreateNestedCatalog(dir.name);
      }
    }
  }
  
  // sanity check that we have processed all the input
  CUSTOM_ASSERT(tree.empty(), "not all directories are processed, malformed input DB?");
  catalog_manager.RemoveSingleCatalogUploadCallback();
  return 0;
}

int swissknife::IngestSQL::add_symlinks(
    catalog::WritableCatalogManager &catalog_manager, const std::vector<Symlink> &symlinks)
{
  for (auto&& symlink : symlinks) {
    catalog::DirectoryEntry dir;
    catalog::DirectoryEntryBase dir2;
    XattrList const xattr;
    bool exists = false;
    exists = catalog_manager.LookupDirEntry(MakeCatalogPath(symlink.name),
                                        catalog::kLookupDefault, &dir);

    dir2.name_ = NameString(get_basename(symlink.name));
    dir2.mtime_ = symlink.mtime / 1000000000;
    dir2.uid_ = symlink.owner;
    dir2.gid_ = symlink.grp;
    dir2.has_xattrs_ = false;
    dir2.symlink_ = LinkString(symlink.target);
    dir2.mode_ = S_IFLNK | 0777;

    int noop=false;

    if (exists) {
      if(symlink.skip_if_file_or_dir) {
        if(S_ISDIR(dir.mode_) || S_ISREG(dir.mode_)) {
          LogCvmfs(kLogCvmfs, kLogVerboseMsg, "File or directory for symlink [%s] exists, skipping symlink creation", symlink.name.c_str() );
          noop = true;
        } else if (S_ISLNK(dir.mode_)) {
           LogCvmfs(kLogCvmfs, kLogVerboseMsg, "Removing existing symlink [%s]", 
               symlink.name.c_str());
           catalog_manager.RemoveFile(symlink.name);
        } else {
          CUSTOM_ASSERT(0, "unknown mode for dirent: %d", dir.mode_);
        }
      } else {
        CUSTOM_ASSERT(!S_ISDIR(dir.mode_), "Not removing directory [%s] to create symlink", symlink.name.c_str());
        LogCvmfs(kLogCvmfs, kLogVerboseMsg, "Removing existing file/symlink [%s]",
               symlink.name.c_str());
        catalog_manager.RemoveFile(symlink.name);
      }
    }
    if(!noop) {
      string const parent = get_parent(symlink.name);
      LogCvmfs(kLogCvmfs, kLogVerboseMsg, "Adding symlink [%s] -> [%s]", symlink.name.c_str(), symlink.target.c_str());
      catalog_manager.AddFile(dir2, xattr, parent);
    }
  }
  return 0;
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

bool check_prefix(const std::string &path , const std::string &prefix) {
    if (prefix=="" || prefix=="/") {return true;}
    if ( "/"+path == prefix ) { return true; }
    if(!HasPrefix( path, prefix, false)) {
      LogCvmfs(kLogCvmfs, kLogVerboseMsg, "Entry %s is outside lease path: %s", path.c_str(), prefix.c_str());
      return false;
    }
    return true;
}

void swissknife::IngestSQL::load_dirs(sqlite3 *db, const std::string &lease_path, const std::string &additional_prefix, std::map<std::string, Directory> &all_dirs) {
  sqlite3_stmt *stmt;
  int const schema_revision = get_db_schema_revision(db);
  string select_stmt = "SELECT name, mode, mtime, owner, grp, acl, nested FROM dirs";
  if (schema_revision <= 3) {
    select_stmt = "SELECT name, mode, mtime, owner, grp, acl FROM dirs";
  }
  int const ret = sqlite3_prepare_v2(db, select_stmt.c_str(), -1, &stmt, NULL);
  CHECK_SQLITE_ERROR(ret, SQLITE_OK);
  while (sqlite3_step(stmt) == SQLITE_ROW) {
    char *name_cstr = (char *)sqlite3_column_text(stmt, 0);
    mode_t const mode = sqlite3_column_int(stmt, 1);
    time_t const mtime = sqlite3_column_int64(stmt, 2);
    uid_t const owner = sqlite3_column_int(stmt, 3);
    gid_t const grp = sqlite3_column_int(stmt, 4);
    int const nested = schema_revision <= 3 ? 1 : sqlite3_column_int(stmt, 6);

    string const name = additional_prefix + sanitise_name(name_cstr);
    CUSTOM_ASSERT(check_prefix(name, lease_path), "%s is not below lease path %s", name.c_str(), lease_path.c_str());

    Directory dir(name, mtime, mode, owner, grp, nested);
    char *acl = (char *)sqlite3_column_text(stmt, 5);
    dir.xattr = marshal_xattrs(acl);
    all_dirs.insert(std::make_pair(name, dir));
  }
  CHECK_SQLITE_ERROR(sqlite3_finalize(stmt), SQLITE_OK);
}

void swissknife::IngestSQL::load_files(sqlite3 *db, const std::string &lease_path, const std::string &additional_prefix, std::map<std::string, std::vector<File>> &all_files) {
  sqlite3_stmt *stmt;
  int const schema_revision = get_db_schema_revision(db);
  string select_stmt = "SELECT name, mode, mtime, owner, grp, size, hashes, internal, compressed FROM files";
  if (schema_revision <= 2) {
    select_stmt = "SELECT name, mode, mtime, owner, grp, size, hashes, internal FROM files";
  }
  int const ret = sqlite3_prepare_v2(db, select_stmt.c_str(), -1, &stmt, NULL);
  CHECK_SQLITE_ERROR(ret, SQLITE_OK);
  while (sqlite3_step(stmt) == SQLITE_ROW) {
    char *name = (char *)sqlite3_column_text(stmt, 0);
    mode_t const mode = sqlite3_column_int(stmt, 1);
    time_t const mtime = sqlite3_column_int64(stmt, 2);
    uid_t const owner = sqlite3_column_int(stmt, 3);
    gid_t const grp = sqlite3_column_int(stmt, 4);
    size_t const size = sqlite3_column_int64(stmt, 5);
    char *hashes_cstr = (char *)sqlite3_column_text(stmt, 6);
    int const internal = sqlite3_column_int(stmt, 7);
    int const compressed = schema_revision <= 2 ? 0 : sqlite3_column_int(stmt, 8);

    string names = additional_prefix + sanitise_name(name);
    CUSTOM_ASSERT(check_prefix(names, lease_path), "%s is not below lease path %s", names.c_str(), lease_path.c_str());
    string const parent_dir = get_parent(names);

    if (!all_files.count(parent_dir)) {
      all_files[parent_dir] = vector<swissknife::IngestSQL::File>();
    }
    all_files[parent_dir].emplace_back(std::move(names), mtime, size, owner, grp, mode, internal, compressed);

    // tokenize hashes
    char *ref;
    char *tok;
    tok = strtok_r(hashes_cstr, ",", &ref);
    vector<off_t> offsets;
    vector<size_t> sizes;
    vector<shash::Any> hashes;
    off_t offset = 0;

    CUSTOM_ASSERT(size>=0, "file size cannot be negative [%s]", names.c_str());
    size_t const kChunkSize = internal ? kInternalChunkSize : kExternalChunkSize;

    while (tok) {
      offsets.push_back(offset);
      // TODO: check the hash format
      CUSTOM_ASSERT(check_hash(tok)==0, "provided hash for [%s] is invalid: %s", names.c_str(), tok);
      hashes.push_back(
          shash::MkFromHexPtr(shash::HexPtr(tok), shash::kSuffixNone));
      tok = strtok_r(NULL, ",", &ref);
      offset += kChunkSize;  // in the future we might want variable chunk
                             // sizes specified in the DB
    }
    size_t expected_num_chunks = size/kChunkSize;
    if (expected_num_chunks * (size_t)kChunkSize < (size_t) size || size==0 ) { expected_num_chunks++; }
    CUSTOM_ASSERT(offsets.size() == expected_num_chunks, "offsets size %ld does not match expected number of chunks %ld", offsets.size(), expected_num_chunks);
    for (size_t i = 0; i < offsets.size() - 1; i++) {  
      sizes.push_back(size_t(offsets[i + 1] - offsets[i]));
    }

    sizes.push_back(size_t(size - offsets[offsets.size() - 1]));
    for (size_t i = 0; i < offsets.size(); i++) {
      FileChunk const chunk = FileChunk(hashes[i], offsets[i], sizes[i]);
      all_files[parent_dir].back().chunks.PushBack(chunk);
    }
  }
  CHECK_SQLITE_ERROR(sqlite3_finalize(stmt), SQLITE_OK);
}

void swissknife::IngestSQL::load_symlinks(sqlite3 *db, const std::string &lease_path, const std::string &additional_prefix, std::map<std::string, std::vector<Symlink>> &all_symlinks) {
  sqlite3_stmt *stmt;
  string const select_stmt = "SELECT name, target, mtime, owner, grp, skip_if_file_or_dir FROM links";
  int const ret = sqlite3_prepare_v2(db, select_stmt.c_str(), -1, &stmt, NULL);
  CHECK_SQLITE_ERROR(ret, SQLITE_OK);
  while (sqlite3_step(stmt) == SQLITE_ROW) {
    char *name_cstr = (char *)sqlite3_column_text(stmt, 0);
    char *target_cstr = (char *)sqlite3_column_text(stmt, 1);
    time_t const mtime = sqlite3_column_int64(stmt, 2);
    uid_t const owner = sqlite3_column_int(stmt, 3);
    gid_t const grp = sqlite3_column_int(stmt, 4);
    int const skip_if_file_or_dir = sqlite3_column_int(stmt, 5);

    string names = additional_prefix + sanitise_name(name_cstr);
    CUSTOM_ASSERT(check_prefix(names, lease_path), "%s is not below lease path %s", names.c_str(), lease_path.c_str());
    string target= target_cstr;
    string const parent_dir = get_parent(names);
    
    if (!all_symlinks.count(parent_dir)) {
      all_symlinks[parent_dir] = vector<swissknife::IngestSQL::Symlink>();
    }
    all_symlinks[parent_dir].emplace_back(std::move(names), std::move(target), mtime, owner, grp, skip_if_file_or_dir);
  }
  CHECK_SQLITE_ERROR(sqlite3_finalize(stmt), SQLITE_OK);
}

int swissknife::IngestSQL::add_files(
    catalog::WritableCatalogManager &catalog_manager, const std::vector<File> &files)
{
  for (auto&& file : files) {
    catalog::DirectoryEntry dir;
    XattrList const xattr;
    bool exists = false;
    exists = catalog_manager.LookupDirEntry(MakeCatalogPath(file.name),
                                        catalog::kLookupDefault, &dir);

    dir.name_ = NameString(get_basename(file.name));
    dir.mtime_ = file.mtime / 1000000000;
    dir.mode_ = file.mode | S_IFREG;
    dir.mode_ &= (S_IFREG | 0777);
    dir.uid_ = file.owner;
    dir.gid_ = file.grp;
    dir.size_ = file.size;
    dir.has_xattrs_ = false;
    dir.is_external_file_ = !file.internal;
    dir.set_is_chunked_file(true);
    dir.checksum_ = shash::MkFromHexPtr(
        shash::HexPtr("0000000000000000000000000000000000000000"),
        shash::kSuffixNone);

    // compression is permitted only for internal data
    CUSTOM_ASSERT(file.internal || (!file.internal && file.compressed<2), "compression is only allowed for internal data [%s]", file.name.c_str());
 
    switch (file.compressed) {
      case 1: // Uncompressed
        dir.compression_algorithm_ = zlib::kNoCompression;
        break;
      case 2: // Compressed with Zlib
        dir.compression_algorithm_ = zlib::kZlibDefault;
        break;
      // future cases: different compression schemes
      default: // default behaviour: compressed if internal, content-addressed. Uncompressed if external
        dir.compression_algorithm_ = file.internal ? zlib::kZlibDefault : zlib::kNoCompression;
    }

    if (exists) {
      CUSTOM_ASSERT(!S_ISDIR(dir.mode()) && !S_ISLNK(dir.mode()), "Refusing to replace existing dir/symlink at %s with a file", file.name.c_str());
      LogCvmfs(kLogCvmfs, kLogVerboseMsg, "Removing existing file [%s]", file.name.c_str());
      catalog_manager.RemoveFile(file.name);
    }
    string const parent = get_parent(file.name);
    LogCvmfs(kLogCvmfs, kLogVerboseMsg, "Adding chunked file [%s]", file.name.c_str());
    catalog_manager.AddChunkedFile(dir, xattr, parent, file.chunks);
  }

  return 0;
}

int swissknife::IngestSQL::do_deletions(
    sqlite3 *db, catalog::WritableCatalogManager &catalog_manager, const std::string &lease_path, const std::string &additional_prefix) {
  sqlite3_stmt *stmt;
  int const row_count = get_row_count(db, "deletions");
  int const print_every = calculate_print_frequency(row_count);
  int curr_row = 0;
  int ret = sqlite3_prepare_v2(db, "SELECT name, directory, file, link FROM deletions ORDER BY length(name) DESC", -1, &stmt, NULL);
  CHECK_SQLITE_ERROR(ret, SQLITE_OK);
  while (sqlite3_step(stmt) == SQLITE_ROW) {
    curr_row++;

    char *name = (char *)sqlite3_column_text(stmt, 0);
    int64_t const isdir  = sqlite3_column_int64(stmt, 1);
    int64_t const isfile = sqlite3_column_int64(stmt, 2);
    int64_t const islink = sqlite3_column_int64(stmt, 3);

    string const names = additional_prefix + sanitise_name(name);
    CUSTOM_ASSERT(check_prefix( names, lease_path), "%s is not below lease path %s", names.c_str(), lease_path.c_str());

    catalog::DirectoryEntry dirent;
    bool exists = false;
    exists = catalog_manager.LookupDirEntry(MakeCatalogPath(names),
                                        catalog::kLookupDefault, &dirent);
    if(exists) {
      if(    (isdir  && S_ISDIR(dirent.mode()) )
          || (islink && S_ISLNK(dirent.mode()) )
          || (isfile && S_ISREG(dirent.mode()) ) ) {
        if (S_ISDIR(dirent.mode())) {
          PathString names_path(names);
          recursively_delete_directory(names_path, catalog_manager);
        } else {
          LogCvmfs(kLogCvmfs, kLogVerboseMsg, "Removing link/file [%s]", names.c_str());
          catalog_manager.RemoveFile(names);
        }
      } else {
        LogCvmfs(kLogCvmfs, kLogVerboseMsg, "Mismatch in deletion type, not deleting: [%s] (dir %ld/%d , link %ld/%d, file %ld/%d)", names.c_str(), isdir,  S_ISDIR(dirent.mode()), islink, S_ISLNK(dirent.mode()), isfile, S_ISREG(dirent.mode()) );
      }
    } else {
      LogCvmfs(kLogCvmfs, kLogVerboseMsg, "Not Removing non-existent [%s]",
               names.c_str());
    }

    SHOW_PROGRESS("deletions", print_every, curr_row, row_count);
  }
  ret = sqlite3_finalize(stmt);
  return ret;
}

const char*schema[] = {
 "PRAGMA journal_mode=WAL;",

 "CREATE TABLE IF NOT EXISTS dirs ( \
        name  TEXT    PRIMARY KEY, \
        mode  INTEGER NOT NULL DEFAULT 493,\
        mtime INTEGER NOT NULL DEFAULT (unixepoch()),\
        owner INTEGER NOT NULL DEFAULT 0, \
        grp   INTEGER NOT NULL DEFAULT 0, \
        acl   TEXT    NOT NULL DEFAULT '', \
        nested INTEGER DEFAULT 1);",

 "CREATE TABLE IF NOT EXISTS files ( \
        name   TEXT    PRIMARY KEY, \
        mode   INTEGER NOT NULL DEFAULT 420, \
        mtime  INTEGER NOT NULL DEFAULT (unixepoch()),\
        owner  INTEGER NOT NULL DEFAULT 0,\
        grp    INTEGER NOT NULL DEFAULT 0,\
        size   INTEGER NOT NULL DEFAULT 0,\
        hashes TEXT    NOT NULL DEFAULT '',\
        internal INTEGER NOT NULL DEFAULT 0,\
        compressed INTEGER NOT NULL DEFAULT 0\
  );",

 "CREATE TABLE IF NOT EXISTS links (\
        name   TEXT    PRIMARY KEY,\
        target TEXT    NOT NULL DEFAULT '',\
        mtime  INTEGER NOT NULL DEFAULT (unixepoch()),\
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

 "CREATE TABLE IF NOT EXISTS properties (\
        key   TEXT PRIMARY KEY,\
        value TEXT NOT NULL\
  );",

 "INSERT INTO properties VALUES ('schema_revision', '4') ON CONFLICT DO NOTHING;",
  NULL
};

static void create_empty_database( string& filename ) {
  sqlite3 *db_out;
  LogCvmfs(kLogCvmfs, kLogStdout, "Creating empty database file %s", filename.c_str());
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

static void recursively_delete_directory(PathString& path, catalog::WritableCatalogManager &catalog_manager) {
   catalog::DirectoryEntryList const listing;

  // Add all names
   catalog::StatEntryList listing_from_catalog;
   bool const retval = catalog_manager.ListingStat(PathString( "/" +  path.ToString()), &listing_from_catalog);

   CUSTOM_ASSERT(retval, "failed to call ListingStat for %s", path.c_str());

  if(!catalog_manager.IsTransitionPoint(path.ToString())) {

   for (unsigned i = 0; i < listing_from_catalog.size(); ++i) {
    PathString entry_path;
    entry_path.Assign(path);
    entry_path.Append("/", 1);
    entry_path.Append(listing_from_catalog.AtPtr(i)->name.GetChars(),
                      listing_from_catalog.AtPtr(i)->name.GetLength());

    if( S_ISDIR( listing_from_catalog.AtPtr(i)->info.st_mode) ) {
      LogCvmfs(kLogCvmfs, kLogVerboseMsg, "Recursing into %s/", entry_path.ToString().c_str());
      recursively_delete_directory(entry_path, catalog_manager);


    } else {
      LogCvmfs(kLogCvmfs, kLogVerboseMsg, " Recursively removing %s", entry_path.ToString().c_str());
      catalog_manager.RemoveFile(entry_path.ToString());
    }

   }
  } else { 
      LogCvmfs(kLogCvmfs, kLogVerboseMsg, "Removing nested catalog %s", path.ToString().c_str());
      catalog_manager.RemoveNestedCatalog(path.ToString(), false);
  }
  LogCvmfs(kLogCvmfs, kLogVerboseMsg, "Removing directory %s", path.ToString().c_str());
     catalog_manager.RemoveDirectory(path.ToString());

}


static void relax_db_locking(sqlite3 *db) {
    int ret=0;
    ret = sqlite3_exec(db, "PRAGMA temp_store=2", NULL, NULL, NULL);
    CHECK_SQLITE_ERROR(ret, SQLITE_OK);
    ret = sqlite3_exec(db, "PRAGMA synchronous=OFF", NULL, NULL, NULL);
    CHECK_SQLITE_ERROR(ret, SQLITE_OK);
}


extern "C" void* lease_refresh_thread(void *payload) {
 while( !g_stop_refresh ) {
   sleep(2);
   refresh_lease();
 }
 return NULL;
}

static bool isDatabaseMarkedComplete(const char *dbfile) {
  int ret;
  sqlite3 *db;
  sqlite3_stmt *stmt;
  bool retval=false;

  ret = sqlite3_open(dbfile, &db);
  if (ret!=SQLITE_OK) { return false; }

  const char *req = "SELECT value FROM properties WHERE key='completed_graft'";
  ret = sqlite3_prepare_v2(db, req, -1, &stmt, NULL);
  if (ret!=SQLITE_OK) { 
    return false; 
  }
  if(sqlite3_step(stmt) == SQLITE_ROW) {
    int const id = sqlite3_column_int(stmt,0);
    if(id>0) { 
      retval=true; 
    }
  }
  sqlite3_close(db);
  return retval;
}

static void setDatabaseMarkedComplete(const char *dbfile) {
  int ret;
  sqlite3 *db;
  char *err;

  ret = sqlite3_open(dbfile, &db);
  if (ret!=SQLITE_OK) { return; }
 
  const char *req = "INSERT INTO properties (key, value) VALUES ('completed_graft',1) ON CONFLICT(key) DO UPDATE SET value=1 WHERE key='completed_graft'";

  ret = sqlite3_exec(db, req, 0, 0, &err);
  if (ret!=SQLITE_OK) { 
    return; 
  }
  sqlite3_close(db);
}

