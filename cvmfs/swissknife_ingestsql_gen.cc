/**
 * This file is part of the CernVM File System.
 */

#include "swissknife_ingestsql_gen.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fnmatch.h>
#include <limits.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cinttypes>
#include <thread>

#include "acl.h"
#include "crypto/hash.h"
#include "ingestsql_schema.h"
#include "util/logging.h"
#include "util/platform.h"
#include "util/posix.h"
#include "util/string.h"

using namespace std;  // NOLINT

namespace {

const char * const kCatalogMarker = ".cvmfscatalog";

int64_t MtimeNs(const platform_stat64 &info) {
#ifdef __APPLE__
  const struct timespec &ts = info.st_mtimespec;
#else
  const struct timespec &ts = info.st_mtim;
#endif
  return static_cast<int64_t>(ts.tv_sec) * 1000000000 + ts.tv_nsec;
}

bool GetXattr(const string &path, const char *name, string *value) {
  ssize_t size = platform_lgetxattr(path.c_str(), name, NULL, 0);
  if (size < 0) {
    return false;
  }
  value->resize(size);
  if (size == 0) {
    return true;
  }
  size = platform_lgetxattr(path.c_str(), name, &(*value)[0], value->size());
  if (size < 0) {
    return false;
  }
  value->resize(size);
  return true;
}

/**
 * Leading '/', no trailing '/', no empty, '.' or '..' components.
 */
bool NormalizeRepoPath(const string &path, string *result) {
  const vector<string> components = SplitString(path, '/');
  string normalized;
  for (unsigned i = 0; i < components.size(); ++i) {
    const string &c = components[i];
    if (c.empty()) {
      // tolerate leading and trailing slashes only
      if (i == 0 || i == components.size() - 1) {
        continue;
      }
      return false;
    }
    if (c == "." || c == "..") {
      return false;
    }
    normalized += "/" + c;
  }
  *result = normalized.empty() ? "/" : normalized;
  return true;
}

string JoinRepoPath(const string &parent, const string &name) {
  return (parent == "/") ? parent + name : parent + "/" + name;
}

string SqlQuote(const string &s) {
  return "'" + ReplaceAll(s, "'", "''") + "'";
}

// Keeps SQL line comments on one line.
string CommentSafe(const string &s) { return ReplaceAll(s, "\n", "?"); }

string ModeString(swissknife::IngestSQLGen::EntryType type, mode_t mode) {
  string s;
  switch (type) {
    case swissknife::IngestSQLGen::kDir:
      s = "d";
      break;
    case swissknife::IngestSQLGen::kLink:
      return "lrwxrwxrwx";
    default:
      s = "-";
  }
  const char *rwx = "rwxrwxrwx";
  for (int i = 0; i < 9; ++i) {
    s += (mode & (0400 >> i)) ? rwx[i] : '-';
  }
  return s;
}

string TimeString(int64_t mtime_ns) {
  const time_t seconds = static_cast<time_t>(mtime_ns / 1000000000);
  struct tm tm;
  gmtime_r(&seconds, &tm);
  char buf[32];
  strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm);
  return buf;
}

string HumanSize(uint64_t bytes) {
  const char *units[] = {"B", "KiB", "MiB", "GiB", "TiB"};
  double value = static_cast<double>(bytes);
  unsigned unit = 0;
  while (value >= 1024 && unit < 4) {
    value /= 1024;
    unit++;
  }
  char buf[32];
  if (unit == 0) {
    snprintf(buf, sizeof(buf), "%" PRIu64 " B", bytes);
  } else {
    snprintf(buf, sizeof(buf), "%.1f %s", value, units[unit]);
  }
  return buf;
}

/**
 * External data layout: SHA-1 of the uncompressed content, one per fixed-size
 * chunk.  An empty file still has one (empty) chunk.
 */
bool HashChunks(const string &path, uint64_t expected_size,
                vector<string> *hashes, string *error) {
  const int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    *error = "cannot open: " + string(strerror(errno));
    return false;
  }
  vector<unsigned char> buf(1024 * 1024);
  shash::ContextPtr ctx(shash::kSha1);
  vector<char> ctx_buf(ctx.size);
  ctx.buffer = ctx_buf.data();
  shash::Any digest(shash::kSha1);

  shash::Init(ctx);
  uint64_t in_chunk = 0;
  uint64_t total = 0;
  while (true) {
    const size_t want = min(static_cast<uint64_t>(buf.size()),
                            ingestsql::kExternalChunkSize - in_chunk);
    const ssize_t n = read(fd, buf.data(), want);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      *error = "read error: " + string(strerror(errno));
      close(fd);
      return false;
    }
    if (n == 0) {
      break;
    }
    shash::Update(buf.data(), static_cast<unsigned>(n), ctx);
    in_chunk += n;
    total += n;
    if (in_chunk == ingestsql::kExternalChunkSize) {
      shash::Final(ctx, &digest);
      hashes->push_back(digest.ToString());
      shash::Init(ctx);
      in_chunk = 0;
    }
  }
  close(fd);
  if (in_chunk > 0 || total == 0) {
    shash::Final(ctx, &digest);
    hashes->push_back(digest.ToString());
  }
  if (total != expected_size) {
    *error = "file changed while scanning (size " + StringifyUint(expected_size)
             + ", read " + StringifyUint(total) + " bytes)";
    return false;
  }
  return true;
}

struct Summary {
  unsigned dirs, nested, files, links, chunks;
  uint64_t bytes;
  explicit Summary(const vector<swissknife::IngestSQLGen::Entry> &entries)
      : dirs(0), nested(0), files(0), links(0), chunks(0), bytes(0) {
    for (unsigned i = 0; i < entries.size(); ++i) {
      const swissknife::IngestSQLGen::Entry &e = entries[i];
      switch (e.type) {
        case swissknife::IngestSQLGen::kDir:
          dirs++;
          nested += e.nested ? 1 : 0;
          break;
        case swissknife::IngestSQLGen::kFile:
          files++;
          bytes += e.size;
          chunks += e.hashes.size();
          break;
        case swissknife::IngestSQLGen::kLink:
          links++;
          break;
      }
    }
  }
  string ToString(unsigned skipped) const {
    char buf[256];
    snprintf(buf, sizeof(buf),
             "dirs: %u (%u nested), files: %u (%s in %u chunks), links: %u, "
             "skipped: %u",
             dirs, nested, files, HumanSize(bytes).c_str(), chunks, links,
             skipped);
    return buf;
  }
};

}  // anonymous namespace


namespace swissknife {

void IngestSQLGen::Warn(const string &local_path, const string &msg) {
  num_warnings_++;
  LogCvmfs(kLogCvmfs, kLogStderr, "Warning: %s: %s", local_path.c_str(),
           msg.c_str());
}

bool IngestSQLGen::IsNested(const string &local_path,
                            const string &name) const {
  platform_stat64 info;
  if (platform_lstat((local_path + "/" + kCatalogMarker).c_str(), &info) == 0
      && S_ISREG(info.st_mode)) {
    return true;
  }
  for (unsigned i = 0; i < nested_globs_.size(); ++i) {
    if (fnmatch(nested_globs_[i].c_str(), name.c_str(), FNM_PATHNAME) == 0) {
      return true;
    }
  }
  return false;
}

bool IngestSQLGen::Scan(const string &local_path, const string &name) {
  platform_stat64 info;
  if (platform_lstat(local_path.c_str(), &info) != 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Cannot stat %s: %s", local_path.c_str(),
             strerror(errno));
    return false;
  }

  Entry e;
  e.name = name;
  e.local_path = local_path;
  e.mode = info.st_mode & 0777;
  e.mtime_ns = MtimeNs(info);
  e.uid = override_owner_ ? uid_ : info.st_uid;
  e.gid = override_owner_ ? gid_ : info.st_gid;

  if (S_ISLNK(info.st_mode)) {
    e.type = kLink;
    e.target = ReadSymlink(local_path);
    if (e.target.empty()) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Cannot read symlink %s",
               local_path.c_str());
      return false;
    }
    entries_.push_back(e);
    return true;
  }
  if (!S_ISDIR(info.st_mode) && !S_ISREG(info.st_mode)) {
    num_skipped_++;
    Warn(local_path, "not a regular file, directory or symlink, skipped");
    return true;
  }

  if (info.st_mode & 07000) {
    Warn(local_path, "setuid/setgid/sticky bits dropped");
  }
  string xattr;
  if (GetXattr(local_path, "system.posix_acl_default", &xattr)) {
    Warn(local_path, "default ACL dropped (not supported by ingestsql)");
  }
  const bool has_acl = GetXattr(local_path, "system.posix_acl_access", &xattr);

  if (S_ISREG(info.st_mode)) {
    e.type = kFile;
    e.size = info.st_size;
    if (has_acl) {
      Warn(local_path, "file ACL dropped (ingestsql supports directory ACLs)");
    }
    if (info.st_nlink > 1) {
      const pair<dev_t, ino_t> key(info.st_dev, info.st_ino);
      const auto it = hardlinks_.find(key);
      if (it == hardlinks_.end()) {
        hardlinks_[key] = name;
      } else {
        Warn(local_path,
             "hardlink to " + it->second + ", stored as an independent copy");
      }
    }
    entries_.push_back(e);
    return true;
  }

  e.type = kDir;
  if (has_acl
      && acl_from_xattr_value_to_text(xattr.data(), xattr.size(), &e.acl)
             != 0) {
    Warn(local_path, "unparsable access ACL dropped");
    e.acl.clear();
  }
  e.nested = IsNested(local_path, name);
  // Mapping onto the repository root needs no entry for the root itself.
  if (name != "/") {
    entries_.push_back(e);
  }
  return ScanDir(local_path, name);
}

bool IngestSQLGen::ScanDir(const string &local_path, const string &name) {
  DIR *dir = opendir(local_path.c_str());
  if (dir == NULL) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Cannot open directory %s: %s",
             local_path.c_str(), strerror(errno));
    return false;
  }
  vector<string> children;
  platform_dirent64 *d;
  while ((d = platform_readdir(dir)) != NULL) {
    const string child = d->d_name;
    if (child == "." || child == ".." || child == kCatalogMarker) {
      continue;
    }
    children.push_back(child);
  }
  closedir(dir);
  sort(children.begin(), children.end());

  for (unsigned i = 0; i < children.size(); ++i) {
    if (!Scan(local_path + "/" + children[i],
              JoinRepoPath(name, children[i]))) {
      return false;
    }
  }
  return true;
}

bool IngestSQLGen::HashFiles(unsigned num_threads) {
  vector<Entry *> files;
  for (unsigned i = 0; i < entries_.size(); ++i) {
    if (entries_[i].type == kFile) {
      files.push_back(&entries_[i]);
    }
  }
  vector<string> errors(files.size());
  atomic<size_t> next(0);

  vector<thread> workers;
  num_threads = max(1u, min(num_threads, static_cast<unsigned>(files.size())));
  for (unsigned t = 0; t < num_threads; ++t) {
    workers.push_back(thread([&]() {
      size_t i;
      while ((i = next++) < files.size()) {
        HashChunks(files[i]->local_path, files[i]->size, &files[i]->hashes,
                   &errors[i]);
      }
    }));
  }
  for (unsigned t = 0; t < workers.size(); ++t) {
    workers[t].join();
  }

  bool ok = true;
  for (unsigned i = 0; i < files.size(); ++i) {
    if (!errors[i].empty()) {
      LogCvmfs(kLogCvmfs, kLogStderr, "%s: %s", files[i]->local_path.c_str(),
               errors[i].c_str());
      ok = false;
    }
  }
  return ok;
}

void IngestSQLGen::PrintSql(const string &input) const {
  printf("-- Generated by cvmfs_server ingestsql-gen from %s\n",
         CommentSafe(input).c_str());
  printf("-- %s\n", Summary(entries_).ToString(num_skipped_).c_str());
  if (no_hash_) {
    printf("-- WARNING: generated without hashing; ingestsql rejects the "
           "empty files.hashes\n");
  }
  printf("--\n");
  printf("%s\n", ingestsql::kSchemaNotes);
  for (const char * const *stmt = ingestsql::kSchema; *stmt != NULL; ++stmt) {
    printf("%s\n\n", *stmt);
  }
  printf("BEGIN TRANSACTION;\n");

  printf("\n-- dirs (name, mode, mtime, owner, grp, acl, nested)\n");
  for (unsigned i = 0; i < entries_.size(); ++i) {
    const Entry &e = entries_[i];
    if (e.type != kDir) {
      continue;
    }
    printf("INSERT INTO dirs VALUES (%s, %u, %" PRId64 ", %u, %u, %s, %d);"
           "  -- %s %s%s\n",
           SqlQuote(e.name).c_str(), static_cast<unsigned>(e.mode), e.mtime_ns,
           static_cast<unsigned>(e.uid), static_cast<unsigned>(e.gid),
           SqlQuote(e.acl).c_str(), e.nested ? 1 : 0,
           ModeString(e.type, e.mode).c_str(), TimeString(e.mtime_ns).c_str(),
           e.nested ? " nested" : "");
  }

  printf("\n-- files (name, mode, mtime, owner, grp, size, hashes, internal, "
         "compressed)\n");
  for (unsigned i = 0; i < entries_.size(); ++i) {
    const Entry &e = entries_[i];
    if (e.type != kFile) {
      continue;
    }
    const string info = ModeString(e.type, e.mode) + " "
                        + TimeString(e.mtime_ns) + " " + HumanSize(e.size);
    printf("INSERT INTO files VALUES (%s, %u, %" PRId64 ", %u, %u, %" PRIu64
           ", ",
           SqlQuote(e.name).c_str(), static_cast<unsigned>(e.mode), e.mtime_ns,
           static_cast<unsigned>(e.uid), static_cast<unsigned>(e.gid), e.size);
    if (e.hashes.size() <= 1) {
      printf("%s, 0, 0);  -- %s\n",
             SqlQuote(e.hashes.empty() ? "" : e.hashes[0]).c_str(),
             info.c_str());
      continue;
    }
    printf("  -- %s\n", info.c_str());
    for (unsigned c = 0; c < e.hashes.size(); ++c) {
      const bool last = (c == e.hashes.size() - 1);
      const uint64_t begin = static_cast<uint64_t>(c)
                             * ingestsql::kExternalChunkSize;
      const uint64_t end = last ? e.size
                                : begin + ingestsql::kExternalChunkSize;
      printf("  '%s%s'%s  -- chunk %u [%s, %s)\n", e.hashes[c].c_str(),
             last ? "" : ",", last ? "," : " ||", c, HumanSize(begin).c_str(),
             HumanSize(end).c_str());
    }
    printf("  0, 0);\n");
  }

  printf("\n-- links (name, target, mtime, owner, grp, skip_if_file_or_dir)\n");
  for (unsigned i = 0; i < entries_.size(); ++i) {
    const Entry &e = entries_[i];
    if (e.type != kLink) {
      continue;
    }
    printf("INSERT INTO links VALUES (%s, %s, %" PRId64 ", %u, %u, 0);"
           "  -- %s\n",
           SqlQuote(e.name).c_str(), SqlQuote(e.target).c_str(), e.mtime_ns,
           static_cast<unsigned>(e.uid), static_cast<unsigned>(e.gid),
           TimeString(e.mtime_ns).c_str());
  }
  printf("\nCOMMIT;\n");
}

void IngestSQLGen::PrintTable() const {
  printf("%-10s  %-11s  %12s  %-19s  %6s  %s\n", "MODE", "OWNER", "SIZE",
         "MTIME (UTC)", "CHUNKS", "NAME");
  for (unsigned i = 0; i < entries_.size(); ++i) {
    const Entry &e = entries_[i];
    const string owner = StringifyUint(e.uid) + ":" + StringifyUint(e.gid);
    const string size = (e.type == kFile) ? StringifyUint(e.size) : "-";
    const string chunks = (e.type == kFile) ? StringifyUint(e.hashes.size())
                                            : "-";
    string name = e.name;
    if (e.type == kDir && e.nested) {
      name += "  [nested]";
    }
    if (e.type == kDir && !e.acl.empty()) {
      name += "  [acl " + e.acl + "]";
    }
    if (e.type == kLink) {
      name += " -> " + e.target;
    }
    printf("%-10s  %-11s  %12s  %-19s  %6s  %s\n",
           ModeString(e.type, e.mode).c_str(), owner.c_str(), size.c_str(),
           TimeString(e.mtime_ns).c_str(), chunks.c_str(), name.c_str());
  }
}

bool IngestSQLGen::WriteDatabase(const string &path) const {
  platform_stat64 info;
  if (platform_lstat(path.c_str(), &info) == 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "%s already exists", path.c_str());
    return false;
  }
  sqlite3 *db;
  if (sqlite3_open_v2(path.c_str(), &db,
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL)
      != SQLITE_OK) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Cannot create %s: %s", path.c_str(),
             sqlite3_errmsg(db));
    sqlite3_close(db);
    return false;
  }

  bool ok = true;
  for (const char * const *stmt = ingestsql::kSchema; ok && *stmt != NULL;
       ++stmt) {
    ok = sqlite3_exec(db, *stmt, NULL, NULL, NULL) == SQLITE_OK;
  }
  ok = ok && sqlite3_exec(db, "BEGIN;", NULL, NULL, NULL) == SQLITE_OK;

  sqlite3_stmt *ins_dir = NULL, *ins_file = NULL, *ins_link = NULL;
  ok = ok
       && sqlite3_prepare_v2(db,
                             "INSERT INTO dirs (name, mode, mtime, owner, grp, "
                             "acl, nested) VALUES (?, ?, ?, ?, ?, ?, ?)",
                             -1, &ins_dir, NULL)
              == SQLITE_OK
       && sqlite3_prepare_v2(db,
                             "INSERT INTO files (name, mode, mtime, owner, "
                             "grp, size, hashes, internal, compressed) VALUES "
                             "(?, ?, ?, ?, ?, ?, ?, 0, 0)",
                             -1, &ins_file, NULL)
              == SQLITE_OK
       && sqlite3_prepare_v2(db,
                             "INSERT INTO links (name, target, mtime, owner, "
                             "grp, skip_if_file_or_dir) VALUES "
                             "(?, ?, ?, ?, ?, 0)",
                             -1, &ins_link, NULL)
              == SQLITE_OK;

  for (unsigned i = 0; ok && i < entries_.size(); ++i) {
    const Entry &e = entries_[i];
    sqlite3_stmt *s = NULL;
    switch (e.type) {
      case kDir:
        s = ins_dir;
        sqlite3_bind_text(s, 6, e.acl.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(s, 7, e.nested ? 1 : 0);
        break;
      case kFile:
        s = ins_file;
        sqlite3_bind_int64(s, 6, static_cast<sqlite3_int64>(e.size));
        sqlite3_bind_text(s, 7, JoinStrings(e.hashes, ",").c_str(), -1,
                          SQLITE_TRANSIENT);
        break;
      case kLink:
        s = ins_link;
        sqlite3_bind_text(s, 2, e.target.c_str(), -1, SQLITE_TRANSIENT);
        break;
    }
    sqlite3_bind_text(s, 1, e.name.c_str(), -1, SQLITE_TRANSIENT);
    if (e.type != kLink) {
      sqlite3_bind_int(s, 2, static_cast<int>(e.mode));
    }
    sqlite3_bind_int64(s, 3, e.mtime_ns);
    sqlite3_bind_int64(s, 4, e.uid);
    sqlite3_bind_int64(s, 5, e.gid);
    ok = sqlite3_step(s) == SQLITE_DONE;
    sqlite3_reset(s);
    sqlite3_clear_bindings(s);
  }
  if (!ok) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Failed to write %s: %s", path.c_str(),
             sqlite3_errmsg(db));
  }
  sqlite3_finalize(ins_dir);
  sqlite3_finalize(ins_file);
  sqlite3_finalize(ins_link);
  ok = ok && sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL) == SQLITE_OK;
  // A WAL database cannot be opened read-only (as ingestsql does) from a
  // directory the reader cannot write the -shm file into.
  ok = ok
       && sqlite3_exec(db, "PRAGMA journal_mode=DELETE;", NULL, NULL, NULL)
              == SQLITE_OK;
  sqlite3_close(db);
  if (!ok) {
    unlink(path.c_str());
  }
  return ok;
}

int IngestSQLGen::Main(const ArgumentList &args) {
  if (args.find('S') != args.end()) {
    printf("%s\n", ingestsql::kSchemaNotes);
    for (const char * const *stmt = ingestsql::kSchema; *stmt != NULL; ++stmt) {
      printf("%s\n\n", *stmt);
    }
    return 0;
  }

  if (args.find('i') == args.end()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Missing input path (-i)");
    return 1;
  }
  string input = *args.find('i')->second;
  while (input.size() > 1 && input[input.size() - 1] == '/') {
    input.erase(input.size() - 1);
  }

  string prefix;
  char *real_input = realpath(input.c_str(), NULL);
  if (real_input != NULL) {
    prefix = "/" + GetFileName(real_input);
    free(real_input);
  }
  if (args.find('p') != args.end()) {
    prefix = *args.find('p')->second;
  }
  if (!NormalizeRepoPath(prefix, &prefix)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Invalid repository path: %s",
             prefix.c_str());
    return 1;
  }

  const bool to_db = args.find('o') != args.end();
  platform_stat64 info;
  if (to_db && platform_lstat(args.find('o')->second->c_str(), &info) == 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "%s already exists",
             args.find('o')->second->c_str());
    return 1;
  }
  no_hash_ = args.find('H') != args.end();
  const bool no_hash = no_hash_;
  if (to_db && no_hash) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "-H produces entries that cannot be ingested; not writing a "
             "database");
    return 1;
  }
  string format = to_db ? "" : "sql";
  if (args.find('f') != args.end()) {
    format = *args.find('f')->second;
    if (format != "sql" && format != "table") {
      LogCvmfs(kLogCvmfs, kLogStderr, "Unknown format: %s", format.c_str());
      return 1;
    }
  }

  override_owner_ = false;
  uid_ = 0;
  gid_ = 0;
  if (args.find('u') != args.end()) {
    const vector<string> ids = SplitString(*args.find('u')->second, ':');
    uint64_t uid, gid;
    if (ids.size() != 2 || !String2Uint64Parse(ids[0], &uid)
        || !String2Uint64Parse(ids[1], &gid)) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "Invalid ownership, expected <uid>:<gid>");
      return 1;
    }
    override_owner_ = true;
    uid_ = static_cast<uid_t>(uid);
    gid_ = static_cast<gid_t>(gid);
  }

  unsigned num_threads = thread::hardware_concurrency();
  if (args.find('j') != args.end()) {
    uint64_t n;
    if (!String2Uint64Parse(*args.find('j')->second, &n) || n == 0) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Invalid number of threads");
      return 1;
    }
    num_threads = static_cast<unsigned>(n);
  }

  if (args.find('N') != args.end()) {
    const vector<string> globs = SplitString(*args.find('N')->second, ',');
    for (unsigned i = 0; i < globs.size(); ++i) {
      if (globs[i].empty()) {
        continue;
      }
      nested_globs_.push_back(globs[i][0] == '/' ? globs[i] : "/" + globs[i]);
    }
  }

  num_skipped_ = 0;
  num_warnings_ = 0;
  if (platform_lstat(input.c_str(), &info) != 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Cannot stat %s: %s", input.c_str(),
             strerror(errno));
    return 1;
  }
  if (prefix == "/" && !S_ISDIR(info.st_mode)) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Only a directory can be mapped onto the repository root");
    return 1;
  }
  if (!Scan(input, prefix)) {
    return 1;
  }
  if (!no_hash && !HashFiles(num_threads)) {
    return 1;
  }

  if (to_db) {
    const string db_path = *args.find('o')->second;
    if (!WriteDatabase(db_path)) {
      return 1;
    }
    LogCvmfs(kLogCvmfs, kLogStderr, "Wrote %s: %s", db_path.c_str(),
             Summary(entries_).ToString(num_skipped_).c_str());
  }
  if (format == "sql") {
    PrintSql(input);
  } else if (format == "table") {
    PrintTable();
  }
  if (num_warnings_ > 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "%u warning(s)", num_warnings_);
  }
  return 0;
}

}  // namespace swissknife
