/**
 * This file is part of the CernVM File System.
 */

#define __STDC_FORMAT_MACROS
#include "authz_session.h"

#include <errno.h>
#include <inttypes.h>
#ifdef __APPLE__
#include <sys/sysctl.h>
#endif

#include <cassert>
#include <cstdio>
#include <cstring>
#include <vector>

#include "authz/authz_fetch.h"
#include "logging.h"
#include "platform.h"
#include "statistics.h"
#include "util/posix.h"
#include "util_concurrency.h"

using namespace std;  // NOLINT


AuthzSessionManager::AuthzSessionManager()
  : deadline_sweep_pids_(0)
  , deadline_sweep_creds_(0)
  , authz_fetcher_(NULL)
  , no_pid_(NULL)
  , no_session_(NULL)
  , n_fetch_(NULL)
  , n_grant_(NULL)
  , n_deny_(NULL)
{
  int retval = pthread_mutex_init(&lock_pid2session_, NULL);
  assert(retval == 0);
  retval = pthread_mutex_init(&lock_session2cred_, NULL);
  assert(retval == 0);

  session2cred_.Init(16, SessionKey(), HashSessionKey);
  pid2session_.Init(16, PidKey(), HashPidKey);
}


AuthzSessionManager::~AuthzSessionManager() {
  int retval = pthread_mutex_destroy(&lock_pid2session_);
  assert(retval == 0);
  retval = pthread_mutex_destroy(&lock_session2cred_);
  assert(retval == 0);

  SessionKey empty_key;
  for (unsigned i = 0; i < session2cred_.capacity(); ++i) {
    if (session2cred_.keys()[i] != empty_key) {
      if ((session2cred_.values() + i)->token.data != NULL)
        free((session2cred_.values() + i)->token.data);
    }
  }
}


void AuthzSessionManager::ClearSessionCache() {
  MutexLockGuard m(&lock_session2cred_);
  session2cred_.Clear();
  no_session_->Set(0);
}


AuthzSessionManager *AuthzSessionManager::Create(
  AuthzFetcher *authz_fetcher,
  perf::Statistics *statistics)
{
  AuthzSessionManager *authz_mgr = new AuthzSessionManager();
  authz_mgr->authz_fetcher_ = authz_fetcher;

  authz_mgr->no_pid_ = statistics->Register("authz.no_pid", "cached pids");
  authz_mgr->no_session_ = statistics->Register(
    "authz.no_session", "cached sessions");
  authz_mgr->n_fetch_ = statistics->Register(
    "authz.n_fetch", "overall number of authz helper invocations");
  authz_mgr->n_grant_ = statistics->Register(
    "authz.n_grant", "overall number of granted membership queries");
  authz_mgr->n_deny_ = statistics->Register(
    "authz.n_deny", "overall number of denied membership queries");

  return authz_mgr;
}


/**
 * Gathers SID, birthday, uid, and gid from given PID.
 */
bool AuthzSessionManager::GetPidInfo(pid_t pid, PidKey *pid_key) {
  int retval;

  // TODO(jblomer): better in platform.h?  Maybe a bit too bulky for that?
#ifdef __APPLE__
  pid_key->sid = getsid(pid);
  if (pid_key->sid == static_cast<pid_t>(-1)) {
    LogCvmfs(kLogAuthz, kLogDebug, "failed to get sid (%s)", strerror(errno));
    return false;
  }

  int mib[] = { CTL_KERN, KERN_PROC, KERN_PROC_PID, pid };
  struct kinfo_proc kp;
  size_t len = sizeof(kp);
  retval = sysctl(mib, 4, &kp, &len, NULL, 0);
  if (retval == -1) {
    LogCvmfs(kLogAuthz, kLogDebug, "failed to get pid info (%s)",
             strerror(errno));
    return false;
  }
  pid_key->uid = kp.kp_eproc.e_pcred.p_ruid;
  pid_key->gid = kp.kp_eproc.e_pcred.p_rgid;
  int64_t usec =
    static_cast<int64_t>(kp.kp_proc.p_un.__p_starttime.tv_sec) * 1000000;
  usec += static_cast<int64_t>(kp.kp_proc.p_un.__p_starttime.tv_usec);
  pid_key->pid_bday = usec;
  pid_key->pid = pid;
  return true;
#endif

  const int kMaxProcPath = 64;  // Enough to store /proc/PID/stat
  char pid_path[kMaxProcPath];
  if (snprintf(pid_path, kMaxProcPath, "/proc/%d/stat", pid) >= kMaxProcPath) {
    return false;
  }

  FILE *fp_stat = fopen(pid_path, "r");
  if (fp_stat == NULL) {
    LogCvmfs(kLogAuthz, kLogDebug,
             "Failed to open status file /proc/%d/stat: (errno=%d) %s",
             pid, errno, strerror(errno));
    LogCvmfs(kLogAuthz, kLogSyslogWarn | kLogDebug,
             "Authorization for session %d disappeared", pid);
    return false;
  }

  // The uid and gid can be gathered from /proc/$PID/stat file ownership
  int fd_stat = fileno(fp_stat);
  platform_stat64 info;
  retval = platform_fstat(fd_stat, &info);
  if (retval != 0) {
    fclose(fp_stat);
    LogCvmfs(kLogAuthz, kLogDebug,
             "Failed to get stat information of running process.");
    return false;
  }
  pid_key->uid = info.st_uid;
  pid_key->gid = info.st_gid;

  // TODO(bbockelm): EINTR handling
  retval = fscanf(fp_stat, "%*d %*s %*c %*d %*d %d %*d %*d %*u %*u %*u %*u "
                  "%*u %*u %*u %*d %*d %*d %*d %*d %*d %" SCNu64,
                  &(pid_key->sid), &(pid_key->pid_bday));
  fclose(fp_stat);
  if (retval != 2) {
    if (errno == 0) {
      errno = EINVAL;
    }
    LogCvmfs(kLogAuthz, kLogDebug, "Failed to parse status file for "
             "pid %d: (errno=%d) %s, fscanf result %d", pid, errno,
             strerror(errno), retval);
    return false;
  }

  pid_key->pid = pid;
  return true;
}


/**
 * Caller is responsible for freeing the returned token.
 */
AuthzToken *AuthzSessionManager::GetTokenCopy(
  const pid_t pid,
  const std::string &membership)
{
  SessionKey session_key;
  PidKey pid_key;
  bool retval = LookupSessionKey(pid, &pid_key, &session_key);
  if (!retval)
    return NULL;

  AuthzData authz_data;
  const bool granted =
    LookupAuthzData(pid_key, session_key, membership, &authz_data);
  if (!granted)
    return NULL;
  return authz_data.token.DeepCopy();
}


bool AuthzSessionManager::IsMemberOf(
  const pid_t pid,
  const std::string &membership)
{
  SessionKey session_key;
  PidKey pid_key;
  bool retval = LookupSessionKey(pid, &pid_key, &session_key);
  if (!retval)
    return false;

  AuthzData authz_data;
  return LookupAuthzData(pid_key, session_key, membership, &authz_data);
}


/**
 * Calls out to the AuthzFetcher if the data is not cached.  Verifies the
 * membership.
 */
bool AuthzSessionManager::LookupAuthzData(
  const PidKey &pid_key,
  const SessionKey &session_key,
  const std::string &membership,
  AuthzData *authz_data)
{
  assert(authz_data != NULL);

  bool found;
  {
    MutexLockGuard m(&lock_session2cred_);
    MaySweepCreds();
    found = session2cred_.Lookup(session_key, authz_data);
  }
  if (found) {
    LogCvmfs(kLogAuthz, kLogDebug,
             "cached authz data for sid %d, membership %s, status %d",
             session_key.sid, authz_data->membership.c_str(),
             authz_data->status);
    const bool granted = authz_data->IsGranted(membership);
    if (granted)
      perf::Inc(n_grant_);
    else
      perf::Inc(n_deny_);
    return granted;
  }

  // Not found in cache, ask for help
  perf::Inc(n_fetch_);
  unsigned ttl;
  authz_data->status = authz_fetcher_->Fetch(
    AuthzFetcher::QueryInfo(pid_key.pid, pid_key.uid, pid_key.gid, membership),
    &(authz_data->token), &ttl);
  authz_data->deadline = platform_monotonic_time() + ttl;
  if (authz_data->status == kAuthzOk)
    authz_data->membership = membership;
  LogCvmfs(kLogAuthz, kLogDebug,
           "fetched authz data for sid %d (pid %d), membership %s, status %d, "
           "ttl %u", session_key.sid, pid_key.pid,
           authz_data->membership.c_str(), authz_data->status, ttl);

  {
    MutexLockGuard m(&lock_session2cred_);
    if (!session2cred_.Contains(session_key)) perf::Inc(no_session_);
    session2cred_.Insert(session_key, *authz_data);
  }
  const bool granted = authz_data->status == kAuthzOk;
  if (granted)
    perf::Inc(n_grant_);
  else
    perf::Inc(n_deny_);
  return granted;
}


/**
 * Translate a PID and its birthday into an SID and its birthday.  The Session
 * ID and its birthday together with UID and GID make the Session Key.  The
 * translation result is cached in pid2session_.
 */
bool AuthzSessionManager::LookupSessionKey(
  pid_t pid,
  PidKey *pid_key,
  SessionKey *session_key)
{
  assert(pid_key != NULL);
  assert(session_key != NULL);
  if (!GetPidInfo(pid, pid_key))
    return false;

  bool found;
  {
    MutexLockGuard m(&lock_pid2session_);
    found = pid2session_.Lookup(*pid_key, session_key);
    MaySweepPids();
  }
  if (found) {
    LogCvmfs(kLogAuthz, kLogDebug,
             "Session key %d/%" PRIu64 " in cache; sid=%d, bday=%" PRIu64,
             pid_key->pid, pid_key->pid_bday,
             session_key->sid, session_key->sid_bday);
    return true;
  }

  LogCvmfs(kLogAuthz, kLogDebug,
           "Session key not found in cache, getting information from OS");
  PidKey sid_key;
  pid_t sid = pid_key->sid;
  if (sid == 0) {
    // This can happen inside process namespaces such as those used by
    //   singularity and cvmfsexec.  Use init process id instead.
    sid = 1;
  }
  if (!GetPidInfo(sid, &sid_key))
    return false;

  session_key->sid = sid_key.pid;
  session_key->sid_bday = sid_key.pid_bday;
  {
    MutexLockGuard m(&lock_pid2session_);
    pid_key->deadline = platform_monotonic_time() + kPidLifetime;
    if (!pid2session_.Contains(*pid_key)) perf::Inc(no_pid_);
    pid2session_.Insert(*pid_key, *session_key);
  }

  LogCvmfs(kLogAuthz, kLogDebug, "Lookup key %d/%" PRIu64 "; sid=%d, bday=%llu",
           pid_key->pid, pid_key->pid_bday,
           session_key->sid, session_key->sid_bday);
  return true;
}


/**
 * Scan through old sessions only every so often.
 */
void AuthzSessionManager::MaySweepCreds() {
  uint64_t now = platform_monotonic_time();
  if (now >= deadline_sweep_creds_) {
    SweepCreds(now);
    deadline_sweep_creds_ = now + kSweepInterval;
  }
}


/**
 * Scan through old PIDs only every so often.
 */
void AuthzSessionManager::MaySweepPids() {
  uint64_t now = platform_monotonic_time();
  if (now >= deadline_sweep_pids_) {
    SweepPids(now);
    deadline_sweep_pids_ = now + kSweepInterval;
  }
}


/**
 * Remove cache PIDs with expired cache life time.
 * TODO(jblomer): a generalized sweeping can become part of smallhash
 */
void AuthzSessionManager::SweepCreds(uint64_t now) {
  SessionKey empty_key;
  vector<SessionKey> trash_bin;
  for (unsigned i = 0; i < session2cred_.capacity(); ++i) {
    SessionKey this_key = session2cred_.keys()[i];
    if (this_key != empty_key) {
      if (now >= (session2cred_.values() + i)->deadline)
        trash_bin.push_back(this_key);
    }
  }

  for (unsigned i = 0; i < trash_bin.size(); ++i) {
    session2cred_.Erase(trash_bin[i]);
    perf::Dec(no_session_);
  }
}


/**
 * Remove cache PIDs with expired cache life time.
 * TODO(jblomer): a generalized sweeping can become part of smallhash
 */
void AuthzSessionManager::SweepPids(uint64_t now) {
  PidKey empty_key;
  vector<PidKey> trash_bin;
  for (unsigned i = 0; i < pid2session_.capacity(); ++i) {
    PidKey this_key = pid2session_.keys()[i];
    if (this_key != empty_key) {
      if (now >= this_key.deadline)
        trash_bin.push_back(this_key);
    }
  }

  for (unsigned i = 0; i < trash_bin.size(); ++i) {
    pid2session_.Erase(trash_bin[i]);
    perf::Dec(no_pid_);
  }
}
