/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_AUTHZ_AUTHZ_SESSION_H_
#define CVMFS_AUTHZ_AUTHZ_SESSION_H_

#include <inttypes.h>
#include <pthread.h>
#include <unistd.h>

#include <string>

#include "authz/authz.h"
#include "duplex_testing.h"
#include "smallhash.h"
#include "statistics.h"
#include "util/murmur.hxx"
#include "util/single_copy.h"

class AuthzFetcher;

// TODO(jblomer): add audit log

/**
 * The authorization manager maintains a list of sessions (sid and its birthday)
 * and their associated credentials.  It is used to pass the credentials to the
 * download module and to control access to the repository.  Every credential
 * has a membership (a string) associated with it.  The credential is thereby
 * confirmed to be a member of the specified group.  The group/membership can
 * be anything, for instance a role in a certificate.  It is stored in the cvmfs
 * root file catalog.
 *
 * An AuthzFetcher is used to gather credentials that are not cached.  Note that
 * the credentials are fetched using original pid/uid/gid but cached under the
 * session.
 */
class AuthzSessionManager : SingleCopy {
  FRIEND_TEST(T_AuthzSession, GetPidInfo);
  FRIEND_TEST(T_AuthzSession, LookupAuthzData);
  FRIEND_TEST(T_AuthzSession, LookupSessionKey);

 public:
  static AuthzSessionManager *Create(AuthzFetcher *authz_fetcher,
                                     perf::Statistics *statistics);
  ~AuthzSessionManager();

  AuthzToken *GetTokenCopy(const pid_t pid, const std::string &membership);
  bool IsMemberOf(const pid_t pid, const std::string &membership);

  /**
   * When the membership string in the root file catalog changes, all entries in
   * the cache become invalid because they only vouch for the previous
   * membership entry. This function is called by MountPoint::ReEvaluateAuthz.
   */
  void ClearSessionCache();

 private:
  /**
   * Sweep caches from old entries not more often than every 5 seconds.
   */
  static const unsigned kSweepInterval = 5;

  /**
   * Pid to session information is cached for 2 minutes.
   */
  static const unsigned kPidLifetime = 120;

  /**
   * Extended information on a PID.
   */
  struct PidKey {
    PidKey() : pid(-1), uid(-1), gid(-1), sid(-1), pid_bday(0), deadline(0) { }
    pid_t pid;
    uid_t uid;
    gid_t gid;
    pid_t sid;
    uint64_t pid_bday;
    uint64_t deadline;

    bool operator==(const PidKey &other) const {
      return (pid == other.pid) && (pid_bday == other.pid_bday);
    }

    bool operator!=(const PidKey &other) const { return !(*this == other); }
  };

  /**
   * Extended information on an SID.
   */
  struct SessionKey {
    SessionKey() : sid(-1), sid_bday(0) { }
    pid_t sid;
    uint64_t sid_bday;

    bool operator==(const SessionKey &other) const {
      return (sid == other.sid) && (sid_bday == other.sid_bday);
    }

    bool operator!=(const SessionKey &other) const { return !(*this == other); }
  };

  static uint32_t HashPidKey(const PidKey &key) {
    struct {
      uint64_t bday;
      pid_t pid;
    } __attribute__((packed)) key_info;
    key_info.pid = key.pid;
    key_info.bday = key.pid_bday;
    return MurmurHash2(&key_info, sizeof(key_info), 0x07387a4f);
  }

  static uint32_t HashSessionKey(const SessionKey &key) {
    struct {
      uint64_t bday;
      pid_t pid;
    } __attribute__((packed)) key_info;
    key_info.pid = key.sid;
    key_info.bday = key.sid_bday;
    return MurmurHash2(&key_info, sizeof(key_info), 0x07387a4f);
  }

  AuthzSessionManager();

  bool GetPidInfo(pid_t pid, PidKey *pid_key);
  bool LookupSessionKey(pid_t pid, PidKey *pid_key, SessionKey *session_key);
  void MaySweepPids();
  void SweepPids(uint64_t now);

  bool LookupAuthzData(const PidKey &pid_key,
                       const SessionKey &session_key,
                       const std::string &membership,
                       AuthzData *authz_data);
  void MaySweepCreds();
  void SweepCreds(uint64_t now);

  /**
   * Caches (extended) session information for an (extended) pid.
   */
  SmallHashDynamic<PidKey, SessionKey> pid2session_;
  pthread_mutex_t lock_pid2session_;
  uint64_t deadline_sweep_pids_;

  /**
   * Caches credentials corresponding to a session.
   */
  SmallHashDynamic<SessionKey, AuthzData> session2cred_;
  pthread_mutex_t lock_session2cred_;
  uint64_t deadline_sweep_creds_;

  /**
   * The helper that takes care of bringing in credentials from the client
   * context.
   */
  AuthzFetcher *authz_fetcher_;

  perf::Counter *no_pid_;
  perf::Counter *no_session_;
  perf::Counter *n_fetch_;
  perf::Counter *n_grant_;
  perf::Counter *n_deny_;
};

#endif  // CVMFS_AUTHZ_AUTHZ_SESSION_H_
