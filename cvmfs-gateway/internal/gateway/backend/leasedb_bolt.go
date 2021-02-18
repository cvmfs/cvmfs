package backend

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

// BoltLeaseDB is a LeaseDB backed by BoltDB
type BoltLeaseDB struct {
	store *bolt.DB
	locks NamedLocks // Per-repository commit locks
}

// OpenBoltLeaseDB creates a new embedded lease DB
func OpenBoltLeaseDB(workDir string) (*BoltLeaseDB, error) {
	if err := os.MkdirAll(workDir, 0777); err != nil {
		return nil, errors.Wrap(err, "could not create directory for backing store")
	}
	store, err := bolt.Open(workDir+"/bolt_lease.db", 0666, nil)
	if err != nil {
		return nil, errors.Wrap(err, "could not open backing store (BoltDB)")
	}

	store.Update(func(txn *bolt.Tx) error {
		// Enable all repos
		if txn.Bucket([]byte("disabled_repos")) != nil {
			txn.DeleteBucket([]byte("disabled_repos"))
		}
		_, err := txn.CreateBucketIfNotExists([]byte("disabled_repos"))
		if err != nil {
			return errors.Wrap(err, "could not create bucket: 'disabled_repos'")
		}

		_, err = txn.CreateBucketIfNotExists([]byte("tokens"))
		if err != nil {
			return errors.Wrap(err, "could not create bucket: 'tokens'")
		}

		return nil
	})

	gw.Log("leasedb_bolt", gw.LogInfo).
		Msgf("database opened (work dir: %v)", workDir)

	return &BoltLeaseDB{store: store, locks: NamedLocks{}}, nil
}

// Close the lease database
func (db *BoltLeaseDB) Close() error {
	return db.store.Close()
}

// NewLease attemps to acquire a new lease for the given path
func (db *BoltLeaseDB) NewLease(ctx context.Context, keyID, leasePath string, protocolVersion int, token LeaseToken) error {
	return db.store.Update(func(txn *bolt.Tx) error {
		t0 := time.Now()
		repoName, subPath, err := gw.SplitLeasePath(leasePath)
		if err != nil {
			return errors.Wrap(err, "invalid lease path")
		}

		lease := Lease{KeyID: keyID, ProtocolVersion: protocolVersion, Token: token}
		buf, err := lease.Serialize()
		if err != nil {
			return err
		}

		disabledRepos := txn.Bucket([]byte("disabled_repos"))
		if disabledRepos.Get([]byte(repoName)) != nil {
			return ErrRepoDisabled
		}

		tokenBucket := txn.Bucket([]byte("tokens"))
		repoBucket, err := txn.CreateBucketIfNotExists([]byte(repoName))
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("could not create bucket: '%v'", repoName))
		}

		// Iterate through the key of the bucket corresponding to the repository
		// and find any conflicting lease (there can be at most 1)
		var existing struct {
			path  string
			lease *Lease
		}

		cursor := repoBucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			kPath := string(k)
			if gw.CheckPathOverlap(kPath, subPath) {
				l, err := DeserializeLease(v)
				if err != nil {
					return err
				}
				existing.lease = l
				existing.path = kPath
				break
			}
		}

		// If a conflicting lease exists, examine its expiration time
		if existing.path != "" {
			timeLeft := existing.lease.Token.Expiration.Sub(time.Now())
			if timeLeft > 0 {
				gw.LogC(ctx, "leasedb_bolt", gw.LogDebug).
					Str("operation", "new_lease").
					Str("token", token.TokenStr).
					Msgf("path_busy, time left: %v s", timeLeft.Seconds())
				return PathBusyError{timeLeft}
			}

			tokenBucket.Delete([]byte(existing.lease.Token.TokenStr))
			repoBucket.Delete([]byte(existing.path))
			tokenBucket.Put([]byte(token.TokenStr), []byte(leasePath))
			repoBucket.Put([]byte(subPath), buf)
		} else {
			tokenBucket.Put([]byte(token.TokenStr), []byte(leasePath))
			repoBucket.Put([]byte(subPath), buf)
		}

		gw.LogC(ctx, "leasedb_bolt", gw.LogDebug).
			Str("operation", "new_lease").
			Str("token", token.TokenStr).
			Dur("task_dt", time.Since(t0)).
			Msgf("key: %v, path: %v", keyID, leasePath)

		return nil
	})
}

// GetLeases returns a list of all active leases
func (db *BoltLeaseDB) GetLeases(ctx context.Context) (map[string]Lease, error) {
	t0 := time.Now()
	leases := make(map[string]Lease)
	err := db.store.View(func(txn *bolt.Tx) error {
		if err := txn.ForEach(func(name []byte, b *bolt.Bucket) error {
			bucketName := string(name)
			if bucketName == "tokens" || bucketName == "disabled_repos" {
				return nil
			}
			if err := b.ForEach(func(k, v []byte) error {
				leasePath := bucketName + string(k)
				lease, err := DeserializeLease(v)
				if err != nil {
					return err
				}
				leases[leasePath] = *lease
				return nil
			}); err != nil {
				return errors.Wrap(
					err, fmt.Sprintf("iteration error in bucket: %v", bucketName))
			}
			return nil
		}); err != nil {
			return errors.Wrap(err, "iteration error over repository buckets")
		}

		return nil
	})

	gw.LogC(ctx, "leasedb_bolt", gw.LogDebug).
		Str("operation", "get_leases").
		Dur("task_dt", time.Since(t0)).
		Msgf("found %v leases", len(leases))

	return leases, err
}

// GetLease returns the lease for a given token string
func (db *BoltLeaseDB) GetLease(ctx context.Context, tokenStr string) (string, *Lease, error) {
	t0 := time.Now()

	lease := Lease{}
	var leasePath string
	err := db.store.View(func(txn *bolt.Tx) error {
		tokens := txn.Bucket([]byte("tokens"))
		lPath := tokens.Get([]byte(tokenStr))
		if lPath == nil {
			return InvalidLeaseError{}
		}

		repoName, subPath, err := gw.SplitLeasePath(string(lPath))
		if err != nil {
			return errors.Wrap(err, "invalid lease path")
		}
		bucket := txn.Bucket([]byte(repoName))
		if bucket == nil {
			return fmt.Errorf("invalid repo name: %v", repoName)
		}
		v := bucket.Get([]byte(subPath))
		if v == nil {
			return InvalidLeaseError{}
		}
		l, err := DeserializeLease(v)
		if err != nil {
			return err
		}
		leasePath = string(lPath)
		lease = *l
		return nil
	})

	gw.LogC(ctx, "leasedb_bolt", gw.LogDebug).
		Str("operation", "get_lease").
		Str("token", tokenStr).
		Dur("task_dt", time.Since(t0)).
		Msgf("success")

	return leasePath, &lease, err
}

// CancelLeases cancels all active leases
func (db *BoltLeaseDB) CancelLeases(ctx context.Context, repoPath string) error {
	return db.store.Update(func(txn *bolt.Tx) error {
		t0 := time.Now()

		repository, prefix, err := gw.SplitLeasePath(repoPath)
		if err != nil {
			return errors.Wrap(err, "invalid path")
		}

		tokens := txn.Bucket([]byte("tokens"))

		leases := txn.Bucket([]byte(repository))
		if leases == nil {
			return fmt.Errorf("missing '%v' bucket", repository)
		}

		tokensForDeletion := make([]string, 0)
		pathsForDeletion := make([]string, 0)
		leases.ForEach(func(subPath, leaseBytes []byte) error {
			sp := string(subPath)
			if strings.HasPrefix(sp, prefix) {
				pathsForDeletion = append(pathsForDeletion, sp)
				l, err := DeserializeLease(leaseBytes)
				if err != nil {
					return err
				}
				tokensForDeletion = append(tokensForDeletion, l.Token.TokenStr)
			}
			return nil
		})

		for _, t := range tokensForDeletion {
			tokens.Delete([]byte(t))
		}
		for _, p := range pathsForDeletion {
			leases.Delete([]byte(p))
		}

		gw.LogC(ctx, "leasedb_bolt", gw.LogDebug).
			Str("operation", "cancel_leases").
			Dur("task_dt", time.Since(t0)).
			Msgf("all leases cancelled")

		return nil
	})
}

// CancelLease cancels the lease for a token string
func (db *BoltLeaseDB) CancelLease(ctx context.Context, tokenStr string) error {
	return db.store.Update(func(txn *bolt.Tx) error {
		t0 := time.Now()

		tokens := txn.Bucket([]byte("tokens"))
		leasePath := tokens.Get([]byte(tokenStr))
		if leasePath == nil {
			gw.LogC(ctx, "leasedb_bolt", gw.LogDebug).
				Str("operation", "cancel_lease").
				Str("token", tokenStr).
				Msgf("cancellation failed, invalid token")
			return InvalidLeaseError{}
		}

		repoName, subPath, err := gw.SplitLeasePath(string(leasePath))
		if err != nil {
			return errors.Wrap(err, "invalid lease path")
		}
		bucket := txn.Bucket([]byte(repoName))
		if bucket == nil {
			return fmt.Errorf("invalid repo name: %v", repoName)
		}

		bucket.Delete([]byte(subPath))
		tokens.Delete([]byte(tokenStr))

		gw.LogC(ctx, "leasedb_bolt", gw.LogDebug).
			Str("operation", "cancel_lease").
			Str("token", tokenStr).
			Dur("task_dt", time.Since(t0)).
			Msgf("lease cancelled")

		return nil
	})
}

// WithLock runs the given task while holding a commit lock for the repository
func (db *BoltLeaseDB) WithLock(ctx context.Context, repository string, task func() error) error {
	return db.locks.WithLock(repository, task)
}

// SetRepositoryEnabled sets the enabled/disabled status for a given repository
func (db *BoltLeaseDB) SetRepositoryEnabled(
	ctx context.Context, repository string, enable bool) error {
	return db.store.Update(func(txn *bolt.Tx) error {
		t0 := time.Now()

		disabledRepos := txn.Bucket([]byte("disabled_repos"))

		if enable {
			disabledRepos.Delete([]byte(repository))
		} else {
			disabledRepos.Put([]byte(repository), []byte{})
		}

		gw.LogC(ctx, "leasedb_bolt", gw.LogDebug).
			Str("operation", "set_repo_enabled").
			Dur("task_dt", time.Since(t0)).
			Str("repository", repository).
			Bool("enabled", enable).
			Msgf("repository status changed")

		return nil
	})
}

// GetRepositoryEnabled returns the enabled status of a repository
func (db *BoltLeaseDB) GetRepositoryEnabled(ctx context.Context, repository string) bool {
	enabled := true
	db.store.View(func(txn *bolt.Tx) error {
		t0 := time.Now()

		disabledRepos := txn.Bucket([]byte("disabled_repos"))

		if disabledRepos.Get([]byte(repository)) != nil {
			enabled = false
		}

		gw.LogC(ctx, "leasedb_bolt", gw.LogDebug).
			Str("operation", "get_repo_enabled").
			Dur("task_dt", time.Since(t0)).
			Str("repository", repository).
			Bool("enabled", enabled).
			Msgf("repository status queried")

		return nil
	})

	return enabled
}
