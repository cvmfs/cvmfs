package backend

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

// PathBusyError is returned as error value for new lease requests on
// paths which are already leased
type PathBusyError struct {
	remaining time.Duration
}

func (e PathBusyError) Error() string {
	return fmt.Sprintf("path busy, remaining = %vs", e.remaining.Seconds())
}

// Remaining number of seconds on the existing lease
func (e *PathBusyError) Remaining() time.Duration {
	return e.remaining
}

// InvalidLeaseError is returned by the GetLeaseXXXX methods in case a
// lease does not exist for the specified path
type InvalidLeaseError struct {
}

func (e InvalidLeaseError) Error() string {
	return "invalid lease"
}

// Lease describes an exclusive lease to a subpath inside the repository:
// keyID and token ()
type Lease struct {
	Token           string
	Repository      string
	Path            string
	KeyID           string
	Expiration      time.Time
	ProtocolVersion int
	Hostname        string
}

func (l Lease) CombinedLeasePath() string {
	return l.Repository + "/" + strings.TrimPrefix(l.Path, "/")
}

func CreateLease(ctx context.Context, tx *sql.Tx, lease Lease) error {
	t0 := time.Now()

	res, err := tx.ExecContext(ctx,
		"insert into Lease (Token, Repository, Path, KeyID, Expiration, ProtocolVersion, Hostname) values (?, ?, ?, ?, ?, ?, ?);",
		lease.Token, lease.Repository, lease.Path, lease.KeyID, lease.Expiration.UnixMilli(), lease.ProtocolVersion, lease.Hostname)
	if err != nil {
		return fmt.Errorf("could not insert new lease: %w", err)
	}
	numInserts, err := res.RowsAffected()
	// err should be nil if DB driver returns the number of affected rows
	if err == nil && numInserts == 0 {
		return fmt.Errorf("new lease not inserted")
	}

	gw.LogC(ctx, "lease_entity", gw.LogDebug).
		Str("operation", "create").
		Dur("task_dt", time.Since(t0)).
		Msgf("key: %v, repo: %v, path: %v, hostname: %v", lease.KeyID, lease.Repository, lease.Path, lease.Hostname)

	return nil
}

func FindAllLeases(ctx context.Context, tx *sql.Tx) ([]Lease, error) {
	t0 := time.Now()

	rows, err := tx.QueryContext(ctx, "select * from Lease;")
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	leases := make([]Lease, 0)
	for rows.Next() {
		var lease Lease
		if err := scanLease(rows, &lease); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		leases = append(leases, lease)
	}

	gw.LogC(ctx, "lease_entity", gw.LogDebug).
		Str("operation", "find_all").
		Dur("task_dt", time.Since(t0)).
		Msgf("found %v leases", len(leases))

	return leases, nil
}

func FindAllActiveLeases(ctx context.Context, tx *sql.Tx) ([]Lease, error) {
	t0 := time.Now()

	rows, err := tx.QueryContext(ctx, "select * from Lease where Expiration >= ?;", t0.UnixMilli())
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	leases := make([]Lease, 0)
	for rows.Next() {
		var lease Lease
		if err := scanLease(rows, &lease); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		leases = append(leases, lease)
	}

	gw.LogC(ctx, "lease_entity", gw.LogDebug).
		Str("operation", "find_all_active").
		Dur("task_dt", time.Since(t0)).
		Msgf("found %v leases", len(leases))

	return leases, nil
}

func FindAllLeasesByRepositoryAndOverlappingPath(ctx context.Context, tx *sql.Tx, repository, path string) ([]Lease, error) {
	t0 := time.Now()

	rows, err := tx.QueryContext(
		ctx,
		"select * from Lease where Repository = ? and (? like Path || '%' or Path like ? || '%');", repository, path, path)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	leases := make([]Lease, 0)
	for rows.Next() {
		var lease Lease
		if err := scanLease(rows, &lease); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		leases = append(leases, lease)
	}

	gw.LogC(ctx, "lease_entity", gw.LogDebug).
		Str("operation", "find_all_by_repository_and_overlapping_path").
		Dur("task_dt", time.Since(t0)).
		Msgf("found %v leases", len(leases))

	return leases, nil
}

func FindLeaseByToken(ctx context.Context, tx *sql.Tx, token string) (*Lease, error) {
	t0 := time.Now()

	rows, err := tx.QueryContext(
		ctx,
		"select * from Lease where Token = ?;", token)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var lease Lease
	if rows.Next() {
		if err := scanLease(rows, &lease); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
	} else {
		return nil, nil
	}

	gw.LogC(ctx, "lease_entity", gw.LogDebug).
		Str("operation", "find_by_token").
		Dur("task_dt", time.Since(t0)).
		Msgf("success")

	return &lease, nil
}

func DeleteAllExpiredLeases(ctx context.Context, tx *sql.Tx) error {
	t0 := time.Now()

	res, err := tx.ExecContext(ctx, "delete from Lease where Expiration < ?", t0.UnixMilli())
	if err != nil {
		return fmt.Errorf("delete statement failed: %w", err)
	}
	numDeleted, _ := res.RowsAffected()

	gw.LogC(ctx, "lease_entity", gw.LogDebug).
		Str("operation", "delete_all_expired").
		Dur("task_dt", time.Since(t0)).
		Msgf("deleted %v leases", numDeleted)

	return nil
}

func DeleteAllLeasesByRepositoryAndPathPrefix(ctx context.Context, tx *sql.Tx, repo, path string) error {
	t0 := time.Now()

	res, err := tx.ExecContext(ctx, "delete from Lease where Repository = ? and Path like ? || '%'", repo, path)
	if err != nil {
		return fmt.Errorf("delete statement failed: %w", err)
	}
	numDeleted, _ := res.RowsAffected()

	gw.LogC(ctx, "lease_entity", gw.LogDebug).
		Str("operation", "delete_all_by_repository_and_path_prefix").
		Dur("task_dt", time.Since(t0)).
		Msgf("deleted %v leases", numDeleted)

	return nil
}

func DeleteAllLeasesByRepository(ctx context.Context, tx *sql.Tx, repo string) error {
	t0 := time.Now()

	res, err := tx.ExecContext(ctx, "delete from Lease where Repository = ?", repo)
	if err != nil {
		return fmt.Errorf("delete statement failed: %w", err)
	}
	numDeleted, _ := res.RowsAffected()

	gw.LogC(ctx, "lease_entity", gw.LogDebug).
		Str("operation", "delete_all_by_repository").
		Dur("task_dt", time.Since(t0)).
		Msgf("deleted %v leases", numDeleted)

	return nil
}

func DeleteLeaseByToken(ctx context.Context, tx *sql.Tx, token string) error {
	t0 := time.Now()

	res, err := tx.ExecContext(ctx, "delete from Lease where Token = ?", token)
	if err != nil {
		return fmt.Errorf("delete statement failed: %w", err)
	}
	numDeleted, _ := res.RowsAffected()

	gw.LogC(ctx, "lease_entity", gw.LogDebug).
		Str("operation", "delete_by_token").
		Dur("task_dt", time.Since(t0)).
		Msgf("deleted %v leases", numDeleted)

	return nil
}

func scanLease(rows *sql.Rows, lease *Lease) error {
	var expMilli int64
	if err := rows.Scan(
		&lease.Token,
		&lease.Repository,
		&lease.Path,
		&lease.KeyID,
		&expMilli,
		&lease.ProtocolVersion,
		&lease.Hostname); err != nil {
		return err
	}

	lease.Expiration = time.UnixMilli(expMilli)

	return nil
}
