package backend

import (
	"context"
	"os/exec"
	"strconv"
	"time"
)

// GCOptions represents the different options supplied for a garbace collection run
type GCOptions struct {
	Repository   string    `json:"repo"`
	NumRevisions int       `json:"num_revisions"`
	Timestamp    time.Time `json:"timestamp"`
	DryRun       bool      `json:"dry_run"`
	Verbose      bool      `json:"verbose"`
}

// RunGC triggers garbage collection on the specified repository
func (s *Services) RunGC(ctx context.Context, options GCOptions) (string, error) {
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "garbage_collection", &outcome, t0)

	baseArgs := []string{"gc", "-f"}
	if options.NumRevisions != 0 {
		baseArgs = append(baseArgs, "-r", strconv.Itoa(options.NumRevisions))
	}
	if !options.Timestamp.IsZero() {
		baseArgs = append(baseArgs, "-t", options.Timestamp.String())
	}
	if options.DryRun {
		baseArgs = append(baseArgs, "-d")
	}
	if options.Verbose {
		baseArgs = append(baseArgs, "-l")
	}

	var output string
	args := append(baseArgs, options.Repository)
	if err := s.Leases.WithLock(ctx, options.Repository, func() error {
		cmd := exec.Command("cvmfs_server", args...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			return err
		}
		output = string(out)
		return nil
	}); err != nil {
		outcome = err.Error()
		return "", err
	}

	return output, nil
}
