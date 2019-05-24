package backend

import (
	"context"
	"time"
)

/*
gc              [-r number of revisions to preserve]
[-t time stamp after which revisions are preserved]
[-l print deleted objects] [-L log of deleted objects]
[-f force] [-d dry run]
[-a collect all garbage-collectable repos, log to gc.log |
  <fully qualified name> ]
*/

// GCOptions represents the different options supplied for a garbace collection run
type GCOptions struct {
	Repository   string    `json:"repo"`
	NumRevisions int       `json:"num_revisions"`
	Timestamp    time.Time `json:"timestamp"`
	Force        bool      `json:"force"`
	DryRun       bool      `json:"dry_run"`
	All          bool      `json:"all"`
	// Verbose is similar to the "-l" parameter, return a list of deleted objects to the caller
	// Verbose bool `json:"verbose"`
}

// RunGC triggers garbage collection on the specified repository
func (s *Services) RunGC(ctx context.Context, options GCOptions) error {
	return nil
}
