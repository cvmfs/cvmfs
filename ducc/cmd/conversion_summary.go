package cmd

import (
	"sort"

	log "github.com/sirupsen/logrus"

	"github.com/cvmfs/ducc/lib"
	l "github.com/cvmfs/ducc/log"
)

func sortedImageList(images []string) []string {
	sorted := append([]string(nil), images...)
	sort.Strings(sorted)
	return sorted
}

func logConversionSummary(message string, summary lib.ConversionSummary) {
	l.Log().WithFields(log.Fields{
		"added":             sortedImageList(summary.Added),
		"updated":           sortedImageList(summary.Updated),
		"already converted": sortedImageList(summary.AlreadyConverted),
	}).Info(message)
}
