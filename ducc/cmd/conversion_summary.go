package cmd

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cvmfs/ducc/lib"
	l "github.com/cvmfs/ducc/log"
)

func logConversionSummary(prefix string, summary lib.ConversionSummary) {
	l.Log().Info(formatConversionSummary(prefix, summary))
}

func formatConversionSummary(prefix string, summary lib.ConversionSummary) string {
	return fmt.Sprintf("%s\n  %s\n  %s\n  %s",
		prefix,
		formatConversionSummaryLine("Added", summary.Added),
		formatConversionSummaryLine("Updated", summary.Updated),
		formatConversionSummaryLine("AlreadyConverted", summary.AlreadyConverted),
	)
}

func formatConversionSummaryLine(label string, images []string) string {
	if len(images) == 0 {
		return fmt.Sprintf("%s (0): none", label)
	}
	sortedImages := append([]string(nil), images...)
	sort.Strings(sortedImages)
	return fmt.Sprintf("%s (%d): %s", label, len(sortedImages), strings.Join(sortedImages, ", "))
}
