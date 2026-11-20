package exec

import (
	"strings"
	"testing"
)

func TestOutputTail(t *testing.T) {
	stderr := []byte("Info: transaction on repository r\n.....\n" +
		"/nv1 does not exist and the gateway cannot create missing parent directories\n" +
		"Synchronization failed\n\nExecuted Command:\ncvmfs_swissknife ingest -u /cvmfs/r\n")
	got := outputTail(stderr, []byte("ignored"))
	if !strings.Contains(got, "/nv1 does not exist") || !strings.HasSuffix(got, "Synchronization failed") ||
		strings.Contains(got, "swissknife") || strings.Contains(got, "ignored") {
		t.Errorf("tail %q", got)
	}
	if got := outputTail(nil, []byte("only stdout\n")); got != "only stdout" {
		t.Errorf("stdout fallback %q", got)
	}
	if got := outputTail([]byte(strings.Repeat("x", 5000))); len(got) > 1100 {
		t.Errorf("tail not bounded: %d bytes", len(got))
	}
	err := ExecCommand("sh", "-c", "echo boom >&2; exit 3").Start()
	if err == nil || !strings.Contains(err.Error(), "exit status 3: boom") {
		t.Errorf("error %v", err)
	}
}
