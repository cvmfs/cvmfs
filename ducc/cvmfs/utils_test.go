package cvmfs

import "testing"

func TestGetRepoAndSubdir(t *testing.T) {
	tests := []struct {
		input        string
		wantRepoName string
		wantSubDir   string
	}{
		// Basic cases
		{"repo.cern.ch", "repo.cern.ch", ""},
		{"repo.cern.ch/subdir", "repo.cern.ch", "subdir"},
		{"repo.cern.ch:subdir", "repo.cern.ch", "subdir"},
		{"repo.cern.ch/nested/subdir", "repo.cern.ch", "nested/subdir"},
		{"repo.cern.ch:nested/subdir", "repo.cern.ch", "nested/subdir"},
		// Relative paths (for testing)
		{"../../tmp/mockrepo", "../../tmp/mockrepo", ""},
		{"../../tmp/mockrepo:subdir", "../../tmp/mockrepo", "subdir"},
		{"/../../tmp/mockrepo", "/../../tmp/mockrepo", ""},
		// Edge cases
		{"", "", ""},                                       // empty input
		{"repo.cern.ch/", "repo.cern.ch", ""},              // trailing slash on repo
		{"repo.cern.ch/subdir/", "repo.cern.ch", "subdir"}, // trailing slash in subdir
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			gotRepoName, gotSubDir := GetRepoAndSubdir(tt.input)
			if gotRepoName != tt.wantRepoName {
				t.Errorf("GetRepoAndSubdir() gotRepoName = %v, want %v", gotRepoName, tt.wantRepoName)
			}
			if gotSubDir != tt.wantSubDir {
				t.Errorf("GetRepoAndSubdir() gotSubDir = %v, want %v", gotSubDir, tt.wantSubDir)
			}
		})
	}
}
