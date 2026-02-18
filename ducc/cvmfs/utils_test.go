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
		{"/tmp/mockrepo", "/tmp/mockrepo", ""},
		{"/tmp/mockrepo/subdir", "/tmp/mockrepo/subdir", ""},
		{"/cvmfs/repo.cern.ch/subdir", "repo.cern.ch", "subdir"},
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

func TestGetRepoAndSubdirAbsolutePathKeepsRepoName(t *testing.T) {
	repo, subdir := GetRepoAndSubdir("/tmp/mockrepo")
	if repo == "" {
		t.Fatal("GetRepoAndSubdir returned an empty repository name for an absolute path")
	}
	if subdir != "" {
		t.Fatalf("GetRepoAndSubdir returned unexpected subdir %q for an absolute path", subdir)
	}
}

func TestPrefixRepoSubdirOnce(t *testing.T) {
	tests := []struct {
		name     string
		repo     string
		path     string
		expected string
	}{
		{
			name:     "root_repo_path_unchanged",
			repo:     "repo.cern.ch",
			path:     ".chains/ab/test",
			expected: ".chains/ab/test",
		},
		{
			name:     "subdir_repo_prefix_added",
			repo:     "repo.cern.ch/compat",
			path:     ".chains/ab/test",
			expected: "compat/.chains/ab/test",
		},
		{
			name:     "subdir_repo_prefix_not_duplicated",
			repo:     "repo.cern.ch/compat",
			path:     "compat/.chains/ab/test",
			expected: "compat/.chains/ab/test",
		},
		{
			name:     "absolute_path_is_normalized",
			repo:     "repo.cern.ch/compat",
			path:     "/compat/.chains/ab/test",
			expected: "compat/.chains/ab/test",
		},
		{
			name:     "similar_prefix_is_not_treated_as_equal",
			repo:     "repo.cern.ch/compat",
			path:     "compat2/.chains/ab/test",
			expected: "compat/compat2/.chains/ab/test",
		},
		{
			name:     "empty_path_points_to_subdir_root",
			repo:     "repo.cern.ch/compat",
			path:     "",
			expected: "compat",
		},
		{
			name:     "mock_repo_colon_separator",
			repo:     "../../tmp/mockrepo:target_subdir",
			path:     ".layers/aa",
			expected: "target_subdir/.layers/aa",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PrefixRepoSubdirOnce(tt.repo, tt.path)
			if got != tt.expected {
				t.Errorf("PrefixRepoSubdirOnce() got = %v, want %v", got, tt.expected)
			}
		})
	}
}
