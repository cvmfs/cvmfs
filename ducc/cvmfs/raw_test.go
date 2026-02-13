package cvmfs

import "testing"

func TestLockRepoKey(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		expected string
	}{
		{"plain_repo", "repo.cern.ch", "repo.cern.ch"},
		{"repo_with_subdir_slash", "repo.cern.ch/ducc/subdir", "repo.cern.ch"},
		{"repo_with_subdir_colon", "repo.cern.ch:ducc/subdir", "repo.cern.ch"},
		{"mock_repo_with_subdir", "../../tmp/mockrepo:target_subdir", "../../tmp/mockrepo"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := lockRepoKey(tc.input)
			if got != tc.expected {
				t.Fatalf("lockRepoKey(%q)=%q, expected %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestTransactionRepoName(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		expected string
	}{
		{"plain_repo", "repo.cern.ch", "repo.cern.ch"},
		{"repo_with_subdir_slash", "repo.cern.ch/ducc/subdir", "repo.cern.ch"},
		{"repo_with_subdir_colon", "repo.cern.ch:ducc/subdir", "repo.cern.ch"},
		{"mock_repo_with_subdir", "../../tmp/mockrepo:target_subdir", "../../tmp/mockrepo"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := transactionRepoName(tc.input)
			if got != tc.expected {
				t.Fatalf("transactionRepoName(%q)=%q, expected %q", tc.input, got, tc.expected)
			}
		})
	}
}
