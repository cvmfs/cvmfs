package cvmfs

import (
	"path/filepath"
	"strings"
)

// GetRepoAndSubdir splits a CVMFSRepo specification into repository name and subdirectory.
//
// In production, repository paths look like "repo.cern.ch/subdir" where the first
// component is the repository name and the rest is the subdirectory path.
//
// For testing with mock repositories (relative paths like "../../tmp/mockrepo"),
// use a colon separator: "../../tmp/mockrepo:subdir" to avoid ambiguity with
// the slashes in the relative path itself.
//
// Examples:
//   - "unpacked.cern.ch"           → ("unpacked.cern.ch", "")
//   - "unpacked.cern.ch/images"    → ("unpacked.cern.ch", "images")
//   - "../../tmp/mock:subdir"      → ("../../tmp/mock", "subdir")
func GetRepoAndSubdir(cvmfsRepo string) (repoName, subDir string) {
	if strings.Contains(cvmfsRepo, ":") {
		repoName, subDir, _ = strings.Cut(cvmfsRepo, ":")
		subDir = strings.TrimSuffix(subDir, "/")
		return
	}
	if strings.HasPrefix(cvmfsRepo, "..") || strings.HasPrefix(cvmfsRepo, "/..") {
		return cvmfsRepo, ""
	}
	repoName, subDir, _ = strings.Cut(cvmfsRepo, "/")
	subDir = strings.TrimSuffix(subDir, "/")
	return
}

// PrefixRepoSubdirOnce ensures repoPath is relative and prefixed with the repo
// subdirectory exactly once.
func PrefixRepoSubdirOnce(cvmfsRepo, repoPath string) string {
	_, subDir := GetRepoAndSubdir(cvmfsRepo)

	relativePath := strings.TrimLeft(repoPath, "/")
	relativePath = filepath.Clean(relativePath)
	if relativePath == "." {
		relativePath = ""
	}

	if subDir == "" {
		return relativePath
	}

	separator := string(filepath.Separator)
	if relativePath == subDir || strings.HasPrefix(relativePath, subDir+separator) {
		return relativePath
	}
	if relativePath == "" {
		return subDir
	}
	return filepath.Join(subDir, relativePath)
}
