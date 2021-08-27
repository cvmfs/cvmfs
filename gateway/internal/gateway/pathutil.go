package gateway

import (
	"fmt"
	"path"
	"strings"
)

// CheckPathOverlap returns true if the two paths overlap
func CheckPathOverlap(path1, path2 string) bool {
	if path.IsAbs(path1) {
		path1 = strings.TrimPrefix(path1, "/")
	}
	if path.IsAbs(path2) {
		path2 = strings.TrimPrefix(path2, "/")
	}

	if len(path1) == 0 || len(path2) == 0 {
		return true
	}

	tokens1 := strings.Split(path1, "/")
	tokens2 := strings.Split(path2, "/")

	return tokensOverlap(tokens1, tokens2)
}

func tokensOverlap(t1, t2 []string) bool {
	var larger, smaller []string
	if len(t1) > len(t2) {
		larger = t1
		smaller = t2
	} else {
		larger = t2
		smaller = t1
	}
	res := true
	for i, tk := range smaller {
		if larger[i] != tk {
			res = false
			break
		}
	}
	return res
}

// SplitLeasePath takes as input a full lease path of the form
// "<REPO_NAME>/<SUBPATH>" and return the repo name and subpath
func SplitLeasePath(leasePath string) (string, string, error) {
	if strings.HasPrefix(leasePath, "/") {
		return "", "", fmt.Errorf("input has leading slash")
	}

	tokens := strings.Split(leasePath, "/")
	if len(tokens) < 2 {
		return "", "", fmt.Errorf("missing repository name or subpath")
	}
	repoName := tokens[0]

	if strings.Count(repoName, ".") < 2 {
		return "", "", fmt.Errorf("input does not start with a FQDN")
	}

	subPath := strings.TrimPrefix(leasePath, repoName)

	return repoName, subPath, nil
}
