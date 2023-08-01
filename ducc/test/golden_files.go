package test

import (
	"io"
	"os"
	"testing"
)

// CompareWithGoldenFile is used when testing for comparing a string to a golden file.
// This type of testing is useful for avoiding accidental changes to function output, for example
// the output of a REST API.
func CompareWithGoldenFile(t *testing.T, goldenFilePath string, value string, update bool) {
	if !update {
		_, err := os.Stat(goldenFilePath)
		if os.IsNotExist(err) {
			t.Fatalf("Golden file \"%s\" does not exist. Run with -update to create it.", goldenFilePath)
		}
	}

	f, err := os.OpenFile(goldenFilePath, os.O_RDWR+os.O_CREATE, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if update {
		_, err := f.WriteString(value)
		if err != nil {
			t.Fatal(err)
		}
		return
	}

	content, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}

	if string(content) != value {
		t.Fatalf("Golden file \"%s\" does not match. Expected \"%s\", got \"%s\"", goldenFilePath, value, string(content))
	}

}
