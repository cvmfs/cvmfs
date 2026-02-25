package lib

import (
	"strings"
	"testing"
)

func TestConstructDeleteCommands(t *testing.T) {
	paths := []string{"a/b"}
	commands, _ := ConstructDeleteCommands(paths, 10, "test.cern.ch")
	if len(commands) != 1 {
		t.Errorf("Delete command missing")
	}
	if strings.Join(commands[0], " ") != "cvmfs_server ingest --delete a/b test.cern.ch" {
		t.Errorf("Wrong delete command")
	}

	paths = []string{"a/b", "c/d"}
	commands, _ = ConstructDeleteCommands(paths, 10, "test.cern.ch")
	if strings.Join(commands[0], " ") != "cvmfs_server ingest --delete a/b --delete c/d test.cern.ch" {
		t.Errorf("Wrong delete command")
	}

	paths = []string{"a/b", "c/d"}
	commands, _ = ConstructDeleteCommands(paths, 1, "test.cern.ch")
	if strings.Join(commands[0], " ") != "cvmfs_server ingest --delete a/b test.cern.ch" {
		t.Errorf("Wrong delete command")
	}
	if strings.Join(commands[1], " ") != "cvmfs_server ingest --delete c/d test.cern.ch" {
		t.Errorf("Wrong delete command")
	}
}

func TestConstructDeleteCommandsSubdir(t *testing.T) {
	paths := []string{".layers/aa", "compat/.layers/bb"}
	commands, _ := ConstructDeleteCommands(paths, 10, "repo.cern.ch/compat")
	if len(commands) != 1 {
		t.Errorf("Delete command missing")
	}
	if strings.Join(commands[0], " ") != "cvmfs_server ingest --delete compat/.layers/aa --delete compat/.layers/bb repo.cern.ch" {
		t.Errorf("Wrong delete command for subdir repo")
	}
}

func TestConstructDeleteCommandsSubdirMockRepoFormat(t *testing.T) {
	paths := []string{".layers/aa", "target_subdir/.layers/bb"}
	commands, _ := ConstructDeleteCommands(paths, 10, "../../tmp/mockrepo///target_subdir")
	if len(commands) != 1 {
		t.Errorf("Delete command missing")
	}
	if strings.Join(commands[0], " ") != "cvmfs_server ingest --delete target_subdir/.layers/aa --delete target_subdir/.layers/bb ../../tmp/mockrepo" {
		t.Errorf("Wrong delete command for mock subdir repo")
	}
}
