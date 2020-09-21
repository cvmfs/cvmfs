package cvmfs

import (
	"fmt"
	"os"
	"path/filepath"

	"testing"
)

func TestCreateACvmfsRepository(t *testing.T) {
	t.Parallel()
	repo := NewRepository("test1.ch")
	err := repo.MkFs()
	if err != nil {
		t.Errorf("Error in creating a new cvmfs FS: %s", err)
		return
	}
	defer repo.RmFs()
	_, err = os.Stat(repo.Root())
	if err != nil {
		t.Errorf("the new filesystem does not seems to be there: %s", err)
	}
}

func TestRemoveACvmfsRepository(t *testing.T) {
	t.Parallel()
	repo := NewRepository("test2.ch")
	err := repo.MkFs()
	if err != nil {
		t.Errorf("Error in creating a new cvmfs FS: %s", err)
		return
	}
	repo.RmFs()
	_, err = os.Stat(repo.Root())
	if err == nil {
		t.Errorf("unable to remove the filesystem")
	}
}

func TestTransactionsAgainstACvmfs(t *testing.T) {
	t.Parallel()
	repo := NewRepository("test3.ch")
	err := repo.MkFs()
	if err != nil {
		t.Errorf("Error in creating a new cvmfs FS: %s", err)
		return
	}
	defer repo.RmFs()

	err = repo.Transaction()
	if err != nil {
		t.Errorf("Error in opening a new transaction: %s", err)
	}

	f, err := os.Create(filepath.Join(repo.Root(), "new_file"))
	if err != nil {
		t.Errorf("Error in creating a new file: %s", err)
	}
	f.Close()

	_, err = os.Stat(f.Name())
	if err != nil {
		t.Errorf("Error in stating the new file during the transaction: %s", err)
	}

	err = repo.Publish()
	if err != nil {
		t.Errorf("Error in committing the transaction: %s", err)
	}

	_, err = os.Stat(f.Name())
	if err != nil {
		t.Errorf("Error in stating the new file after the transaction: %s", err)
	}
}

func TestAbortAgainstACvmfs(t *testing.T) {
	t.Parallel()
	repo := NewRepository("test4.ch")
	err := repo.MkFs()
	if err != nil {
		t.Errorf("Error in creating a new cvmfs FS: %s", err)
		return
	}
	defer repo.RmFs()

	err = repo.Transaction()
	if err != nil {
		t.Errorf("Error in opening a new transaction: %s", err)
	}

	f, err := os.Create(filepath.Join(repo.Root(), "new_file"))
	if err != nil {
		t.Errorf("Error in creating a new file: %s", err)
	}
	f.Close()

	_, err = os.Stat(f.Name())
	if err != nil {
		t.Errorf("Error in stating the new file during the transaction: %s", err)
	}

	err = repo.Abort()
	if err != nil {
		t.Errorf("Error in committing the transaction: %s", err)
	}

	_, err = os.Stat(f.Name())
	if err == nil {
		t.Errorf("Error the file appeared in the repository even if we aborted the transaction")
	}
}

func TestOperations(t *testing.T) {
	t.Parallel()
	repo := NewRepository("test5.ch")
	t.Log("Making repo...")
	err := repo.MkFs()
	if err != nil {
		t.Errorf("Error in creating a new cvmfs FS: %s", err)
		return
	}
	t.Log("Done creating repo...")

	defer repo.RmFs()

	paths := []string{
		filepath.Join(repo.Root(), "a"),
		filepath.Join(repo.Root(), "b", "1"),
		filepath.Join(repo.Root(), "c", "1", "2"),
		filepath.Join(repo.Root(), "d", "3", "2", "1"),
		filepath.Join(repo.Root(), "e", "4", "3", "2", "1"),
		filepath.Join(repo.Root(), "f", "5", "4", "3", "2", "1"),
	}

	waitFor := uint64(0)
	for _, path := range paths {
		waitFor, _ = repo.AddFSOperations(CreateDirectory{path})
		fmt.Println(waitFor)
	}

	if waitFor != 6 {
		t.Errorf("Expected index to wait equal to %d found equal to %d", 6, waitFor)
	}

	go repo.Execs()

	for {
		done := repo.DoneIndex()
		fmt.Println(done)
		if done >= waitFor {
			break
		}
	}

	for _, path := range paths {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("Expected directory but directory not found: %s", path)
		}
	}
}
