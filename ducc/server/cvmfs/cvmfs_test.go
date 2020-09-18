package cvmfs

import (
	"errors"
	"io/ioutil"
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
	err := repo.MkFs()
	if err != nil {
		t.Errorf("Error in creating a new cvmfs FS: %s", err)
		return
	}

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
		waitFor, _ = repo.AddFSOperations(NewCreateDirectory(path))
	}

	if waitFor != 6 {
		t.Errorf("Expected index to wait equal to %d found equal to %d", 6, waitFor)
	}

	go repo.StartOperationsLoop()

	err = repo.WaitFor(waitFor)
	if err != nil {
		t.Errorf("Internal inconsistency in the wait for")
	}

	for _, path := range paths {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("Expected directory but directory not found: %s", path)
		}
	}
}

func TestWaitForErrors(t *testing.T) {
	t.Parallel()
	repo := NewRepository("virtual.test.ch")
	repo.opsIndex = uint64(20)
	repo.doneIndex = uint64(20)

	err := repo.WaitFor(uint64(10))
	if !errors.Is(err, WaitForExpiredError) {
		t.Errorf("returned from type of error")
	}
	err = repo.WaitFor(uint64(30))
	if !errors.Is(err, WaitForNotScheduledError) {
		t.Errorf("returned wrong typ of error")
	}
}

func TestOperationsWithError(t *testing.T) {
	t.Parallel()
	repo := NewRepository("test6.ch")
	err := repo.MkFs()
	if err != nil {
		t.Errorf("Error in creating a new cvmfs FS: %s", err)
		return
	}

	defer repo.RmFs()

	f, _ := ioutil.TempFile("", "testgocvmfsunreadablefile*")
	defer os.Remove(f.Name())
	ioutil.WriteFile(f.Name(), []byte("foooo"), 0200)
	os.Chmod(f.Name(), 0200)
	cp, err := NewCopyFile(f.Name(), filepath.Join(repo.Root(), "a", "b", ".cvmfscatalog"))

	if err != nil {
		t.Errorf("trying to copy a file that does not exists")
	}
	f2, _ := ioutil.TempFile("", "testgocvmfs*")
	defer os.Remove(f2.Name())
	cp2, err := NewCopyFile(f2.Name(), filepath.Join(repo.Root(), "1", "2", ".cvmfscatalog"))

	repo.AddFSOperations(cp)
	repo.AddFSOperations(cp2)

	go repo.StartOperationsLoop()

	ch := cp.ErrorsChannel()

	err1 := <-ch
	if err1 == nil {
		t.Errorf("Expected error, file should not be readable")
	}
	err2 := <-ch
	if err2 != nil {
		t.Errorf("The transaction should conclude normally")
	}
	// we are checking that this channel is closed
	_, ok := <-ch
	if ok {
		t.Errorf("The channel was not closed")
	}

	ch2 := cp2.ErrorsChannel()
	if <-ch2 != nil {
		t.Errorf("Unexpected error in creating file")
	}
	if <-ch2 != nil {
		t.Errorf("Unexpected error in transaction")
	}
	if _, ok = <-ch2; ok {
		t.Errorf("Channel not closed")
	}

	if _, err = os.Stat(filepath.Join(repo.Root(), "a", "b", ".cvmfscatalog")); err == nil {
		t.Errorf("Unexpected file created")
	}
	if _, err = os.Stat(filepath.Join(repo.Root(), "1", "2", ".cvmfscatalog")); err != nil {
		t.Errorf("File should be there")
	}

}
