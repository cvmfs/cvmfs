package lib

import (
	"bytes"
	"crypto/rand"
	"io/ioutil"
	"os"
	"testing"
)

func TestOnDiskReadAndHashSize(t *testing.T) {
	array := make([]byte, 1024*1024)
	n, err := rand.Read(array)
	if err != nil {
		t.Fatal("Error in creating random bytes array")
	}
	if n != 1024*1024 {
		t.Fatal("n not the size of the array")
	}
	r := bytes.NewReader(array)
	rc := TestReadCloser{r}
	onDisk, err := NewOnDiskReadAndHash(rc)
	if err != nil {
		t.Fatal("Error in creating the OnDisk structure")
	}
	defer onDisk.Close()
	path := onDisk.path
	stat, err := os.Stat(path)
	if err != nil {
		t.Fatal("Error in stating the file from the OnDiskReadAndHash")
	}
	if stat.Size() != int64(n) {
		t.Fatal("File of the wrong dimension")
	}
	b, err := ioutil.ReadAll(onDisk)
	if err != nil {
		t.Fatal("Error in reading all the OnDiskReadAndHash")
	}
	if len(b) != n {
		t.Fatal("Wrong read size")
	}
	if onDisk.GetSize() != 1024*1024 {
		t.Fatal("Wrong result from GetSize")
	}
}
