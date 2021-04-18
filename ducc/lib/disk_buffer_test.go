package lib

import (
	"bytes"
	"crypto/rand"
	"io/ioutil"
	"testing"
)

func TestOnDiskBufferSize(t *testing.T) {
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
	onDisk, err := NewDiskBuffer(rc)
	if err != nil {
		t.Fatal("Error in creating the OnDisk structure")
	}
	defer onDisk.Close()
	b, err := ioutil.ReadAll(onDisk)
	if err != nil {
		t.Fatal("Error in reading all the OnDiskReadAndHash")
	}
	if len(b) != n {
		t.Fatal("Wrong read size")
	}
}
