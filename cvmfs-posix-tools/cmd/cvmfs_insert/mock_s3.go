package main

import (
	"io"

	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
)

type MockS3Manager struct {
}

var newMockBasicS3Manager = func(ctx Context) (S3Interface, error) {
	return &MockS3Manager{}, nil
}

func (s3Manager *MockS3Manager) uploadGivenInfo(reader io.ReadSeeker, partSize int64, retries int, s3Dest string, fileToUpload *pkg.UploadFile, overwriteExisting bool) error {
	return nil
}

func (s3Manager *MockS3Manager) upload(uploadFile pkg.UploadFile, s3Dest string) (err error) {
	return nil
}

func (S3Manager *MockS3Manager) uploadReaderList(fileName string, readers []pkg.NamedReader, partSize int64, maxConcurrentUploaders int, overwriteExisting bool, fileToUpload *pkg.UploadFile, compressor *pkg.Compressor) error {
	return nil
}

func (s3Manager *MockS3Manager) checkObjectExists(s3Dest string, retries int) (bool, error) {
	return true, nil
}

func (s3Manager *MockS3Manager) checkObjectExistsName(s3Dest string, retries int) (bool, error) {
	return true, nil
}

func (s3Manager *MockS3Manager) purge(s3Dest string) error {
	return nil
}
