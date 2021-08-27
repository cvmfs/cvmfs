package lib

import (
	"io"
)

type TestReadCloser struct {
	io.Reader
}

func (r TestReadCloser) Close() error {
	return nil
}
