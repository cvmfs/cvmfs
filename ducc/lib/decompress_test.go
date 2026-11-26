package lib

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
)

type closeTracker struct {
	io.Reader
	closed bool
}

func (c *closeTracker) Close() error {
	c.closed = true
	return nil
}

func TestDecompressLayer(t *testing.T) {
	payload := bytes.Repeat([]byte("layer content "), 1000)

	var gz bytes.Buffer
	gw := gzip.NewWriter(&gz)
	gw.Write(payload)
	gw.Close()

	var zs bytes.Buffer
	zw, err := zstd.NewWriter(&zs)
	if err != nil {
		t.Fatal(err)
	}
	zw.Write(payload)
	zw.Close()

	cases := map[string][]byte{
		"gzip":  gz.Bytes(),
		"zstd":  zs.Bytes(),
		"plain": payload,
	}
	for name, blob := range cases {
		t.Run(name, func(t *testing.T) {
			src := &closeTracker{Reader: bytes.NewReader(blob)}
			r, err := decompressLayer(src)
			if err != nil {
				t.Fatalf("decompressLayer: %v", err)
			}
			got, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("got %d bytes, want %d", len(got), len(payload))
			}
			if err := r.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			if !src.closed {
				t.Fatal("underlying blob stream not closed")
			}
		})
	}
}

func TestDecompressLayerShortBlob(t *testing.T) {
	r, err := decompressLayer(io.NopCloser(bytes.NewReader([]byte{0x1})))
	if err != nil {
		t.Fatalf("decompressLayer: %v", err)
	}
	got, _ := io.ReadAll(r)
	if !bytes.Equal(got, []byte{0x1}) {
		t.Fatalf("got %v", got)
	}
}
