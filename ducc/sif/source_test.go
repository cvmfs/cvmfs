//go:build linux

package sif

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cvmfs/ducc/sif/squashfs"
)

func streamFrom(t *testing.T, src Source, workers int) ([]byte, error) {
	t.Helper()
	im, err := Parse(src, src.Size())
	if err != nil {
		return nil, err
	}
	fs, err := squashfs.Open(im.Partition(src), im.PartSize)
	if err != nil {
		return nil, err
	}
	ix, err := fs.BuildIndex(1 << 20)
	if err != nil {
		return nil, err
	}
	fs.DropMetadataCache()
	opts := DefaultStreamOptions()
	opts.Workers = workers
	var buf bytes.Buffer
	_, err = Stream(context.Background(), &buf, fs, ix, opts)
	return buf.Bytes(), err
}

// bigImage builds a SIF with ~20 MB of poorly compressible data spread over
// many files of mixed sizes, so fragments and full blocks interleave.
func bigImage(t *testing.T) string {
	t.Helper()
	requireMksquashfs(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	writeFile(t, filepath.Join(src, ".singularity.d/runscript"), []byte("#!/bin/sh\n"), 0755)
	sizes := []int{100, 3000, 70 * 1024, 200 * 1024, 1500 * 1024}
	for i := 0; i < 120; i++ {
		data := pattern(sizes[i%len(sizes)], byte(i))
		for k := range data {
			data[k] ^= byte(k * i) // defeat the "every 7th byte" compressibility
		}
		writeFile(t, filepath.Join(src, fmt.Sprintf("d%d/f%d", i%9, i)), data, 0644)
	}
	img := filepath.Join(dir, "img.sqfs")
	mksquashfs(t, src, img)
	sifPath := filepath.Join(dir, "img.sif")
	writeSIF(t, img, sifPath, FsSquash, PartPrimSys)
	return sifPath
}

func TestHTTPSourceMatchesFile(t *testing.T) {
	path := bigImage(t)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	mod := time.Unix(1700000000, 0)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"v1"`)
		http.ServeContent(w, r, "img.sif", mod, bytes.NewReader(data))
	}))
	defer srv.Close()

	fsrc, err := OpenSource(context.Background(), path, DefaultSourceOptions())
	if err != nil {
		t.Fatal(err)
	}
	want, err := streamFrom(t, fsrc, 4)
	if err != nil {
		t.Fatal(err)
	}

	opts := DefaultSourceOptions()
	opts.ChunkSize = 1 << 20
	opts.CacheSize = 16 << 20
	hsrc, err := OpenSource(context.Background(), srv.URL+"/img.sif", opts)
	if err != nil {
		t.Fatal(err)
	}
	got, err := streamFrom(t, hsrc, 4)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(want, got) {
		t.Fatalf("tar over HTTP differs from tar from file (%d vs %d bytes)", len(got), len(want))
	}
	st := hsrc.Stats()
	ratio := float64(st.BytesFetched) / float64(len(data))
	t.Logf("fetched %d of %d bytes (%.3f), %d requests, %d hits", st.BytesFetched, len(data), ratio, st.Requests, st.CacheHits)
	if ratio > 1.1 {
		t.Errorf("fetch ratio %.3f > 1.1", ratio)
	}
	if !strings.Contains(hsrc.Identity(), `"v1"`) {
		t.Errorf("identity %q lacks etag", hsrc.Identity())
	}
}

func TestHTTPSourceDetectsChange(t *testing.T) {
	path := bigImage(t)
	data, _ := os.ReadFile(path)
	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		etag := `"v1"`
		if atomic.AddInt32(&requests, 1) > 3 {
			etag = `"v2"`
		}
		w.Header().Set("ETag", etag)
		http.ServeContent(w, r, "img.sif", time.Time{}, bytes.NewReader(data))
	}))
	defer srv.Close()
	opts := DefaultSourceOptions()
	opts.ChunkSize = 1 << 20
	opts.Readahead = 0
	src, err := OpenSource(context.Background(), srv.URL, opts)
	if err != nil {
		t.Fatal(err)
	}
	_, err = streamFrom(t, src, 2)
	if err == nil || !strings.Contains(err.Error(), "source changed") {
		t.Fatalf("want source-changed error, got %v", err)
	}
}

func TestHTTPSourceWeakToStrongETag(t *testing.T) {
	data := bytes.Repeat([]byte("abcdefgh"), 200000)
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		etag := `"v1"`
		if atomic.AddInt32(&n, 1) == 1 {
			etag = `W/"v1"`
		}
		w.Header().Set("ETag", etag)
		http.ServeContent(w, r, "x", time.Unix(1, 0), bytes.NewReader(data))
	}))
	defer srv.Close()
	opts := DefaultSourceOptions()
	opts.ChunkSize = 64 << 10
	src, err := OpenSource(context.Background(), srv.URL, opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(io.NewSectionReader(src, 0, src.Size())); err != nil {
		t.Fatalf("weak-to-strong ETag treated as change: %v", err)
	}
}

func TestHTTPSourceRequiresRanges(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("no ranges here"))
	}))
	defer srv.Close()
	_, err := OpenSource(context.Background(), srv.URL, DefaultSourceOptions())
	if err == nil || !strings.Contains(err.Error(), "range") {
		t.Fatalf("want range error, got %v", err)
	}
}

func TestHTTPSourceRetries(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789"), 100000)
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&n, 1)%3 == 2 {
			http.Error(w, "flaky", http.StatusServiceUnavailable)
			return
		}
		http.ServeContent(w, r, "x", time.Unix(1, 0), bytes.NewReader(data))
	}))
	defer srv.Close()
	opts := DefaultSourceOptions()
	opts.ChunkSize = 64 << 10
	opts.Readahead = 0
	src, err := OpenSource(context.Background(), srv.URL, opts)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(io.NewSectionReader(src, 0, src.Size()))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("content mismatch")
	}
	if src.Stats().Retries == 0 {
		t.Error("expected retries")
	}
}

func TestStdinRejected(t *testing.T) {
	if _, err := OpenSource(context.Background(), "-", DefaultSourceOptions()); err == nil {
		t.Fatal("stdin accepted")
	}
}
