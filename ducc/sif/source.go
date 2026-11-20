package sif

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

// Source is random-access, read-only image storage.
type Source interface {
	io.ReaderAt
	Size() int64
	// Identity identifies the version of the bytes (ETag, Last-Modified,
	// file mtime); used for change detection, not content addressing.
	Identity() string
	// KnownDigest is a sha256 of the whole image if the source provides one.
	KnownDigest() string
	Stats() SourceStats
	Close() error
}

type SourceStats struct {
	BytesFetched int64 `json:"bytes_fetched"`
	Requests     int64 `json:"requests"`
	CacheHits    int64 `json:"cache_hits"`
	Retries      int64 `json:"retries"`
}

type SourceOptions struct {
	ChunkSize int64
	CacheSize int64
	Readahead int
	Attempts  int
	Headers   http.Header
	Client    *http.Client
	Logger    *log.Entry
}

func DefaultSourceOptions() SourceOptions {
	return SourceOptions{ChunkSize: 8 << 20, CacheSize: 64 << 20, Readahead: 4, Attempts: 5}
}

// OpenSource dispatches on the reference: a path or file:// URL, or an
// http(s) URL served with range support.
func OpenSource(ctx context.Context, ref string, opts SourceOptions) (Source, error) {
	switch {
	case ref == "-":
		return nil, fmt.Errorf("reading a SIF from stdin is not supported: squashfs keeps its " +
			"metadata at the end of the image; serve it over HTTP with range support instead")
	case strings.HasPrefix(ref, "http://"), strings.HasPrefix(ref, "https://"):
		return openHTTP(ctx, ref, opts)
	case strings.HasPrefix(ref, "oras://"):
		return nil, fmt.Errorf("oras:// sources are not supported yet")
	case strings.HasPrefix(ref, "file://"):
		ref = strings.TrimPrefix(ref, "file://")
	case strings.Contains(ref, "://"):
		return nil, fmt.Errorf("unsupported source scheme in %q", ref)
	}
	return openFile(ref)
}

type fileSource struct {
	*os.File
	size  int64
	ident string
	read  int64
}

func openFile(path string) (Source, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		f.Close()
		return nil, fmt.Errorf("%s is not a regular file", path)
	}
	return &fileSource{File: f, size: fi.Size(),
		ident: fmt.Sprintf("file:%d:%d", fi.Size(), fi.ModTime().UnixNano())}, nil
}

func (s *fileSource) ReadAt(p []byte, off int64) (int, error) {
	n, err := s.File.ReadAt(p, off)
	atomic.AddInt64(&s.read, int64(n))
	return n, err
}
func (s *fileSource) Size() int64         { return s.size }
func (s *fileSource) Identity() string    { return s.ident }
func (s *fileSource) KnownDigest() string { return "" }
func (s *fileSource) Stats() SourceStats {
	return SourceStats{BytesFetched: atomic.LoadInt64(&s.read)}
}

// httpSource serves ReadAt from aligned chunks fetched with range requests,
// kept in an LRU, with readahead on sequential access.
type httpSource struct {
	ctx       context.Context
	url       string
	client    *http.Client
	headers   http.Header
	size      int64
	etag      string
	modified  string
	ident     string
	chunk     int64
	maxChunks int
	readahead int
	attempts  int
	logger    *log.Entry

	mu       sync.Mutex
	lru      *list.List
	chunks   map[int64]*list.Element
	inflight map[int64]*chunkFetch
	last     int64

	stats SourceStats
}

type chunkFetch struct {
	done chan struct{}
	data []byte
	err  error
}

type cachedChunk struct {
	idx  int64
	data []byte
}

var (
	errSourceChanged = errors.New("source changed during conversion")
	errNoRanges      = errors.New("server ignored the range request")
)

func openHTTP(ctx context.Context, url string, opts SourceOptions) (Source, error) {
	def := DefaultSourceOptions()
	if opts.ChunkSize <= 0 {
		opts.ChunkSize = def.ChunkSize
	}
	if opts.CacheSize < opts.ChunkSize {
		opts.CacheSize = opts.ChunkSize * 2
	}
	if opts.Attempts <= 0 {
		opts.Attempts = def.Attempts
	}
	if opts.Client == nil {
		opts.Client = &http.Client{Timeout: 5 * time.Minute}
	}
	if opts.Logger == nil {
		opts.Logger = log.NewEntry(log.StandardLogger())
	}
	s := &httpSource{
		ctx: ctx, url: url, client: opts.Client, headers: opts.Headers,
		chunk: opts.ChunkSize, maxChunks: int(opts.CacheSize / opts.ChunkSize),
		readahead: opts.Readahead, attempts: opts.Attempts, logger: opts.Logger,
		lru: list.New(), chunks: map[int64]*list.Element{}, inflight: map[int64]*chunkFetch{},
		last: -2,
	}
	if err := s.probe(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *httpSource) newRequest(method string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(s.ctx, method, s.url, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range s.headers {
		req.Header[k] = v
	}
	return req, nil
}

// probe learns size and validator. A range GET is used rather than HEAD
// because it also proves range support.
func (s *httpSource) probe() error {
	var resp *http.Response
	err := s.retry(func() error {
		req, err := s.newRequest(http.MethodGet)
		if err != nil {
			return err
		}
		req.Header.Set("Range", "bytes=0-0")
		resp, err = s.client.Do(req)
		if err != nil {
			return err
		}
		if retryable(resp.StatusCode) {
			resp.Body.Close()
			return fmt.Errorf("%s: HTTP %s", s.url, resp.Status)
		}
		return nil
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	switch resp.StatusCode {
	case http.StatusPartialContent:
	case http.StatusOK:
		return fmt.Errorf("%s: server does not support range requests", s.url)
	default:
		return fmt.Errorf("%s: HTTP %s", s.url, resp.Status)
	}
	cr := resp.Header.Get("Content-Range")
	i := strings.LastIndexByte(cr, '/')
	if i < 0 {
		return fmt.Errorf("%s: invalid Content-Range %q", s.url, cr)
	}
	size, err := strconv.ParseInt(cr[i+1:], 10, 64)
	if err != nil || size <= 0 {
		return fmt.Errorf("%s: unknown size in Content-Range %q", s.url, cr)
	}
	s.size = size
	// Validators are compared on every response rather than sent as
	// If-Range: If-Range needs a strong validator, and Apache treats weak
	// ETags and a Last-Modified within a second of Date as weak.
	s.etag = resp.Header.Get("ETag")
	s.modified = resp.Header.Get("Last-Modified")
	s.ident = fmt.Sprintf("http:%d:%s:%s", size, s.etag, s.modified)
	if s.etag == "" && s.modified == "" {
		s.logger.WithField("url", s.url).Warn("server sends no ETag or Last-Modified; changes during conversion cannot be detected")
	}
	return nil
}

// weakETag drops the weak marker: Apache sends W/"x" while a file is less
// than a second old and "x" afterwards, for the same content.
func weakETag(e string) string { return strings.TrimPrefix(e, "W/") }

func retryable(code int) bool {
	return code == http.StatusTooManyRequests || code >= 500
}

func (s *httpSource) retry(f func() error) error {
	var err error
	for attempt := 0; attempt < s.attempts; attempt++ {
		if attempt > 0 {
			atomic.AddInt64(&s.stats.Retries, 1)
			d := time.Duration(1<<uint(attempt-1)) * 500 * time.Millisecond
			select {
			case <-time.After(d):
			case <-s.ctx.Done():
				return s.ctx.Err()
			}
		}
		err = f()
		if err == nil || errors.Is(err, errSourceChanged) || errors.Is(err, errNoRanges) || s.ctx.Err() != nil {
			return err
		}
		s.logger.WithFields(log.Fields{"url": s.url, "attempt": attempt + 1, "error": err}).Debug("range request failed")
	}
	return err
}

func (s *httpSource) fetchChunk(idx int64) ([]byte, error) {
	start := idx * s.chunk
	end := start + s.chunk
	if end > s.size {
		end = s.size
	}
	var data []byte
	err := s.retry(func() error {
		req, err := s.newRequest(http.MethodGet)
		if err != nil {
			return err
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end-1))
		atomic.AddInt64(&s.stats.Requests, 1)
		resp, err := s.client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		switch {
		case resp.StatusCode == http.StatusPartialContent:
		case resp.StatusCode == http.StatusOK:
			return fmt.Errorf("%s: %w (range request answered with full content)", s.url, errNoRanges)
		default:
			return fmt.Errorf("%s: range %d-%d: HTTP %s", s.url, start, end-1, resp.Status)
		}
		if weakETag(resp.Header.Get("ETag")) != weakETag(s.etag) || resp.Header.Get("Last-Modified") != s.modified {
			return fmt.Errorf("%s: %w (ETag %q, Last-Modified %q)", s.url, errSourceChanged,
				resp.Header.Get("ETag"), resp.Header.Get("Last-Modified"))
		}
		if !strings.HasPrefix(resp.Header.Get("Content-Range"), fmt.Sprintf("bytes %d-%d/", start, end-1)) {
			return fmt.Errorf("%s: unexpected Content-Range %q", s.url, resp.Header.Get("Content-Range"))
		}
		buf := make([]byte, end-start)
		n, err := io.ReadFull(resp.Body, buf)
		atomic.AddInt64(&s.stats.BytesFetched, int64(n))
		if err != nil {
			return fmt.Errorf("%s: range %d-%d: %w", s.url, start, end-1, err)
		}
		data = buf
		return nil
	})
	return data, err
}

// get returns chunk idx, fetching it if needed; concurrent callers share
// one fetch.
func (s *httpSource) get(idx int64, prefetch bool) ([]byte, error) {
	s.mu.Lock()
	if el, ok := s.chunks[idx]; ok {
		s.lru.MoveToFront(el)
		s.mu.Unlock()
		if !prefetch {
			atomic.AddInt64(&s.stats.CacheHits, 1)
		}
		return el.Value.(*cachedChunk).data, nil
	}
	if f, ok := s.inflight[idx]; ok {
		s.mu.Unlock()
		if prefetch {
			return nil, nil
		}
		<-f.done
		return f.data, f.err
	}
	f := &chunkFetch{done: make(chan struct{})}
	s.inflight[idx] = f
	s.mu.Unlock()

	f.data, f.err = s.fetchChunk(idx)

	s.mu.Lock()
	delete(s.inflight, idx)
	if f.err == nil {
		s.chunks[idx] = s.lru.PushFront(&cachedChunk{idx, f.data})
		for s.lru.Len() > s.maxChunks {
			old := s.lru.Back()
			s.lru.Remove(old)
			delete(s.chunks, old.Value.(*cachedChunk).idx)
		}
	}
	s.mu.Unlock()
	close(f.done)
	return f.data, f.err
}

func (s *httpSource) maybeReadahead(idx int64) {
	s.mu.Lock()
	sequential := idx == s.last+1
	s.last = idx
	s.mu.Unlock()
	if !sequential {
		return
	}
	nchunks := (s.size + s.chunk - 1) / s.chunk
	for k := int64(1); k <= int64(s.readahead) && idx+k < nchunks; k++ {
		go s.get(idx+k, true)
	}
}

func (s *httpSource) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset")
	}
	if off >= s.size {
		return 0, io.EOF
	}
	n := 0
	for n < len(p) && off < s.size {
		idx := off / s.chunk
		if off%s.chunk == 0 || n == 0 {
			s.maybeReadahead(idx)
		}
		data, err := s.get(idx, false)
		if err != nil {
			return n, err
		}
		c := copy(p[n:], data[off-idx*s.chunk:])
		n += c
		off += int64(c)
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (s *httpSource) Size() int64         { return s.size }
func (s *httpSource) Identity() string    { return s.ident }
func (s *httpSource) KnownDigest() string { return "" }
func (s *httpSource) Close() error        { return nil }
func (s *httpSource) Stats() SourceStats {
	return SourceStats{
		BytesFetched: atomic.LoadInt64(&s.stats.BytesFetched),
		Requests:     atomic.LoadInt64(&s.stats.Requests),
		CacheHits:    atomic.LoadInt64(&s.stats.CacheHits),
		Retries:      atomic.LoadInt64(&s.stats.Retries),
	}
}
