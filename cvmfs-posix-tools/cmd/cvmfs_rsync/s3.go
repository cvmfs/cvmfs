package main

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	humanize "github.com/dustin/go-humanize"
	"github.com/rs/dnscache"
	"github.com/rs/zerolog/log"
)

var (
	HeaderValueRegex = regexp.MustCompile(`[^A-Za-z0-9_ :;.,\/"'?!(){}[\]@<>=\-+*#$&` + "`" + `|~^%]`)
)

type S3Metadata map[string]string
type S3FinalMetadata map[string]*string

type S3Interface interface {
	uploadGivenInfo(reader io.ReadSeeker, partSize int64, retries int, s3Dest string, fileToUpload *pkg.UploadFile, overwriteExisting bool, headObjectCheck bool) error
	uploadReaderList(fileName string, readers []pkg.NamedReader, partSize int64, maxConcurrentUploaders int, overwriteExisting bool, headObjectCheck bool, fileToUpload *pkg.UploadFile, compressor *pkg.Compressor) error
	checkObjectExists(s3Dest string, retries int) (bool, error)
	checkObjectExistsName(s3Name string, retries int) (bool, error)
	purge(s3Name string) error
}

type S3Manager struct {
	svc              *s3.S3
	reqHeaders       request.Option
	uploader         *s3manager.Uploader
	bucket           string
	prefix           string
	bucketUrlPrefix  string
	compressorPool   *sync.Pool
	bufferPool       *sync.Pool
	bufferReaderPool *sync.Pool
	copyBufferPool   *sync.Pool
	region           string
}

// Setup custom transport with custom dial context
func setupCustomTransport(proxy func(*http.Request) (*url.URL, error), maxConns int) *http.Transport {
	r := &dnscache.Resolver{}

	return &http.Transport{
		WriteBufferSize: pkg.IOBufferSize,
		Proxy:           proxy,
		DialContext: func(ctx context.Context, network string, addr string) (conn net.Conn, err error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			ips, err := r.LookupHost(ctx, host)
			if err != nil {
				return nil, err
			}

			rand.Seed(time.Now().UnixNano())

			rand.Shuffle(len(ips), func(i, j int) {
				ips[i], ips[j] = ips[j], ips[i]
			})

			for _, ip := range ips {

				var dialer = net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
					Control: func(network, address string, c syscall.RawConn) (err error) {
						return c.Control(func(fd uintptr) {
							err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_TOS, 32)
							if err != nil {
								return
							}
						})
					},
				}

				conn, err = dialer.DialContext(ctx, network, net.JoinHostPort(ip, port))
				if err == nil {
					break
				}
			}
			return
		},
		ForceAttemptHTTP2:     false,
		MaxIdleConns:          maxConns * 2,
		MaxIdleConnsPerHost:   maxConns * 2,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig: &tls.Config{
			NextProtos: []string{"http/1.1"},
		},
	}
}

// Set an unsigned payload if uploading to google
func SetUnsignedPayload(r *request.Request) {
	// log.Info().Str("Key", r.Operation.Name).Msg("Upload Op")
	if r.Operation.Name != "UploadPart" && r.Operation.Name != "PutObject" {
		return
	}

	if strings.Contains(r.HTTPRequest.URL.Hostname(), "google") {
		r.HTTPRequest.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

		for name, values := range r.HTTPRequest.Header {

			if strings.Contains(name, "X-Amz-Meta-") {

			}
			for _, value := range values {
				if strings.Contains(name, "X-Amz-Meta-") {
					r.HTTPRequest.Header.Set(strings.Replace(name, "X-Amz-Meta-", "x-goog-meta-", 1), value)
					r.HTTPRequest.Header[name] = nil
				}
				// fmt.Println(name, value)
			}
		}

	}

}

func SetUnsignedPayload2(r *request.Request) {
	// log.Info().Str("Key", r.Operation.Name).Msg("Upload Op")
	if r.Operation.Name != "UploadPart" && r.Operation.Name != "PutObject" {
		return
	}

	if strings.Contains(r.HTTPRequest.URL.Hostname(), "google") {
		r.HTTPRequest.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

		// for name, values := range r.HTTPRequest.Header {

		// 	if strings.Contains(name, "X-Amz-Meta-") {

		// 	}
		// 	for _, value := range values {
		// 		if strings.Contains(name, "X-Amz-Meta-") {
		// 			r.HTTPRequest.Header.Set(strings.Replace(name, "X-Amz-Meta-", "x-goog-meta-", 1), value)
		// 			r.HTTPRequest.Header[name] = nil
		// 		}
		// 		// fmt.Println(name, value)
		// 	}
		// }

	}

}

// Create a new s3 manager from the passed in arguments
func newS3Manager(numConcurrentUploaders, numWorkers int, endpoint, region, accessKey, secretKey, bucket, prefix, bucketUrlPrefix, hostname, uidString string,
	proxy func(*http.Request) (*url.URL, error)) (S3Manager, error) {

	s3Session, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Endpoint:             aws.String(endpoint),
			Region:               aws.String(region),
			Credentials:          credentials.NewStaticCredentials(accessKey, secretKey, ""),
			HTTPClient:           &http.Client{Transport: setupCustomTransport(proxy, numConcurrentUploaders*numWorkers)},
			S3ForcePathStyle:     aws.Bool(true),
			DisableSSL:           aws.Bool(true),
			LowerCaseHeaderMaps:  aws.Bool(true),
			Logger:               newCustomLogger(),
			S3Disable100Continue: aws.Bool(true),
			// S3DisableContentMD5Validation: aws.Bool(true),
		},
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed to create s3session")
		return S3Manager{}, err
	}

	return S3Manager{
		svc: s3.New(s3Session),
		reqHeaders: request.WithSetRequestHeaders(map[string]string{
			"User-Agent":   "cvmfs-rsync-" + CVMFS_RSYNC_VERSION,
			"X-CVMFS-GRID": hostname,
			"X-CVMFS-UID":  uidString,
		}),
		uploader: s3manager.NewUploader(s3Session, func(u *s3manager.Uploader) {
			u.Concurrency = numConcurrentUploaders
			u.BufferProvider = NewUnboundBufferedReadSeekerWriteToPool(pkg.IOBufferSize, pkg.IOBufferSize)
		}),
		bucket:          bucket,
		prefix:          prefix,
		region:          region,
		bucketUrlPrefix: bucketUrlPrefix,
		compressorPool: &sync.Pool{
			New: func() any {
				return zlib.NewWriter(nil)
			},
		},
		bufferPool: &sync.Pool{
			New: func() any {
				return new(bytes.Buffer)
			},
		},
		bufferReaderPool: &sync.Pool{
			New: func() any {
				return bytes.NewReader(nil)
			},
		},
		copyBufferPool: &sync.Pool{
			New: func() any {
				s := make([]byte, pkg.IOBufferSize)
				return &s
			},
		},
	}, nil
}

// Create a new s3 manager from ctx
var newBasicS3Manager = func(ctx Context) (S3Interface, error) {
	proxy, err := getProxy(ctx.cfg.Repo.Proxy)
	if err != nil {
		return nil, err
	}
	s3Interface, err := newS3Manager(ctx.numConcurrentUploaders, ctx.numWorkers, ctx.cfg.Repo.S3Endpoint, AutoRegion, ctx.cfg.Repo.S3AccessKey, ctx.cfg.Repo.S3SecretKey,
		ctx.cfg.Repo.S3Bucket, ctx.cfg.Repo.S3Prefix, ctx.cfg.Repo.UrlPrefix, ctx.hostname, ctx.uidString, proxy)
	if err != nil {
		return nil, err
	}
	return &s3Interface, nil
}

// Create a new alternate s3 manager from ctx
var newAlternateS3Manager = func(ctx Context) (S3Interface, error) {
	proxy, err := getProxy(ctx.cfg.Repo.Proxy)
	if err != nil {
		return nil, err
	}
	s3Interface, err := newS3Manager(ctx.numConcurrentUploaders, ctx.numWorkers, ctx.cfg.Repo.AlternateS3Endpoint, AutoRegion, ctx.cfg.Repo.AlternateS3AccessKey, ctx.cfg.Repo.AlternateS3SecretKey,
		ctx.cfg.Repo.AlternateS3Bucket, ctx.cfg.Repo.AlternateS3Prefix, ctx.cfg.Repo.AlternateUrlPrefix, ctx.hostname, ctx.uidString, proxy)
	if err != nil {
		return nil, err
	}
	return &s3Interface, nil
}

// Get the part size for uploading this file
func getPartSize(uploadFile pkg.UploadFile) (int64, error) {
	if uploadFile.FileSize > MaxFileSize {
		err := fmt.Errorf("filesize of %s exceeds max upload limit of %s", humanize.IBytes(uint64(uploadFile.FileSize)), humanize.IBytes(uint64(MaxFileSize)))
		log.Error().Err(err).Str("Path", uploadFile.DestPathString).Msg("Filesize too large for path")
		return 0, err
	}

	partSize := MinUploadPartSize
	if partSize > MaxUploadPartSize {
		partSize = MaxUploadPartSize
	}
	return partSize, nil
}

func genMetadataName(entry string) string {
	return "cvmfs-" + entry
}

func sanitizeMetadata(m *S3Metadata) {
	for k, v := range *m {
		(*m)[k] = HeaderValueRegex.ReplaceAllLiteralString(v, "_")
	}
}

func getMetadataForUploadFile(fileToUpload *pkg.UploadFile) S3FinalMetadata {
	metadata := make(S3Metadata)
	// var atime int64
	var mtime int64

	if fileToUpload == nil {
		return S3FinalMetadata{}
	}
	// var ctime int64

	// workingUploadFile.FileSize = srcInfo.Size()
	mtime = fileToUpload.SrcInfo.ModTime().UnixNano()
	// SrcPathString  string
	// DestPathString string
	// FileSize       int64

	srcInfo, err := os.Lstat(fileToUpload.SrcPathString)
	if err != nil {
		log.Error().Err(err).Msg("Error in debugging code")
	}

	metadata[genMetadataName("st-owner")] = strconv.Itoa(fileToUpload.Owner)
	metadata[genMetadataName("st-group")] = strconv.Itoa(fileToUpload.Group)
	metadata[genMetadataName("st-mode")] = fmt.Sprintf("0o%s", strconv.FormatInt(int64(fileToUpload.Mode), 8))
	metadata[genMetadataName("version")] = fmt.Sprintf("cvmfs_rsync %s", CVMFS_RSYNC_VERSION)
	// Any uploads have atime=mtime=ctime for cvmfs
	metadata[genMetadataName("st-atime")] = strconv.FormatInt(mtime, 10)
	metadata[genMetadataName("st-mtime")] = strconv.FormatInt(mtime, 10)
	metadata[genMetadataName("st-mtime_2")] = strconv.FormatInt(srcInfo.ModTime().UnixNano(), 10)
	metadata[genMetadataName("st-ctime")] = strconv.FormatInt(mtime, 10)
	metadata[genMetadataName("original")] = fileToUpload.SrcPathString

	metadata[genMetadataName("uploader")] = fmt.Sprintf("%s@%s", USERNAME, HOSTNAME)

	metadata[genMetadataName("size")] = strconv.FormatInt(fileToUpload.FileSize, 10)
	metadata[genMetadataName("hash")] = pkg.PSha1MetadataString
	metadata[genMetadataName(pkg.PSha1MetadataString)] = fileToUpload.Checksum
	metadata[genMetadataName("hash-chunk-size")] = strconv.FormatInt(pkg.CVMFSChunkSize, 10)
	// metadata.sanitize()

	sanitizeMetadata(&metadata)

	m := S3FinalMetadata{}
	for k, v := range metadata {
		m[k] = aws.String(v)
	}
	return m
}

// Upload a file to s3 with the following info
func (s3Interface *S3Manager) uploadGivenInfo(reader io.ReadSeeker, chunkSize int64, retries int, s3Dest string, fileToUpload *pkg.UploadFile, overwriteExisting bool, headObjectCheck bool) error {
	s3Dest = pkg.EscapeLineFeedAndCarriageReturn(s3Dest)

	if len(s3Dest) >= pkg.GCSMaxKeyLength {
		hash_path := pathlib.NewPath("/" + s3Interface.bucket).Join(pkg.EscapeCVMFSURL(s3Dest))
		log.Debug().Str("HASHED NAME OBJECT", hash_path.String()).Msg("Long path object")
		s3Dest = "/really_long_files/" + pkg.MD5HashURL(hash_path.String())
	}
	retryVal := retries
	for retries > 0 {
		if !overwriteExisting && headObjectCheck {
			if objExists, err := s3Interface.checkObjectExists(s3Dest, retryVal); err != nil {
				log.Error().Err(err).Msg("Non retryable error")
				return err
			} else if objExists {
				log.Debug().Str("Object", s3Dest).Msg("Object exists, skipping")
				return nil
			}
		}

		if err := pkg.SeekStart(reader); err != nil {
			return err
		}
		log.Debug().Str("Bucket", s3Interface.bucket).Str("Path", s3Dest).Msg("Uploading to s3 bucket with path")
		metadata := getMetadataForUploadFile(fileToUpload)
		if _, err := s3Interface.uploader.Upload(&s3manager.UploadInput{
			Bucket:   aws.String(s3Interface.bucket), // Should get this from config
			Key:      aws.String(s3Dest),
			Body:     reader,
			Metadata: metadata,
		}, func(u *s3manager.Uploader) {
			u.PartSize = chunkSize
			u.RequestOptions = []request.Option{s3Interface.reqHeaders}
			u.RequestOptions = append(u.RequestOptions, SetUnsignedPayload)
			// u.RequestOptions = append(u.RequestOptions, request.WithSetRequestHeaders(metadata))
		}); err != nil {
			aerr, ok := err.(awserr.Error)
			if aerr.Code() == S3SlowDown {
				retries--
				log.Error().Err(err).Msg("Too many requests, upload failed, backing off and retrying...")
				numOfSecondsToSleep := (UploadRetries - retries) * 5
				time.Sleep(time.Duration(numOfSecondsToSleep) * time.Second)
			} else if ok { //&& (aerr.Code() == S3InternalError || aerr.Code() == S3ServiceUnavailable || aerr.Code() == S3SlowDown || aerr.Code() == S3503SlowDown) {
				// These are retryable errors so we will just retry them. May be valuable to put in a retry timer/counter, but for now that's not present
				log.Debug().Str("Code", aerr.Code()).Msg("GCS error code")
				log.Error().Err(err).Msg("Upload failed, retrying...")
				retries--
			} else {
				log.Error().Err(err).Msg("Non retryable error")
				return err
			}
		} else {
			var objExists bool
			if headObjectCheck {
				if objExists, err = s3Interface.checkObjectExists(s3Dest, retryVal); err != nil {
					return err
				} else if !objExists {
					log.Debug().Str("Object", s3Dest).Msg("Failed to get head object")
					retries--
					// return err
				} else {
					log.Debug().Str("Object", s3Dest).Msg("Uploaded Object")
					return err
				}
			} else {
				log.Debug().Str("Object", s3Dest).Msg("Uploaded Object (no head check)")
				return err
			}
		}
	}
	err := fmt.Errorf("couldn't perform upload")
	log.Error().Err(err).Str("Key", s3Dest).Msg("Non retryable error")
	return err
}

// Worker to upload named readers from the provided channel
func (s3Interface *S3Manager) uploadReaderListWorker(readers <-chan pkg.NamedReader, errs chan<- error, partSize int64, overwriteExisting bool, headObjectCheck bool, fileToUpload *pkg.UploadFile, compressor *pkg.Compressor) {
	for reader := range readers {
		s3Dest := pathlib.NewPath(s3Interface.prefix).Join(reader.Name).Clean().String()

		errChan := make(chan error)
		itemsToWaitFor := 1
		go func() {
			// COMPRESSIONCHECK: Should compress here into reader and clean up afterwards
			activeReader := reader.Reader
			if compressor != nil {
				var cleanupFunction func() error
				var err error
				activeReader, cleanupFunction, err = pkg.CompressSectionReader(reader.Reader, compressor)
				if err != nil {
					errChan <- err
				}
				itemsToWaitFor++
				defer func() {
					if err := cleanupFunction(); err != nil {
						log.Error().Err(err).Msg("Error cleaning up compression code")
						errChan <- err
					} else {
						errChan <- nil
					}
				}()
			}
			errChan <- s3Interface.uploadGivenInfo(activeReader, partSize, UploadRetries, s3Dest, fileToUpload, overwriteExisting, headObjectCheck)
		}()

		for i := 0; i < itemsToWaitFor; i++ {
			if e := <-errChan; e != nil {
				errs <- e
			}
		}
	}
}

// Upload a list of named readers to the provided s3Interface
func (s3Interface *S3Manager) uploadReaderList(fileName string, readers []pkg.NamedReader, partSize int64, maxConcurrentUploaders int, overwriteExisting bool, headObjectCheck bool, fileToUpload *pkg.UploadFile, compressor *pkg.Compressor) error {
	numReaders := len(readers)

	var wg sync.WaitGroup
	readersChan := make(chan pkg.NamedReader, numReaders)
	errs := make(chan error, numReaders)

	t := numReaders
	if t > maxConcurrentUploaders {
		t = maxConcurrentUploaders
	}

	for i := 0; i < t; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s3Interface.uploadReaderListWorker(readersChan, errs, partSize, overwriteExisting, headObjectCheck, fileToUpload, compressor)
		}()
	}

	for _, reader := range readers {
		readersChan <- reader
	}
	close(readersChan)
	wg.Wait()
	close(errs)

	// Process any errors
	if len(errs) > 0 {
		for err := range errs {
			log.Error().Err(err).Msg("Received error while hashing")
		}
		err := fmt.Errorf("error while hashing file")
		log.Error().Err(err).Str("File", fileName).Msg("Error on file")
		return err
	}
	return nil
}

// Check an objects existance by name
func (s3Interface *S3Manager) checkObjectExistsName(s3Name string, retries int) (bool, error) {
	s3Dest := pathlib.NewPath(s3Interface.prefix).Join(s3Name).Clean().String()
	return s3Interface.checkObjectExists(s3Dest, retries)
}

// Check an objects existance by full destination (includes prefix)
func (s3Interface *S3Manager) checkObjectExists(s3Dest string, retries int) (bool, error) {
	// s3Dest := pathlib.NewPath(s3Interface.prefix).Join(s3Name).Clean().String()
	var err error
	for retries > 0 {
		headObj := s3.HeadObjectInput{
			Bucket: aws.String(s3Interface.bucket),
			Key:    aws.String(s3Dest),
		}
		if _, err = s3Interface.svc.HeadObjectWithContext(context.TODO(), &headObj, s3Interface.reqHeaders); err != nil {
			aerr, ok := err.(awserr.Error)
			// I very much dislike that this error code is hardcoded, but I couldn't find the s3 go call that maps to it
			if ok && aerr.Code() == S3NotFoundError {
				return false, nil
			}
			log.Debug().Err(err).Str("Object", s3Dest).Msg("Failed to get head object, retrying...")
		} else {
			return true, nil
		}
		retries--
	}
	log.Error().Err(err).Str("Object", s3Dest).Msg("Failed to get head object")
	return false, err
}

// Purge the given s3 destination
func (s3Interface *S3Manager) purge(s3Name string) error {
	// log.Debug().Str("File", fileName).Msg("Attempting to Purge File")
	var err error
	s3Dest := pathlib.NewPath(s3Interface.prefix).Join(s3Name).Clean().String()
	if len(s3Dest) >= pkg.GCSMaxKeyLength {
		hash_path := pathlib.NewPath("/" + s3Interface.bucket).Join(pkg.EscapeCVMFSURL(s3Dest))
		log.Debug().Str("HASHED NAME OBJECT", hash_path.String()).Msg("Long path object")
		s3Dest = "/really_long_files/" + pkg.MD5HashURL(hash_path.String())
	}

	retries := 0
	for retries < pkg.S3PurgeRetry {
		if _, err := s3Interface.svc.DeleteObjectWithContext(
			context.TODO(), // I'm not sure what context to use here
			&s3.DeleteObjectInput{
				Bucket: aws.String(s3Interface.bucket),
				Key:    aws.String(s3Dest),
			},
			s3Interface.reqHeaders,
		); err != nil {
			aerr, ok := err.(awserr.Error)
			if ok && (aerr.Code() == pkg.S3RequestLimitExceeded || aerr.Code() == pkg.S3SlowDown || aerr.Code() == pkg.S3InternalError || aerr.Code() == pkg.S3RequestError) {
				log.Debug().Err(err).Str("Object", s3Dest).Msg("Error in purging object, retrying")
				time.Sleep(pkg.S3RequestLimitExceededRetrySleep * time.Second)
			} else {
				log.Error().Err(err).Str("Object", s3Dest).Msg("Error in purging object")
				return err
			}
		} else {
			return nil
		}
		retries++
	}
	log.Error().Err(err).Str("Object", s3Dest).Msg("Error in purging object, exceeded retry count")
	return err
}

// A structure by which to spawn multiple threads to perform a modify function over a list of files
func s3Modifier[T pkg.S3File, E FileErrorData | ErrorData](ctx Context, filesModified []T, s3Interface, alternateS3Interface S3Interface, compressor *pkg.Compressor,
	modSingleFile func(ctx Context, uploadFile T, s3Interface S3Interface, compressor *pkg.Compressor) E, errManager func(E)) {

	var wg sync.WaitGroup

	files := make(chan T)

	for i := 0; i < ctx.numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for f := range files {
				var errorData E
				if f.UseAlternateBucket() {
					errorData = modSingleFile(ctx, f, alternateS3Interface, compressor)
				} else {
					errorData = modSingleFile(ctx, f, s3Interface, compressor)
				}
				errManager(errorData)
			}
		}()
	}

	for _, modFile := range filesModified {
		files <- modFile
	}
	close(files)

	wg.Wait()
}
