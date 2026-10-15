package main

import (
	"context"
	"crypto/tls"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/rs/dnscache"
	"github.com/rs/zerolog/log"
)

type S3Interface interface {
	purge(s3Dest string) error
}

type S3Manager struct {
	svc        *s3.S3
	reqHeaders request.Option
	bucket     string
}

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

type CustomLogger struct {
}

func newCustomLogger() CustomLogger {
	return CustomLogger{}
}

func (l CustomLogger) Log(args ...interface{}) {
	// Currently not logging anything from s3
}

func newS3Manager(numWorkers int, endpoint, region, accessKey, secretKey, bucket, bu, hostname, uidString string,
	proxy func(*http.Request) (*url.URL, error)) (S3Manager, error) {

	s3Session, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Endpoint:             aws.String(endpoint),
			Region:               aws.String(region),
			Credentials:          credentials.NewStaticCredentials(accessKey, secretKey, ""),
			HTTPClient:           &http.Client{Transport: setupCustomTransport(proxy, numWorkers)},
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
			"User-Agent":   "cvmfs-rm-" + CVMFS_RSYNC_VERSION,
			"X-CVMFS-BU":   bu,
			"X-CVMFS-GRID": hostname,
			"X-CVMFS-UID":  uidString,
		}),
		bucket: bucket,
	}, nil
}

var newBasicS3Manager = func(ctx Context) (S3Interface, error) {
	proxy, err := getProxy(ctx.cfg.Repo.Proxy)
	if err != nil {
		return nil, err
	}
	s3Interface, err := newS3Manager(ctx.numWorkers, ctx.cfg.Repo.S3Endpoint, AutoRegion, ctx.cfg.Repo.S3AccessKey, ctx.cfg.Repo.S3SecretKey,
		ctx.cfg.Repo.S3Bucket, ctx.bu, ctx.hostname, ctx.uidString, proxy)
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
	s3Interface, err := newS3Manager(ctx.numWorkers, ctx.cfg.Repo.AlternateS3Endpoint, AutoRegion, ctx.cfg.Repo.AlternateS3AccessKey, ctx.cfg.Repo.AlternateS3SecretKey,
		ctx.cfg.Repo.AlternateS3Bucket, ctx.bu, ctx.hostname, ctx.uidString, proxy)
	if err != nil {
		return nil, err
	}
	return &s3Interface, nil
}

func (s3Interface *S3Manager) purge(s3Dest string) error {
	// log.Debug().Str("File", fileName).Msg("Attempting to Purge File")
	// s3Dest := pathlib.NewPath(ctx.cfg.Repo.S3Prefix).JoinPath(pathlib.NewPath(fileName))
	var err error
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

func s3Modifier[T pkg.S3File, E ErrorData](ctx Context, filesModified []T, s3Interface, alternateS3Interface S3Interface,
	modSingleFile func(ctx Context, uploadFile T, s3Interface S3Interface) E, errManager func(E)) {

	var wg sync.WaitGroup

	files := make(chan T)

	for i := 0; i < ctx.numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for f := range files {
				var errorData E
				if f.UseAlternateBucket() {
					errorData = modSingleFile(ctx, f, alternateS3Interface)
				} else {
					errorData = modSingleFile(ctx, f, s3Interface)
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
