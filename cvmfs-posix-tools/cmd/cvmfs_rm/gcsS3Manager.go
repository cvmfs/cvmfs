package main

import (
	"fmt"
	"net/http"
	"net/url"

	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/rs/zerolog/log"
)

type ErrorData struct {
	err error
}

func getProxy(proxy string) (func(*http.Request) (*url.URL, error), error) {
	if proxy == "" || proxy == "DIRECT" {
		return nil, nil
	}
	proxyUrl, err := url.Parse(proxy)
	if err != nil {
		log.Error().Err(err).Str("Url", proxy).Msg("Failed to parse URL")
		return nil, err
	}
	if !(proxyUrl.Scheme == "http" || proxyUrl.Scheme == "https") {
		err := fmt.Errorf("Proxy URL supplied (%s) is not of http or https schema", proxy)
		log.Error().Err(err)
		return nil, err
	}
	return http.ProxyURL(proxyUrl), nil
}

func purgeSingleFile(ctx Context, pFile pkg.PurgeFile, s3Interface S3Interface) ErrorData {
	log.Debug().Str("File", pFile.PathStr).Msg("Attempting to Purge File")
	s3Dest := pathlib.NewPath(ctx.cfg.Repo.S3Prefix).JoinPath(pathlib.NewPath(pFile.PathStr))
	if err := s3Interface.purge(s3Dest.Clean().String()); err != nil {
		return ErrorData{err: err}
	}
	log.Debug().Str("File", pFile.PathStr).Msg("File Purged")
	return ErrorData{}
}

var purgeFiles = func(ctx Context, db pkg.DB, s3Interface, alternateS3Interface S3Interface) error {
	log.Info().Msg("Purging Files")
	filesPurged := db.QueryPurges()

	errs := make(chan error, len(filesPurged))
	purgeErrManager := func(purgeErrorData ErrorData) {
		if purgeErrorData.err != nil {
			errs <- purgeErrorData.err
		}
	}
	s3Modifier[pkg.PurgeFile, ErrorData](ctx, filesPurged, s3Interface, alternateS3Interface, purgeSingleFile, purgeErrManager)

	close(errs)

	if len(errs) > 0 {
		unskippableErrors := 0
		for err := range errs {
			aerr, ok := err.(awserr.Error)
			// I very much dislike that this error code is hardcoded, but I couldn't find the s3 go call that maps to it
			if !ok || aerr.Code() != S3NoSuchKey {
				unskippableErrors += 1
				log.Error().Err(err).Msg("Error occured")
			}
		}
		if unskippableErrors > 0 {
			err := fmt.Errorf("one or more files failed purging")
			log.Error().Err(err).Msg("One or more files failed purging, check logs for more info")
			return err
		}
	}

	log.Info().Msg("Finished Purging All Files")
	return nil
}
