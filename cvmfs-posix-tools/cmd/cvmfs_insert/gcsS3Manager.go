package main

import (
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	pathlib "github.com/chigopher/pathlib"
	"github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"
	"github.com/rs/zerolog/log"
)

type UploadStatistics struct {
	numFiles  int
	delta     float64
	rate      float64
	totalSize int64 // bytes
}

type FileErrorData struct {
	uploadFileData     *pkg.UploadFile
	originalUploadDest string
	updatedFileData    pkg.DBFileReplace
	updatedLinkData    pkg.DBLinkReplace
	update             bool
	err                error
}

type ErrorData struct {
	err error
}

// Get proxy url object from proxy string
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

// Get updated file info based on the info passed in
func updateUploadFileInfo(ctx Context, srcInfo fs.FileInfo, workingUploadFile *pkg.UploadFile, fileErrData *FileErrorData, firstTime bool, hashes pkg.FileHashData) error {
	var err error
	if !pathlib.IsFile(srcInfo.Mode()) {
		err := fmt.Errorf(workingUploadFile.SrcPathString + " is no longer a file")
		log.Error().Err(err).Str("File", workingUploadFile.SrcPathString).Msg("This is no longer a file")
		return err
	}
	workingUploadFile.FileSize = srcInfo.Size()
	workingUploadFile.Modtime = srcInfo.ModTime().UnixNano()
	// fileErrorData.update = true
	workingCtx := ctx
	workingCtx.cfg = pkg.GetBasePathPrefix(ctx.cfg, pathlib.NewPath(workingUploadFile.DestPathString).Parent())
	hashStrings := pkg.HashesToStrings(hashes.Hashes)

	destPath := pathlib.NewPath(workingUploadFile.DestPathString)

	if workingCtx.cfg.Repo.DotScheme {
		if firstTime {
			workingUploadFile.DestPathString, fileErrData.updatedLinkData, err = getDotLinkData(workingCtx, destPath, srcInfo, hashes)
		} else {
			workingUploadFile.DestPathString, fileErrData.updatedLinkData, err = updatedDotLinkData(workingCtx, destPath, srcInfo, hashes)
		}
		if err != nil {
			return err
		}
	}

	owner, group, mode, err := pkg.GetPermsForUpload(workingCtx.cfg, srcInfo, true, workingCtx.acls)
	if err != nil {
		return err
	}
	workingUploadFile.Owner = owner
	workingUploadFile.Group = group
	workingUploadFile.Mode = mode
	workingUploadFile.Checksum = fmt.Sprintf("%040x", hashes.Checksum)
	// destPathName := pathlib.NewPath(workingUploadFile.destPathString).Name() // Variable created for readability
	fileErrData.updatedFileData = pkg.CreateFileReplace(workingUploadFile.DestPathString, mode, srcInfo.ModTime().UnixNano(), owner, group, srcInfo.Size(),
		strings.Join(hashStrings, ","), workingUploadFile.Checksum, srcInfo)
	return nil
}

// Get new dot link data for passed in info and hashes
func getDotLinkData(ctx Context, destPath *pathlib.Path, srcInfo fs.FileInfo, hashes pkg.FileHashData) (string, pkg.DBLinkReplace, error) {
	var err error
	destPathNameSlice := strings.Split(destPath.Name(), pkg.DotSchemeDelimeter)
	destPathName := pkg.DotSchemeDelimeter + strings.Join(append(destPathNameSlice, fmt.Sprintf("%040x", hashes.Checksum)), pkg.DotSchemeDelimeter)
	newDestPathString := destPath.Parent().Join(destPathName).Clean().String()
	owner, group, _, err := pkg.GetPermsForUpload(ctx.cfg, srcInfo, true, ctx.acls)
	if err != nil {
		return newDestPathString, pkg.DBLinkReplace{}, err
	}
	return newDestPathString, pkg.CreateLinkReplace(
		destPath.Clean().String(),
		destPathName,
		srcInfo.ModTime().UnixNano(),
		owner,
		group,
		pkg.SkipIfFileOrDir,
	), nil
}

// Get updated dot link data from passed in info and hashes
func updatedDotLinkData(ctx Context, destPath *pathlib.Path, srcInfo fs.FileInfo, hashes pkg.FileHashData) (string, pkg.DBLinkReplace, error) {
	var err error
	destPathNameSlice := strings.Split(destPath.Name(), pkg.DotSchemeDelimeter)
	destPathName := strings.Join(append(destPathNameSlice[:len(destPathNameSlice)-1], fmt.Sprintf("%040x", hashes.Checksum)), pkg.DotSchemeDelimeter)
	newDestPathString := destPath.Parent().Join(destPathName).Clean().String()
	owner, group, _, err := pkg.GetPermsForUpload(ctx.cfg, srcInfo, true, ctx.acls)
	if err != nil {
		return newDestPathString, pkg.DBLinkReplace{}, err
	}
	return newDestPathString, pkg.CreateLinkReplace(
		destPath.Parent().Join(strings.Join(destPathNameSlice[1:len(destPathNameSlice)-1], pkg.DotSchemeDelimeter)).Clean().String(),
		destPathName,
		srcInfo.ModTime().UnixNano(),
		owner,
		group,
		pkg.SkipIfFileOrDir,
	), nil
}

// Determine if file needs to be retried with updated info
func retryFileUpload(srcInfo fs.FileInfo, workingUploadFile *pkg.UploadFile) bool {
	return workingUploadFile.Modtime != srcInfo.ModTime().UnixNano()
}

// Hash the file and create update info. Return the associated upload readers
func hashUpdateAndGetUploadReaders(ctx Context, workingUploadFile *pkg.UploadFile, fileErrorData *FileErrorData, srcInfo fs.FileInfo, hashReaders []pkg.NamedReader, f io.ReadSeekCloser, firstTime bool, compressor *pkg.Compressor) ([]pkg.NamedReader, error) {
	uploadReaders := []pkg.NamedReader{}
	hashData, zeroLength, err := ctx.hasher.HashFileFromReaderList(workingUploadFile.SrcPathString, hashReaders, ctx.cvmfsChunkSize, compressor)
	if err != nil {
		return nil, err
	}
	log.Debug().Str("File", workingUploadFile.DestPathString).Msg("Adding in checksum data")
	if err := updateUploadFileInfo(ctx, srcInfo, workingUploadFile, fileErrorData, firstTime, hashData); err != nil {
		return nil, err
	}
	fileErrorData.update = true
	if ctx.cfg.Repo.ContentAddressable {
		if !zeroLength {
			for i, hashStr := range pkg.HashesToStrings(hashData.Hashes) {
				s3NamePath := pathlib.NewPath(hashStr[0:2]).Join(hashStr[2:] + "P")
				uploadReaders = append(uploadReaders, pkg.NamedReader{Name: s3NamePath.Clean().String(), Reader: hashReaders[i].Reader, StartOffset: hashReaders[i].StartOffset, PartSize: hashReaders[i].PartSize})
			}
		} else {
			hashStr := pkg.HashesToStrings(hashData.Hashes)[0]
			s3NamePath := pathlib.NewPath(hashStr[0:2]).Join(hashStr[2:] + "P")
			uploadReaders = []pkg.NamedReader{{Name: s3NamePath.Clean().String(), Reader: f, StartOffset: 0, PartSize: 0}}
		}
	} else {
		uploadReaders = []pkg.NamedReader{{Name: workingUploadFile.DestPathString, Reader: f, StartOffset: 0, PartSize: workingUploadFile.FileSize}}
	}
	return uploadReaders, nil
}

// Upload a single file to the s3 bucket. If not checksum, start by getting the proper hash
func uploadSingleFile(ctx Context, uploadFile pkg.UploadFile, s3Interface S3Interface, compressor *pkg.Compressor) FileErrorData {
	workingUploadFile := uploadFile
	fileErrorData := FileErrorData{uploadFileData: &workingUploadFile, originalUploadDest: uploadFile.DestPathString, update: false, err: nil}

	// Get file readers to pass into hasher and uploader
	readers, f, err := pkg.GetFileReaders(workingUploadFile.SrcPathString, workingUploadFile.FileSize, ctx.cvmfsChunkSize, ctx.numHashers)
	if err != nil {
		fileErrorData.err = err
		return fileErrorData
	}
	defer func() {
		if tempErr := f.Close(); tempErr != nil {
			log.Error().Err(tempErr).Msg("Error in cleanup")
			if fileErrorData.err == nil {
				fileErrorData.err = tempErr
			}
		}
	}()

	var uploadReaders []pkg.NamedReader

	// Hash files if necessary and get the readers for the uploader
	if !ctx.cfg.Repo.ContentAddressable {
		uploadReaders = []pkg.NamedReader{{Name: workingUploadFile.DestPathString, Reader: f, StartOffset: 0, PartSize: workingUploadFile.FileSize}}
	} else {
		if uploadReaders, err = hashUpdateAndGetUploadReaders(ctx, &workingUploadFile, &fileErrorData, uploadFile.SrcInfo, readers, f, true, compressor); err != nil {
			fileErrorData.err = err
			return fileErrorData
		}
	}

	var partSize int64
	partSize, err = getPartSize(uploadFile)
	if err != nil {
		fileErrorData.err = err
		return fileErrorData
	}

	// Try to upload the file, retrying on file data change
	retries := UploadRetries
	for retries > 0 {
		log.Info().Str("File", workingUploadFile.DestPathString).Msg("Uploading File")

		err := s3Interface.uploadReaderList(workingUploadFile.DestPathString, uploadReaders, partSize, ctx.numConcurrentUploaders, !ctx.cfg.Repo.DotScheme && !ctx.cfg.Repo.ContentAddressable, &workingUploadFile, compressor)
		if err != nil {
			fileErrorData.err = err
			return fileErrorData
		}

		// We always want to target the underlying file
		srcInfo, err := os.Stat(workingUploadFile.SrcPathString)
		if err != nil {
			log.Error().Err(err).Str("File", workingUploadFile.SrcPathString).Msg("Error stating file")
			fileErrorData.err = err
			return fileErrorData
		}

		// Retry with new data, but update the current readers and file first to reflect changes
		if retryFileUpload(srcInfo, &workingUploadFile) {

			if !ctx.retryChangedFiles {
				fileErrorData.err = fmt.Errorf(workingUploadFile.SrcPathString + " has been changed. Will not upload for now.")
				log.Error().Err(fileErrorData.err).Str("File", workingUploadFile.SrcPathString).Str("RETRY_NUM", strconv.Itoa(UploadRetries+1)).Msg("File has been changed, will not upload right now.")
				return fileErrorData
			}

			log.Info().Str("File", workingUploadFile.SrcPathString).Msg("Reuploading File")

			if err := f.Close(); err != nil {
				fileErrorData.err = err
				return fileErrorData
			}
			readers, f, err = pkg.GetFileReaders(workingUploadFile.SrcPathString, srcInfo.Size(), ctx.cvmfsChunkSize, ctx.numHashers)
			if err != nil {
				fileErrorData.err = err
				return fileErrorData
			}
			if uploadReaders, err = hashUpdateAndGetUploadReaders(ctx, &workingUploadFile, &fileErrorData, srcInfo, readers, f, false, compressor); err != nil {
				fileErrorData.err = err
				return fileErrorData
			}
			retries--
		} else {
			return fileErrorData
		}
	}
	fileErrorData.err = fmt.Errorf(workingUploadFile.SrcPathString + " has been retried, but has been changed each time. Will not upload for now.")
	log.Error().Err(fileErrorData.err).Str("File", workingUploadFile.SrcPathString).Str("RETRY_NUM", strconv.Itoa(UploadRetries+1)).Msg("File has been retried RETRY_NUM times, will not upload right now.")
	return fileErrorData
}

// Process any errors encountered during uploading files
func processFileErrors(errs <-chan FileErrorData, dotScheme bool, db pkg.DB) error {
	if len(errs) > 0 {
		for fileErrorData := range errs {
			log.Error().Err(fileErrorData.err).Str("File", fileErrorData.uploadFileData.SrcPathString).Msg("File was not able to be uploaded at this time due to the noted error. Removing from grafting.")
			if dotScheme {
				destPathPath := pathlib.NewPath(fileErrorData.uploadFileData.DestPathString)
				destPathNameSlice := strings.Split(destPathPath.Name(), pkg.DotSchemeDelimeter)
				if err := db.RemoveLink(destPathPath.Parent().Join(strings.Join(destPathNameSlice[1:len(destPathNameSlice)-1], pkg.DotSchemeDelimeter)).Clean().String()); err != nil {
					log.Error().Err(err).Msg("Error removing link in upload err")
				}
			}
			if err := db.RemoveFile(fileErrorData.uploadFileData.DestPathString); err != nil {
				log.Error().Err(err).Msg("Error removing file in upload err")
			}
		}
		return fmt.Errorf("errors occured during uploading to gcs")
	}
	return nil
}

// Process any updates encountered during uploading files
func processFileUpdates(updates <-chan FileErrorData, dotScheme, contentAddressable bool, db pkg.DB) error {
	if len(updates) > 0 {
		for fileErrorData := range updates {
			if dotScheme {
				uLinkData := fileErrorData.updatedLinkData
				if err := db.ReplaceLink(uLinkData.GetName(), uLinkData.GetTarget(), uLinkData.GetMtime(), uLinkData.GetOwner(), uLinkData.GetGroup(), uLinkData.GetSkip()); err != nil {
					log.Error().Err(err).Msg("Error replacing link in upload update")
					return err
				}
			}
			uFileData := fileErrorData.updatedFileData
			if err := db.RemoveFile(fileErrorData.originalUploadDest); err != nil {
				log.Error().Err(err).Msg("Error removing file in upload update")
				return err
			}
			if err := db.InsertUpdatedFile(uFileData.GetName(), fileErrorData.uploadFileData.SrcPathString, uFileData.GetMode(), uFileData.GetMtime(), uFileData.GetOwner(),
				uFileData.GetGroup(), uFileData.GetSize(), uFileData.GetHashes(), uFileData.GetChecksum(), uFileData.GetSrcInfo(), pkg.BoolToInt(contentAddressable), uFileData.GetAlternateBucket()); err != nil {
				log.Error().Err(err).Msg("Error inserting file in upload update")
				return err
			}
		}
		if err := db.UpdateFilesTableWithUpdatedFiles(); err != nil {
			return err
		}
	}
	return nil
}

// Upload files from db to s3 bucket provided from s3Interface
var uploadFiles = func(ctx Context, s3Interface, alternateS3Interface S3Interface, db pkg.DB) (UploadStatistics, error) {
	log.Info().Msg("Uploading Files:")
	filesUploaded := db.QueryUploadFiles()

	total_size := int64(0)
	for _, v := range filesUploaded {
		total_size += v.FileSize
	}
	start_time := time.Now()

	var compressor *pkg.Compressor
	if ctx.cfg.Repo.ContentAddressable {
		compressor = pkg.NewZlibCompressor(pkg.IOBufferSize)
	}

	errs := make(chan FileErrorData, len(filesUploaded))
	updates := make(chan FileErrorData, len(filesUploaded))
	fileErrorDataManager := func(fileErrorData FileErrorData) {
		if fileErrorData.err != nil {
			errs <- fileErrorData
		} else if fileErrorData.update {
			updates <- fileErrorData
		}
	}

	s3Modifier[pkg.UploadFile, FileErrorData](ctx, filesUploaded, s3Interface, alternateS3Interface, compressor, uploadSingleFile, fileErrorDataManager)

	end_time := time.Now()
	delta := end_time.Sub(start_time).Seconds()
	rate := float64(total_size/1000000.) / delta
	uploadStatistics := UploadStatistics{numFiles: len(filesUploaded), delta: delta, rate: rate, totalSize: total_size}
	if len(filesUploaded) > 0 {
		log.Info().Int("Num files", uploadStatistics.numFiles).Float64("delta (s)", delta).Float64("Upload MB/s", rate).Msg("File uploads done")
	}
	close(errs)
	close(updates)

	if err := processFileErrors(errs, ctx.cfg.Repo.DotScheme, db); err != nil {
		return uploadStatistics, err
	}
	if err := processFileUpdates(updates, ctx.cfg.Repo.DotScheme, ctx.cfg.Repo.ContentAddressable, db); err != nil {
		return uploadStatistics, err
	}
	// time.Sleep(10 * time.Minute)
	log.Info().Msg("Finished Uploading Files")

	return uploadStatistics, nil
}
