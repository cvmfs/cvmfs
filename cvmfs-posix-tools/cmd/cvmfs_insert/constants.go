package main

import "github.com/cvmfs/cvmfs/cvmfs-posix-tools/pkg"

const (
	MinUploadPartSize    int64 = 4 * pkg.CVMFSChunkSize                                             // 96MiB, multiple of chunk size
	MaxUploadPartSize    int64 = (5 * 1024 * 1024 * 1024 / pkg.CVMFSChunkSize) * pkg.CVMFSChunkSize // largest multiple of 24MiB that's under 5 GiB (5 GiB is the maximum allowed by XML multipart upload)
	MaxFileSize          int64 = 5 * 1024 * 1024 * 1024 * 1024                                      // 5 TiB is the hard limit of file size
	S3NotFoundError            = "NotFound"
	S3InternalError            = "InternalError"
	S3ServiceUnavailable       = "ServiceUnavailable"
	SlurmJobCpusPerNode        = "SLURM_JOB_CPUS_PER_NODE"
	UploadRetries              = 3
	AutoRegion                 = "auto"
)
