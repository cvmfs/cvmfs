package pkg

import "time"

const (
	CVMFSChunkSize              = 24 * 1024 * 1024
	CVMFSInternalChunkSize      = 6 * 1024 * 1024
	IOBufferSize                = 1 * 1024 * 1024
	SkipIfFileOrDir             = 0
	DotSchemeDelimeter          = "."
	CurrentDirectory            = "."
	PreviousDirectory           = ".."
	EXTERNAL                    = 1
	CvmfsLocation               = "/cvmfs"
	FileDelimeter               = "/"
	CVMFSProtectedFile          = ".cvmfscatalog"
	CVMFSAutoProtectedFile      = ".cvmfsautocatalog"
	CVMFSConfigFileOverride     = "./.cvmfs_rsync.yaml"
	CVMFSCAConfigFileOverride   = "./.cvmfs_rsync_ca.yaml"
	CVMFSConfigFileSuffix       = "cvmfs-rsync.yaml"
	ConfigFilePrefix            = "/etc/cvmfs/gateway-client/"
	DefaultGroupPath            = "DEFAULT"
	DefaultGroup                = "DEFAULT"
	DefaultVersion              = "DEFAULT"
	OtherWriteableMask          = 0o0002
	GroupWriteableMask          = 0o0020
	OwnerWriteableMask          = 0o0200
	CvmfsRsyncDBDirPrefix       = "cvmfs_rsync_run_database.*"
	CvmfsRsyncDBName            = "cvmfs_rsync_database.db"
	PreSyncDelaySeconds         = 2
	TrueString                  = "true"
	WriteBitLetter              = "w"
	PSha1MetadataString         = "psha1"
	StartOffsetHeader           = "pull-start-offset"
	PartSizeHeader              = "pull-part-size"
	CompressedHeader            = "pull-compressed"
	MaxCoresTaken               = 8
	SlurmJobCpusPerNode         = "SLURM_JOB_CPUS_PER_NODE"
	HasherUploaderAmount        = 2
	WorkerScalar                = 2
	WorkerMax                   = 64
	IOFilewalkThreadScalar      = 4
	IOFilewalkThreadMax         = 128
	ComputeFilewalkThreadScalar = 2
	ComputeFilewalkThreadMax    = 128
	DeprecationTimeout          = 3  // Seconds
	DeprecationWarningTime      = 14 // Deprecation warning time in days
	TeamProductsRetry           = 5
	GCSMaxKeyLength             = 1024
	HighPriorityVal             = 2
	MedPriorityVal              = 1
	LowPriorityVal              = 0
	HighPriority                = "high"
	MedPriority                 = "med"
	LowPriority                 = "low"
	SmallFileSizeHeuristic      = 6 * 1024 * 1024
	LargeFileSizeHeuristic      = 1 * 1024 * 1024 * 1024
	ChunkListXattr              = "user.chunk_list"
	CommaSeparator              = ","
	LineSeparator               = "\012"
	NestedCatalog               = 1
	FilesUncompressed           = 0
	DefaultTimeout              = 5 * time.Second
	DefaultTelegrafAddr         = "127.0.0.1:8092"
	FullyContainerizedTempDir   = "/dev"
	TestingUnownedUGid          = 65534
	TestingUnownedUserGroup     = "nobody"

	LocalTestMount                  = "/tmp/cvmfs_rsync_test_mount"
	FullyContainerizedTestMount     = "/cvmfs/test.repo"
	LocalTestMountName              = "cvmfs_rsync_test_mount"
	FullyContainerizedTestMountName = "test.repo"

	FaclCommentChar = '#'
	FaclDefaultStr  = "default"

	S3PurgeRetry                     = 5
	S3RequestLimitExceeded           = "RequestLimitExceeded"
	S3SlowDown                       = "SlowDown"
	S3InternalError                  = "InternalError"
	S3RequestError                   = "RequestError"
	S3RequestLimitExceededRetrySleep = 5 // Seconds

	LeasePathRegexName = "lease_path"
	RevisionRegexName  = "revision"
	LeasePathRegex     = `Lease path is (.*)`
	RevisionRegex      = `new_root_hash: .*, new revision: (\d+)`
	MissingMetric      = "None"
	MountRevisionXattr = "user.revision"
)
