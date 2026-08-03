// CernVM-FS server configuration schema

// Field forms:
//   NAME: T             required
//   NAME?: T            optional
//   NAME: T | *default  optional; carries a default because a dependency rule reads it

package cvmfs

import "strconv"

// Boolean field values are case-insensitive:
#On:    =~"^([Yy][Ee][Ss]|[Oo][Nn]|1|[Tt][Rr][Uu][Ee])$"
#Off:   =~"^([Nn][Oo]|[Oo][Ff][Ff]|0|[Ff][Aa][Ll][Ss][Ee])$"
#Bool:  #On | #Off
_#onRE: "^([Yy][Ee][Ss]|[Oo][Nn]|1|[Tt][Rr][Uu][Ee])$"

#FQRN: =~"^[a-z0-9][a-z0-9.-]*\\.[a-z0-9-]+$"

// Comma-separated list of fully qualified repository names.
#FQRNList: =~"^[a-z0-9][a-z0-9.-]*\\.[a-z0-9-]+(,[a-z0-9][a-z0-9.-]*\\.[a-z0-9-]+)*$"
#AbsPath:  =~"^/"

// Colon-separated list of absolute paths (files or directories).
#AbsPathList: =~"^/[^:]*(:/[^:]*)*$"

// Unsigned integer, 0 allowed.
#UInt: =~"^[0-9]+$"

// Strictly positive integer.
#PosInt: =~"^0*[1-9][0-9]*$"

// Signed integer.
#Int: =~"^-?[0-9]+$"

// Comma-separated list of unsigned integers.
#UIntList: =~"^[0-9]+(,[0-9]+)*$"

#IPv4: =~"^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])$"
#IPv6: =~"^(([0-9A-Fa-f]{1,4}:){7}[0-9A-Fa-f]{1,4}|([0-9A-Fa-f]{1,4}:){1,7}:|([0-9A-Fa-f]{1,4}:){1,6}:[0-9A-Fa-f]{1,4}|([0-9A-Fa-f]{1,4}:){1,5}(:[0-9A-Fa-f]{1,4}){1,2}|([0-9A-Fa-f]{1,4}:){1,4}(:[0-9A-Fa-f]{1,4}){1,3}|([0-9A-Fa-f]{1,4}:){1,3}(:[0-9A-Fa-f]{1,4}){1,4}|([0-9A-Fa-f]{1,4}:){1,2}(:[0-9A-Fa-f]{1,4}){1,5}|[0-9A-Fa-f]{1,4}:(:[0-9A-Fa-f]{1,4}){1,6}|:((:[0-9A-Fa-f]{1,4}){1,7}|:))$"
#IP:   #IPv4 | #IPv6

// RFC 1123 hostname (also matches IPv4 literals).
#Hostname: =~"^[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?(\\.[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*$"
#Host:     #Hostname | #IP

// TCP port, 1-65535.
#Port: =~"^([1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$"

// Single HTTP(S) URL; no whitespace or chain separators allowed.
#URL: =~"(?i)^https?://[^ \\t;|,]+$"

// Chain of HTTP(S) URLs: ';' separates failover groups,
// '|' separates load-balanced members within a group.
#URLChain: =~"(?i)^https?://[^ \\t;|]+([;|]https?://[^ \\t;|]+)*$"

// Proxy chain: ';' separates failover groups, '|' load-balanced members.
// An entry is DIRECT (no proxy), auto (WPAD/PAC discovery, wpad.cc) or a
// proxy address with optional scheme and port.
_#proxyEntry: "(DIRECT|auto|(https?://)?[A-Za-z0-9._-]+(:[0-9]{1,5})?)"
#ProxyChain:  =~"(?i)^\(_#proxyEntry)([;|]\(_#proxyEntry))*$"

// Date threshold as understood by `date -d`, e.g. "3 days ago".
#TimeSpan: =~"^[0-9]+ +(second|minute|hour|day|week|month|year)s? +ago$"

// POSIX user name.
#UserName: =~"^[A-Za-z_][A-Za-z0-9._-]*$"

// Cache quota in MB; -1 means unlimited.
#QuotaLimit: =~"^(-1|[0-9]+)$"

// ===========================================================================
// Server configuration
// ===========================================================================

#ServerConfig: {
	// Set to false to silence AUFS kernel deadlock warning.
	CVMFS_AUFS_WARNING?: #Bool
	// Enables the automatic garbage collection on publish and snapshot.
	CVMFS_AUTO_GC: #Bool | *""
	// Date-threshold for automatic garbage collection (for example:
	// "3 days ago", "1 week ago").
	CVMFS_AUTO_GC_TIMESPAN?: #TimeSpan
	// Frequency of auto garbage collection, only garbage collect if
	// last GC is before the given threshold (for example: "1 day ago").
	CVMFS_AUTO_GC_LAPSE?: #TimeSpan
	// Set to true to enable automatic recovery from bogus server mount
	// states.
	CVMFS_AUTO_REPAIR_MOUNTPOINT?: #Bool
	// Creates a generic revision tag for each published revision (if
	// set to true).
	CVMFS_AUTO_TAG?: #Bool
	// Date-threshold for automatic tags, after which auto tags get
	// removed (for example: "4 days ago").
	CVMFS_AUTO_TAG_TIMESPAN?: #TimeSpan
	// Enable/disable automatic catalog management using autocatalogs.
	CVMFS_AUTOCATALOGS?: #Bool
	// Maximum number of entries in an autocatalog to be considered
	// overflowed. Default value: 100000 (see also CVMFS_AUTOCATALOGS).
	CVMFS_AUTOCATALOGS_MAX_WEIGHT: #PosInt | *""
	// Minimum number of entries in an autocatalog to be considered
	// underflowed. Default value: 1000 (see also CVMFS_AUTOCATALOGS).
	CVMFS_AUTOCATALOGS_MIN_WEIGHT: #PosInt | *""
	// Desired Average size of a file chunk in bytes (see also
	// CVMFS_USE_FILE_CHUNKING).
	CVMFS_AVG_CHUNK_SIZE: #PosInt | *"8388608"
	// Enable/disable generation of catalog bootstrapping shortcuts
	// during publishing. (Useful when backend directory /data is not
	// publicly accessible)
	CVMFS_CATALOG_ALT_PATHS?: #Bool
	// Minimum number of days between checking each repository with
	// `cvmfs_server check -a`. Default value: 30.
	CVMFS_CHECK_ALL_MIN_DAYS?: #UInt
	// Compression algorithm to be used during publishing (currently
	// either 'default' or 'none'; "zlib" is accepted as an alias of
	// "default", compression.cc).
	CVMFS_COMPRESSION_ALGORITHM?: "default" | "zlib" | "none"
	// The CernVM-FS version that was used to create this repository
	// (do not change manually).
	CVMFS_CREATOR_VERSION: =~"^[0-9]+(\\.[0-9]+(\\.[0-9]+)?)?(-[0-9]+)?$"
	// Disable checking of OverlayFS version before usage. (see
	// Requirements for a new Repository)
	CVMFS_DONT_CHECK_OVERLAYFS_VERSION?: #Bool
	// Use nanosecond-granularity for modification time of files
	// (instead of milliseconds).
	CVMFS_ENABLE_MTIME_NS?: #Bool
	// Set to true to cause exceeding *LIMIT variables to be fatal to a
	// publish instead of a warning.
	CVMFS_ENFORCE_LIMITS?: #Bool
	// Set to true to keep track of the volume of garbage collected
	// files (increases GC running time).
	CVMFS_EXTENDED_GC_STATS?: #Bool
	// Set to true to mark repository to contain external data that is
	// served from an external HTTP server.
	CVMFS_EXTERNAL_DATA?: #Bool
	// Maximum number of megabytes for a published file, default value:
	// 1024 (see also CVMFS_ENFORCE_LIMITS).
	CVMFS_FILE_MBYTE_LIMIT?: #PosInt
	// Enable/disable warning through wall and grace period before
	// forcefully remounting a CernVM-FS repository on the release
	// manager machine.
	CVMFS_FORCE_REMOUNT_WARNING?: #Bool
	// Enables repository garbage collection (Stratum 0 only, if set to
	// true).
	CVMFS_GARBAGE_COLLECTION: "true" | "false" | *""
	// Log file path to track all garbage collected objects during
	// sweeping for bookkeeping or debugging.
	CVMFS_GC_DELETION_LOG?: #AbsPath
	// Path to externally updated location of geolite2 city database,
	// or 'None' for no database.
	CVMFS_GEO_DB_FILE?: #AbsPath | =~"(?i)^none$"
	// A license key for downloading the geolite2 city database from
	// maxmind.
	CVMFS_GEO_LICENSE_KEY?: string
	// Path of a file for the mapping of file owner group ids.
	CVMFS_GID_MAP?: #AbsPath
	// Define which secure hash algorithm should be used by CernVM-FS
	// for CAS objects (supported are: sha1, rmd160 and shake128).
	CVMFS_HASH_ALGORITHM?: "sha1" | "rmd160" | "shake128"
	// Set to true to skip special files (pipes, sockets, block device
	// and character device files) during publish without aborting.
	CVMFS_IGNORE_SPECIAL_FILES?: #Bool
	// Set to true to process extended attributes.
	CVMFS_INCLUDE_XATTRS?: #Bool
	// Maximal size of a file chunk in bytes (see also
	// CVMFS_USE_FILE_CHUNKING).
	CVMFS_MAX_CHUNK_SIZE: #PosInt | *"16777216"
	// Maximal number of concurrently processed files during publishing.
	CVMFS_MAXIMAL_CONCURRENT_WRITES?: #PosInt
	// Minimal size of a file chunk in bytes (see also
	// CVMFS_USE_FILE_CHUNKING).
	CVMFS_MIN_CHUNK_SIZE: #PosInt | *"4194304"
	// Maximum thousands of files allowed in nested catalogs, default
	// 500 (see also CVMFS_ROOT_KCATALOG_LIMIT and CVMFS_ENFORCE_LIMITS).
	CVMFS_NESTED_KCATALOG_LIMIT?: #PosInt
	// Number of threads used to commit data to storage during
	// publication. Currently only used by the local backend.
	CVMFS_NUM_UPLOAD_TASKS?: #PosInt
	// Maximal number of concurrently downloaded files during a
	// Stratum1 pull operation (Stratum 1 only).
	CVMFS_NUM_WORKERS?: #PosInt
	// Colon-separated path to the public key file(s) or directory(ies)
	// of the repository to be replicated. (Stratum 1 only)
	CVMFS_PUBLIC_KEY?: #AbsPathList
	// Set to true to show publisher statistics on the console.
	CVMFS_PRINT_STATISTICS?: #Bool
	// Stratum1-only: Set to no to skip this repository when executing
	// `cvmfs_server snapshot -a`.
	CVMFS_REPLICA_ACTIVE: #Bool | *""
	// The fully qualified name of the specific repository.
	CVMFS_REPOSITORY_NAME: #FQRN
	// Defines if the repository is a master copy (stratum0) or a
	// replica (stratum1).
	CVMFS_REPOSITORY_TYPE: "stratum0" | "stratum1"
	// The frequency in seconds of client lookups for changes in the
	// repository. Defaults to 4 minutes.
	CVMFS_REPOSITORY_TTL?: #UInt
	// Maximum thousands of files allowed in root catalogs, default 200
	// (see also CVMFS_NESTED_KCATALOG_LIMIT and CVMFS_ENFORCE_LIMITS).
	CVMFS_ROOT_KCATALOG_LIMIT?: #PosInt
	// Group name for subset of repositories used with `cvmfs_server
	// snapshot -a -g`. Added with `cvmfs_server add-replica -g`.
	CVMFS_SNAPSHOT_GROUP?: =~"^[A-Za-z0-9_-]+$"
	// Location of the upstream spooler scratch directories; the
	// read-only CernVM-FS mount point and copy-on-write storage reside
	// here.
	CVMFS_SPOOL_DIR: #AbsPath
	// Set a custom path for the publisher statistics database.
	CVMFS_STATISTICS_DB?: #AbsPath
	// Sets the pruning interval for the publisher statistics database
	// (365 by default).
	CVMFS_STATS_DB_DAYS_TO_KEEP?: #UInt
	// URL of the master copy (stratum0) of this specific repository.
	CVMFS_STRATUM0: #URL
	// URL of the Stratum1 HTTP server for this specific repository.
	CVMFS_STRATUM1: #URL | *""
	// Controls how often sync will be called by cvmfs_server
	// operations. Possible levels are 'none', 'default', 'cautious'.
	CVMFS_SYNCFS_LEVEL?: "none" | "default" | "cautious"
	// S3 backend (see S3 Parameter table): CVMFS_S3_<param>
	CVMFS_S3_HOST?:                               #Host
	CVMFS_S3_PORT?:                               #Port
	CVMFS_S3_BUCKET?:                             string
	CVMFS_S3_REGION?:                             string
	CVMFS_S3_FLAVOR?:                             "azure" | "awsv2" | "awsv4"
	CVMFS_S3_ACCESS_KEY?:                         string
	CVMFS_S3_SECRET_KEY?:                         string
	CVMFS_S3_PROXY?:                              string
	CVMFS_S3_TIMEOUT?:                            #UInt
	CVMFS_S3_MAX_RETRIES?:                        #UInt
	CVMFS_S3_MAX_NUMBER_OF_PARALLEL_CONNECTIONS?: #PosInt
	CVMFS_S3_USE_HTTPS?:                          #Bool
	CVMFS_S3_PEEK_BEFORE_PUT?:                    #Bool
	CVMFS_S3_BATCH_DELETE?:                       #Bool
	CVMFS_S3_BATCH_DELETE_SIZE?:                  #PosInt
	CVMFS_S3_DNS_BUCKETS?:                        #Bool
	// Canned ACL applied to uploaded objects (upload_s3.cc).
	CVMFS_S3_X_AMZ_ACL?: "private" | "public-read" | "public-write" |
		"authenticated-read" | "aws-exec-read" | "bucket-owner-read" |
		"bucket-owner-full-control"
	// Path of a file for the mapping of file owner user ids.
	CVMFS_UID_MAP?: #AbsPath
	// Mount point of the union file system for copy-on-write semantics
	// of CernVM-FS. Here, changes to the repository are performed (see
	// CernVM-FS Repository Creation and Updating).
	CVMFS_UNION_DIR: #AbsPath
	// Defines the union file system to be used for the repository.
	// (only overlayfs is fully supported, aufs has no active support
	// anymore)
	CVMFS_UNION_FS_TYPE: "overlayfs" | "aufs"
	// Publish repository statistics data file to the Stratum 0 /stats
	// location.
	CVMFS_UPLOAD_STATS_DB?: #Bool
	// Publish repository statistics plots and webpage to the Stratum 0
	// /stats location (requires ROOT).
	CVMFS_UPLOAD_STATS_PLOTS?: #Bool
	// Upstream spooler description defining the basic upstream storage
	// type and configuration, one of (apx-parameters.md):
	//   local,<tmp dir>,<storage dir>
	//   s3,<tmp dir>,<repo entry URL>@<S3 config file>
	//   gw,<tmp dir>,<gateway endpoint URL>
	CVMFS_UPSTREAM_STORAGE: =~"^local,/[^,]+,/[^,]+$" |
		=~"(?i)^s3,/[^,]+,[^,@]+@/[^,]+$" |
		=~"(?i)^gw,/[^,]+,https?://[^,]+$"
	// Allows backend to split big files into small chunks (true |
	// false).
	CVMFS_USE_FILE_CHUNKING?: #Bool
	// The user name that owns and manipulates the files inside the
	// repository.
	CVMFS_USER: #UserName
	// Set to true to enable the hidden, virtual .cvmfs/snapshots
	// directory containing entry points to all named tags.
	CVMFS_VIRTUAL_DIR?: #Bool
	// Membership requirement (e.g. VOMS authentication) to be added
	// into the file catalogs.
	CVMFS_VOMS_AUTHZ?: string
	// Bundle file with CA certificates for HTTPS connections (see
	// Large-Scale Data CernVM-FS).
	X509_CERT_BUNDLE?: #AbsPath
	// Directory file with CA certificates for HTTPS connections,
	// defaults to /etc/grid-security/certificates (see Large-Scale
	// Data CernVM-FS).
	X509_CERT_DIR?: #AbsPath

	// --- Deprecated --------------------------------------------------
	// Deprecated, set to true to enable generation of whole-file
	// objects for large files.
	CVMFS_GENERATE_LEGACY_BULK_CHUNKS?: #Bool
	// Deprecated, defaults to true. If cross-directory hardlinks are
	// found, automatically break the hardlinks instead of aborting.
	CVMFS_IGNORE_XDIR_HARDLINKS?: #Bool

	// --- Undocumented ----------
	// PIN of the master key card, if used.
	CVMFS_MASTERKEYCARD_PIN?: string
	// Warn when a catalog has more entries than this threshold.
	CVMFS_CATALOG_ENTRY_WARN_THRESHOLD?: #UInt
	// Asynchronous cleanup of the scratch area after publishing.
	CVMFS_ASYNC_SCRATCH_CLEANUP?: #Bool
	// Use the catalog cache of the server (params.cc; undocumented).
	CVMFS_SERVER_USE_CATALOG_CACHE?: #Bool
	// Fuse passthrough for the server-side mount (undocumented).
	CVMFS_PASSTHROUGH?: #Bool
	// Publish CVMFS versions into the repository meta file (undocumented).
	CVMFS_PUBLISH_VERSIONS_IN_META_FILE?: #Bool
	// Batch size of the GC SQLite operations (undocumented).
	CVMFS_GC_DB_BATCH_SIZE?: #Int | *"1000"
	// Maximum parallel snapshots in snapshot -a.
	CVMFS_MAX_PARALLEL_SNAPSHOTS?: #PosInt
	// Partial replication of a repository; the scripts compare against
	// the literal string "true" (cvmfs_server_snapshot.sh; undocumented).
	CVMFS_PARTIAL_REPLICATION: "true" | "false" | *""
	// Path of the spec file for partial replication (undocumented).
	CVMFS_PARTIAL_REPLICATION_SPEC: #AbsPath | *""
	// HTTP timeout in seconds for replication downloads (undocumented).
	CVMFS_HTTP_TIMEOUT?: #UInt
	// HTTP retries for replication downloads (undocumented).
	CVMFS_HTTP_RETRIES?: #UInt
	// Proxy used by the server tools (params.cc; undocumented).
	CVMFS_SERVER_PROXY?: =~"(?i)^\(_#proxyEntry)$"
	// Account ID for the geolite2 download service.
	CVMFS_GEO_ACCOUNT_ID?: #UInt
	// Automatically update the geo database.
	CVMFS_GEO_AUTO_UPDATE?: #Bool
	// updategeo helper knobs (mount/serverorder.sh; undocumented).
	CVMFS_UPDATEGEO?:           string
	CVMFS_UPDATEGEO_DAY?:       #UInt
	CVMFS_UPDATEGEO_HOUR?:      #UInt
	CVMFS_UPDATEGEO_MINDAYS?:   #UInt
	CVMFS_UPDATEGEO_MAXDAYS?:   #UInt
	CVMFS_UPDATEGEO_DIR?:       #AbsPath
	CVMFS_UPDATEGEO_DB?:        string
	CVMFS_UPDATEGEO_OLDDB?:     string
	CVMFS_UPDATEGEO_SOURCE?:    "openhtc" | "maxmind" | "none" | "NONE"
	CVMFS_UPDATEGEO_URLBASE?:   #URL
	CVMFS_UPDATEGEO_URLSUFFIX?: string
	// Debug/developer knobs of the cvmfs_server script (undocumented).
	CVMFS_SERVER_DEBUG?:                    "0" | "1" | "2" | "3"
	CVMFS_SERVER_FLAGS?:                    string
	CVMFS_SERVER_PUBLISH?:                  string
	CVMFS_SERVER_PUBLISH_DEBUG?:            string
	CVMFS_SERVER_SWISSKNIFE?:               string
	CVMFS_SERVER_SWISSKNIFE_DEBUG?:         string
	CVMFS_SERVER_CHECK_SUMMARY?:            string
	CVMFS_SERVER_APACHE_RELOAD_IS_RESTART?: #Bool
	// Log level of server helper tools (undocumented).
	CVMFS_LOG_LEVEL?: string
	// Script-internal default holders (cvmfs_server_common.sh); the
	// authoritative values live in the parameters they seed.
	CVMFS_DEFAULT_AUTO_GC_LAPSE?:               #TimeSpan
	CVMFS_DEFAULT_AVG_CHUNK_SIZE?:              #PosInt
	CVMFS_DEFAULT_ENFORCE_LIMITS?:              #Bool
	CVMFS_DEFAULT_GENERATE_LEGACY_BULK_CHUNKS?: #Bool
	CVMFS_DEFAULT_MAX_CHUNK_SIZE?:              #PosInt
	CVMFS_DEFAULT_MIN_CHUNK_SIZE?:              #PosInt
	CVMFS_DEFAULT_USE_FILE_CHUNKING?:           #Bool

	// ===================================================================
	// Cross-parameter dependency rules
	// ===================================================================

	if CVMFS_AUTO_GC == "true" && CVMFS_REPOSITORY_TYPE == "stratum0" {
		_rule_auto_gc_requires_garbage_collection: true & (CVMFS_GARBAGE_COLLECTION == "true")
	}

	// Chunk size sanity: MIN <= AVG <= MAX (CUE-only rule, not expressible
	// in JSON Schema draft-7). Unset values compare against the defaults
	// cvmfs_server seeds on every publish.
	_rule_min_chunk_size_le_avg_chunk_size: true & (strconv.Atoi(CVMFS_MIN_CHUNK_SIZE) <= strconv.Atoi(CVMFS_AVG_CHUNK_SIZE))
	_rule_avg_chunk_size_le_max_chunk_size: true & (strconv.Atoi(CVMFS_AVG_CHUNK_SIZE) <= strconv.Atoi(CVMFS_MAX_CHUNK_SIZE))
	// Autocatalog underflow threshold must not exceed the overflow
	// threshold (catalog auto-balancing). CUE-only rule, as above.
	if CVMFS_AUTOCATALOGS_MIN_WEIGHT != "" && CVMFS_AUTOCATALOGS_MAX_WEIGHT != "" {
		_rule_autocatalogs_min_weight_le_max_weight: true & (strconv.Atoi(CVMFS_AUTOCATALOGS_MIN_WEIGHT) <= strconv.Atoi(CVMFS_AUTOCATALOGS_MAX_WEIGHT))
	}

	// Replicas always get their Stratum 1 URL written by `cvmfs_server
	// add-replica` (cvmfs_server_add_replica.sh).
	if CVMFS_REPOSITORY_TYPE == "stratum1" {
		_rule_stratum1_requires_stratum1_url: true & (CVMFS_STRATUM1 != "")
	}

	// CVMFS_REPLICA_ACTIVE is only evaluated on replicas
	// (cvmfs_server_common.sh: is_stratum1 gate).
	if CVMFS_REPLICA_ACTIVE != "" {
		_rule_replica_active_requires_stratum1: true & (CVMFS_REPOSITORY_TYPE == "stratum1")
	}

	// Partial replication dies without a spec file
	// (cvmfs_server_snapshot.sh).
	if CVMFS_PARTIAL_REPLICATION == "true" {
		_rule_partial_replication_requires_spec: true & (CVMFS_PARTIAL_REPLICATION_SPEC != "")
	}
}
