// CernVM-FS server configuration schema

// Field forms:
//   NAME: T             required
//   NAME?: T            optional
//   NAME: T | *default  optional; carries a default value

package cvmfs

#FQRN: =~"^[a-z0-9][a-z0-9.-]*\\.[a-z0-9-]+$"

#AbsPath:  =~"^/(?:[^/]+/)*[^/]*$"

// Colon-separated list of absolute paths (files or directories).
#AbsPathList: =~"^/(?:[^/:]+/)*[^/:]*(?::/(?:[^/:]+/)*[^/:]*)*$"

// Unsigned integer, 0 allowed.
#UInt: int & >=0

// Strictly positive integer.
#PosInt: int & >0

// Signed integer.
#Int: int

#IPv4: =~"^((25[0-5]|(2[0-4]|1\\d|[1-9]|)\\d)\\.?\\b){4}$"
#IPv6: =~"^((([0-9A-Fa-f]{1,4}:){7}[0-9A-Fa-f]{1,4}|([0-9A-Fa-f]{1,4}:){1,7}:|([0-9A-Fa-f]{1,4}:){1,6}:[0-9A-Fa-f]{1,4}|([0-9A-Fa-f]{1,4}:){1,5}(:[0-9A-Fa-f]{1,4}){1,2}|([0-9A-Fa-f]{1,4}:){1,4}(:[0-9A-Fa-f]{1,4}){1,3}|([0-9A-Fa-f]{1,4}:){1,3}(:[0-9A-Fa-f]{1,4}){1,4}|([0-9A-Fa-f]{1,4}:){1,2}(:[0-9A-Fa-f]{1,4}){1,5}|[0-9A-Fa-f]{1,4}:(:[0-9A-Fa-f]{1,4}){1,6}|:((:[0-9A-Fa-f]{1,4}){1,7}|:))|(([0-9A-Fa-f]{1,4}:){6}|::([0-9A-Fa-f]{1,4}:){0,5}|([0-9A-Fa-f]{1,4}:){1}:([0-9A-Fa-f]{1,4}:){0,4}|([0-9A-Fa-f]{1,4}:){2}:([0-9A-Fa-f]{1,4}:){0,3}|([0-9A-Fa-f]{1,4}:){3}:([0-9A-Fa-f]{1,4}:){0,2}|([0-9A-Fa-f]{1,4}:){4}:([0-9A-Fa-f]{1,4}:){0,1}|([0-9A-Fa-f]{1,4}:){5}:)((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9]))$"
#IP:   #IPv4 | #IPv6

// RFC 1123 hostname. A label may start with a digit, but the last one must
// be alphabetic, so dotted-decimal and bare numbers are not hostnames.
#Hostname: =~"^([A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?\\.)*[A-Za-z]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?$"
#Host:     #Hostname | #IP

// TCP port, 1-65535.
#Port: int & >=1 & <=65535

// Regex helper for accepting valid ports as strings ("1" up to "65535")
_#portRe: "([1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])"

// Single HTTP(S) URL; no whitespace or chain separators allowed.
#URL: =~"(?i)^https?://[^ \\t;|,]+$"

// DIRECT, auto (WPAD/PAC discovery) or an address with optional scheme/port.
_#proxyEntry: "(DIRECT|auto|(https?://)?[A-Za-z0-9._-]+(:\(_#portRe))?)"

// Date threshold as understood by `date -d`, e.g. "3 days ago".
#TimeSpan: =~"^[0-9]+ +(second|minute|hour|day|week|month|year)s? +ago$"

// POSIX user name.
#UserName: =~"^[A-Za-z_][A-Za-z0-9._-]*$"

// Server configuration

#ServerConfig: {
	CVMFS_AUTO_GC: bool | *false @description("Enables the automatic garbage collection on publish and snapshot.")
	CVMFS_AUTO_GC_TIMESPAN?: #TimeSpan @description("Date-threshold for automatic garbage collection (for example: \"3 days ago\", \"1 week ago\").")
	CVMFS_AUTO_GC_LAPSE?: #TimeSpan @description("Frequency of auto garbage collection, only garbage collect if last GC is before the given threshold (for example: \"1 day ago\").")
	CVMFS_AUTO_REPAIR_MOUNTPOINT?: bool @description("Set to true to enable automatic recovery from bogus server mount states.")
	CVMFS_AUTO_TAG?: bool @description("Creates a generic revision tag for each published revision (if set to true).")
	CVMFS_AUTO_TAG_TIMESPAN?: #TimeSpan @description("Date-threshold for automatic tags, after which auto tags get removed (for example: \"4 days ago\").")
	CVMFS_AUTOCATALOGS?: bool @description("Enable/disable automatic catalog management using autocatalogs.")
	CVMFS_AUTOCATALOGS_MAX_WEIGHT: #PosInt | *100000 @description("Maximum number of entries in an autocatalog to be considered overflowed. Default value: 100000 (see also CVMFS_AUTOCATALOGS).")
	CVMFS_AUTOCATALOGS_MIN_WEIGHT: #PosInt | *1000 @description("Minimum number of entries in an autocatalog to be considered underflowed. Default value: 1000 (see also CVMFS_AUTOCATALOGS).")
	CVMFS_AVG_CHUNK_SIZE: #PosInt | *8388608 @description("Desired Average size of a file chunk in bytes (see also CVMFS_USE_FILE_CHUNKING).")
	CVMFS_CATALOG_ALT_PATHS?: bool @description("Enable/disable generation of catalog bootstrapping shortcuts during publishing. (Useful when backend directory /data is not publicly accessible)")
	CVMFS_CHECK_ALL_MIN_DAYS: #UInt | *30 @description("Minimum number of days between checking each repository with `cvmfs_server check -a`. Default value: 30.")
	CVMFS_COMPRESSION_ALGORITHM?: "default" | "zlib" | "none" @description("Compression algorithm to be used during publishing (currently either 'default' or 'none'; \"zlib\" is accepted as an alias of \"default\", compression.cc).")
	CVMFS_CREATOR_VERSION: =~"^[0-9]+(\\.[0-9]+(\\.[0-9]+)?)?(-[0-9]+)?$" @description("The CernVM-FS version that was used to create this repository (do not change manually).")
	CVMFS_DONT_CHECK_OVERLAYFS_VERSION?: string @description("Disable checking of OverlayFS version before usage. (see Requirements for a new Repository) Presence-only; any value disables the check (cvmfs_server_util.sh).")
	CVMFS_ENABLE_MTIME_NS?: bool @description("Use nanosecond-granularity for modification time of files (instead of milliseconds).")
	CVMFS_ENFORCE_LIMITS?: bool @description("Set to true to cause exceeding *LIMIT variables to be fatal to a publish instead of a warning.")
	CVMFS_EXTENDED_GC_STATS?: bool @description("Set to true to keep track of the volume of garbage collected files (increases GC running time).")
	CVMFS_EXTERNAL_DATA?: bool @description("Set to true to mark repository to contain external data that is served from an external HTTP server.")
	CVMFS_FILE_MBYTE_LIMIT: #PosInt | *1024 @description("Maximum number of megabytes for a published file, default value: 1024 (see also CVMFS_ENFORCE_LIMITS).")
	CVMFS_FORCE_REMOUNT_WARNING?: bool @description("Enable/disable warning through wall and grace period before forcefully remounting a CernVM-FS repository on the release manager machine.")
	CVMFS_GARBAGE_COLLECTION: "true" | "false" | *"" @description("Enables repository garbage collection (Stratum 0 only, if set to true).")
	CVMFS_GC_DELETION_LOG?: #AbsPath @description("Log file path to track all garbage collected objects during sweeping for bookkeeping or debugging.")
	CVMFS_GEO_DB_FILE?: #AbsPath | =~"(?i)^none$" @description("Path to externally updated location of geolite2 city database, or 'None' for no database.")
	CVMFS_GEO_LICENSE_KEY?: string @description("A license key for downloading the geolite2 city database from maxmind.")
	CVMFS_GID_MAP?: #AbsPath @description("Path of a file for the mapping of file owner group ids.")
	CVMFS_HASH_ALGORITHM?: "sha1" | "rmd160" | "shake128" @description("Define which secure hash algorithm should be used by CernVM-FS for CAS objects (supported are: sha1, rmd160 and shake128).")
	CVMFS_IGNORE_SPECIAL_FILES?: bool @description("Set to true to skip special files (pipes, sockets, block device and character device files) during publish without aborting.")
	CVMFS_INCLUDE_XATTRS?: bool @description("Set to true to process extended attributes.")
	CVMFS_MAX_CHUNK_SIZE: #PosInt | *16777216 @description("Maximal size of a file chunk in bytes (see also CVMFS_USE_FILE_CHUNKING).")
	CVMFS_MAXIMAL_CONCURRENT_WRITES?: #PosInt @description("Maximal number of concurrently processed files during publishing.")
	CVMFS_MIN_CHUNK_SIZE: #PosInt | *4194304 @description("Minimal size of a file chunk in bytes (see also CVMFS_USE_FILE_CHUNKING).")
	CVMFS_NESTED_KCATALOG_LIMIT?: #PosInt @description("Maximum thousands of files allowed in nested catalogs, default 500 (see also CVMFS_ROOT_KCATALOG_LIMIT and CVMFS_ENFORCE_LIMITS).")
	CVMFS_NUM_UPLOAD_TASKS?: #PosInt @description("Number of threads used to commit data to storage during publication. Currently only used by the local backend.")
	CVMFS_NUM_WORKERS?: #PosInt @description("Maximal number of concurrently downloaded files during a Stratum1 pull operation (Stratum 1 only).")
	CVMFS_PUBLIC_KEY?: #AbsPathList @description("Colon-separated path to the public key file(s) or directory(ies) of the repository to be replicated. (Stratum 1 only)")
	CVMFS_PRINT_STATISTICS?: bool @description("Set to true to show publisher statistics on the console.")
	CVMFS_REPLICA_ACTIVE: bool | *null @description("Stratum1-only: Set to no to skip this repository when executing `cvmfs_server snapshot -a`.")
	CVMFS_REPOSITORY_NAME: #FQRN @description("The fully qualified name of the specific repository.")
	CVMFS_REPOSITORY_TYPE: "stratum0" | "stratum1" @description("Defines if the repository is a master copy (stratum0) or a replica (stratum1).")
	CVMFS_REPOSITORY_TTL?: #UInt @description("The frequency in seconds of client lookups for changes in the repository. Defaults to 4 minutes.")
	CVMFS_ROOT_KCATALOG_LIMIT?: #PosInt @description("Maximum thousands of files allowed in root catalogs, default 200 (see also CVMFS_NESTED_KCATALOG_LIMIT and CVMFS_ENFORCE_LIMITS).")
	CVMFS_SNAPSHOT_GROUP?: =~"^[A-Za-z0-9_-]+$" @description("Group name for subset of repositories used with `cvmfs_server snapshot -a -g`. Added with `cvmfs_server add-replica -g`.")
	CVMFS_SPOOL_DIR: #AbsPath @description("Location of the upstream spooler scratch directories; the read-only CernVM-FS mount point and copy-on-write storage reside here.")
	CVMFS_STATISTICS_DB?: #AbsPath @description("Set a custom path for the publisher statistics database.")
	CVMFS_STATS_DB_DAYS_TO_KEEP: #UInt | *365 @description("Sets the pruning interval for the publisher statistics database (365 by default).")
	CVMFS_STRATUM0: #URL @description("URL of the master copy (stratum0) of this specific repository.")
	CVMFS_STRATUM1: #URL | *"" @description("URL of the Stratum1 HTTP server for this specific repository.")
	CVMFS_SYNCFS_LEVEL?: "none" | "default" | "cautious" @description("Controls how often sync will be called by cvmfs_server operations. Possible levels are 'none', 'default', 'cautious'.")
	CVMFS_S3_HOST?:                               #Host @description("S3 server hostname, e.g. s3.amazonaws.com; must not be prefixed by http://.")
	CVMFS_S3_PORT?:                               #Port @description("Port on which the S3 instance is running.")
	CVMFS_S3_BUCKET?:                             string @description("S3 bucket name; the repository name is used as a subdirectory inside the bucket.")
	CVMFS_S3_REGION?:                             string @description("S3 region, e.g. eu-central-1; if set, the AWSv4 authorization protocol is used.")
	CVMFS_S3_FLAVOR?:                             "azure" | "awsv2" | "awsv4" @description("Authorization flavor of the storage backend: azure, awsv2 or awsv4 (upload_s3.cc).")
	CVMFS_S3_ACCESS_KEY?:                         string @description("S3 account access key.")
	CVMFS_S3_SECRET_KEY?:                         string @description("S3 account secret key.")
	CVMFS_S3_PROXY?:                              string @description("Proxy used for the connection to the S3 server.")
	CVMFS_S3_TIMEOUT?:                            #UInt @description("Timeout in seconds for the connection to the S3 server.")
	CVMFS_S3_MAX_RETRIES?:                        #UInt @description("Number of retries for the connection to the S3 server.")
	CVMFS_S3_MAX_NUMBER_OF_PARALLEL_CONNECTIONS?: #PosInt @description("Number of parallel uploads to the S3 server, e.g. 400.")
	CVMFS_S3_USE_HTTPS?:                          bool @description("Connect to the S3 implementation over HTTPS instead of HTTP.")
	CVMFS_S3_PEEK_BEFORE_PUT?:                    bool @description("Make PUT requests conditional on a prior HEAD request. Enabled by default.")
	CVMFS_S3_BATCH_DELETE?:                       bool @description("Remove objects with the S3 multi-object DELETE request (upload_s3.cc).")
	CVMFS_S3_BATCH_DELETE_SIZE?:                  #PosInt @description("Number of objects per multi-object DELETE request; clamped to the S3 limit (upload_s3.cc).")
	CVMFS_S3_DNS_BUCKETS?:                        bool @description("Set to false to disable DNS-style bucket URLs. Enabled by default.")
	CVMFS_S3_X_AMZ_ACL?: "private" | "public-read" | "public-write" |
		"authenticated-read" | "aws-exec-read" | "bucket-owner-read" |
		"bucket-owner-full-control" @description("Canned access control list (ACL) sent with uploaded objects.")
	CVMFS_UID_MAP?: #AbsPath @description("Path of a file for the mapping of file owner user ids.")
	CVMFS_UNION_DIR: #AbsPath @description("Mount point of the union file system for copy-on-write semantics of CernVM-FS. Here, changes to the repository are performed (see CernVM-FS Repository Creation and Updating).")
	CVMFS_UNION_FS_TYPE: "overlayfs" | "aufs" @description("Defines the union file system to be used for the repository. (only overlayfs is fully supported, aufs has no active support anymore)")
	CVMFS_UPLOAD_STATS_DB?: bool @description("Publish repository statistics data file to the Stratum 0 /stats location.")
	CVMFS_UPLOAD_STATS_PLOTS?: bool @description("Publish repository statistics plots and webpage to the Stratum 0 /stats location (requires ROOT).")
	CVMFS_UPSTREAM_STORAGE: =~"^local,/[^,]+,/[^,]+$" |
		=~"(?i)^s3,/[^,]+,[^,@]+@/[^,]+$" |
		=~"(?i)^gw,/[^,]+,https?://[^,]+$" @description("Upstream spooler description defining the basic upstream storage type and configuration, one of (apx-parameters.md): local,<tmp dir>,<storage dir> s3,<tmp dir>,<repo entry URL>@<S3 config file> gw,<tmp dir>,<gateway endpoint URL>")
	CVMFS_USE_FILE_CHUNKING?: bool @description("Allows backend to split big files into small chunks (true | false).")
	CVMFS_USER: #UserName @description("The user name that owns and manipulates the files inside the repository.")
	CVMFS_VIRTUAL_DIR?: bool @description("Set to true to enable the hidden, virtual .cvmfs/snapshots directory containing entry points to all named tags.")
	CVMFS_VOMS_AUTHZ?: string @description("Membership requirement (e.g. VOMS authentication) to be added into the file catalogs.")
	X509_CERT_BUNDLE?: #AbsPath @description("Bundle file with CA certificates for HTTPS connections (see Large-Scale Data CernVM-FS).")
	X509_CERT_DIR?: #AbsPath @description("Directory file with CA certificates for HTTPS connections, defaults to /etc/grid-security/certificates (see Large-Scale Data CernVM-FS).")

	// Deprecated
	CVMFS_GENERATE_LEGACY_BULK_CHUNKS?: bool @description("Deprecated, set to true to enable generation of whole-file objects for large files.")
	CVMFS_IGNORE_XDIR_HARDLINKS?: bool @description("Deprecated, defaults to true. If cross-directory hardlinks are found, automatically break the hardlinks instead of aborting.")

	// Dead: no longer read by any code
	CVMFS_AUFS_WARNING?: bool @description("The AUFS kernel-deadlock check this guarded was removed in commit 04be280b5 (\"Remove: potential kernel deadlock warning\", 2015). No read site remains anywhere in cvmfs/ or mount/; setting this parameter has no effect.")
	CVMFS_CATALOG_ENTRY_WARN_THRESHOLD?: #UInt @description("The mechanism this fed (catalog_entry_warn_threshold in swissknife sync, the -j flag, CheckParams' \">10000\" validation) was replaced by CVMFS_ROOT_KCATALOG_LIMIT / CVMFS_NESTED_KCATALOG_LIMIT in commit 87bb14c9e (\"change warn threshold to limits\", 2017); -j was later reused for an unrelated switch (CVMFS_ENABLE_MTIME_NS). Setting this parameter has no effect.")

	// Undocumented
	CVMFS_MASTERKEYCARD_PIN?: string @description("PIN of the master key card, if used.")
	CVMFS_ASYNC_SCRATCH_CLEANUP?: bool @description("Asynchronous cleanup of the scratch area after publishing.")
	CVMFS_SERVER_USE_CATALOG_CACHE?: bool @description("Use the catalog cache of the server (params.cc; undocumented).")
	CVMFS_PASSTHROUGH?: bool @description("Fuse passthrough for the server-side mount (undocumented).")
	CVMFS_PUBLISH_VERSIONS_IN_META_FILE?: bool @description("Publish CVMFS versions into the repository meta file (undocumented).")
	CVMFS_GC_DB_BATCH_SIZE: #Int | *1000 @description("Batch size of the GC SQLite operations (undocumented).")
	CVMFS_MAX_PARALLEL_SNAPSHOTS?: #PosInt @description("Maximum parallel snapshots in snapshot -a.")
	CVMFS_PARTIAL_REPLICATION: "true" | "false" | *"" @description("Partial replication of a repository; the scripts compare against the literal string \"true\" (cvmfs_server_snapshot.sh; undocumented).")
	CVMFS_PARTIAL_REPLICATION_SPEC: #AbsPath | *"" @description("Path of the spec file for partial replication (undocumented).")
	CVMFS_HTTP_TIMEOUT?: #UInt @description("HTTP timeout in seconds for replication downloads (undocumented).")
	CVMFS_HTTP_RETRIES?: #UInt @description("HTTP retries for replication downloads (undocumented).")
	CVMFS_SERVER_PROXY?: =~"(?i)^\(_#proxyEntry)$" @description("Proxy used by the server tools (params.cc; undocumented).")
	CVMFS_GEO_ACCOUNT_ID?: #UInt @description("Account ID for the geolite2 download service.")
	CVMFS_GEO_AUTO_UPDATE?: bool @description("Automatically update the geo database.")
	CVMFS_UPDATEGEO?:           string @description("Not a parameter of its own; the geo database update is configured through the CVMFS_UPDATEGEO_* parameters below.")
	CVMFS_UPDATEGEO_DAY?:       #UInt @description("Weekday of the geo database update, 0-6 where 0 is Sunday (default Tuesday).")
	CVMFS_UPDATEGEO_HOUR?:      #UInt @description("First hour of the day for the geo database update, 0-23 (default 10).")
	CVMFS_UPDATEGEO_MINDAYS?:   #UInt @description("Minimum number of days between geo database update attempts.")
	CVMFS_UPDATEGEO_MAXDAYS?:   #UInt @description("Age in days after which a geo database update is considered urgent.")
	CVMFS_UPDATEGEO_DIR?:       #AbsPath @description("Directory holding the geo database.")
	CVMFS_UPDATEGEO_DB?:        string @description("File name of the geo database.")
	CVMFS_UPDATEGEO_OLDDB?:     string @description("File name of a previous geo database, removed if still present.")
	CVMFS_UPDATEGEO_SOURCE?:    "openhtc" | "maxmind" | "none" | "NONE" @description("Source the geo database is downloaded from: openhtc, maxmind or none.")
	CVMFS_UPDATEGEO_URLBASE?:   #URL @description("Base of the URL the geo database is downloaded from.")
	CVMFS_UPDATEGEO_URLSUFFIX?: string @description("Suffix appended to CVMFS_UPDATEGEO_URLBASE to form the download URL.")
	CVMFS_SERVER_DEBUG?:                    0 | 1 | 2 | 3 @description("Debug level of the cvmfs_server script; 1-3 select the debug binaries and gdb wrappers (cvmfs_server_coda.sh).")
	CVMFS_SERVER_FLAGS?:                    string @description("Additional command line switches passed to the garbage collection call (cvmfs_server_gc.sh).")
	CVMFS_SERVER_PUBLISH?:                  string @description("Path of the cvmfs_publish binary used by the server tools.")
	CVMFS_SERVER_PUBLISH_DEBUG?:            string @description("Command used instead of CVMFS_SERVER_PUBLISH when a debug level is set.")
	CVMFS_SERVER_SWISSKNIFE?:               string @description("Name of the cvmfs_swissknife binary used by the server tools.")
	CVMFS_SERVER_SWISSKNIFE_DEBUG?:         string @description("Command used instead of CVMFS_SERVER_SWISSKNIFE when a debug level is set.")
	CVMFS_SERVER_CHECK_SUMMARY?:            string @description("Set to 0 to restore the output cvmfs_server check produced before it summarised its findings.")
	CVMFS_SERVER_APACHE_RELOAD_IS_RESTART?: bool @description("Set to true to restart Apache where the server tools would otherwise reload it.")
	CVMFS_LOG_LEVEL?: string @description("Log level of server helper tools (undocumented).")
	CVMFS_DEFAULT_AUTO_GC_LAPSE?:               #TimeSpan @description("Built-in default for CVMFS_AUTO_GC_LAPSE, used when the repository sets none (cvmfs_server_coda.sh).")
	CVMFS_DEFAULT_AVG_CHUNK_SIZE?:              #PosInt @description("Built-in default for CVMFS_AVG_CHUNK_SIZE, used when the repository sets none (cvmfs_server_coda.sh).")
	CVMFS_DEFAULT_ENFORCE_LIMITS?:              bool @description("Built-in default for CVMFS_ENFORCE_LIMITS, used when the repository sets none (cvmfs_server_coda.sh).")
	CVMFS_DEFAULT_GENERATE_LEGACY_BULK_CHUNKS?: bool @description("Built-in default for CVMFS_GENERATE_LEGACY_BULK_CHUNKS, used when the repository sets none (cvmfs_server_coda.sh).")
	CVMFS_DEFAULT_MAX_CHUNK_SIZE?:              #PosInt @description("Built-in default for CVMFS_MAX_CHUNK_SIZE, used when the repository sets none (cvmfs_server_coda.sh).")
	CVMFS_DEFAULT_MIN_CHUNK_SIZE?:              #PosInt @description("Built-in default for CVMFS_MIN_CHUNK_SIZE, used when the repository sets none (cvmfs_server_coda.sh).")
	CVMFS_DEFAULT_USE_FILE_CHUNKING?:           bool @description("Built-in default for CVMFS_USE_FILE_CHUNKING, used when the repository sets none (cvmfs_server_coda.sh).")

	// Cross-parameter dependency rules

	if CVMFS_AUTO_GC && CVMFS_REPOSITORY_TYPE == "stratum0" {
		_rule_auto_gc_requires_garbage_collection: true & (CVMFS_GARBAGE_COLLECTION == "true")
	}

	// Chunk size sanity: MIN <= AVG <= MAX. Cross-parameter comparisons like
	// these are the reason for CUE: JSON Schema draft-7 cannot express them.
	_rule_min_chunk_size_le_avg_chunk_size: true & (CVMFS_MIN_CHUNK_SIZE <= CVMFS_AVG_CHUNK_SIZE)
	_rule_avg_chunk_size_le_max_chunk_size: true & (CVMFS_AVG_CHUNK_SIZE <= CVMFS_MAX_CHUNK_SIZE)
	// Autocatalog underflow threshold must not exceed the overflow one.
	_rule_autocatalogs_min_weight_le_max_weight: true & (CVMFS_AUTOCATALOGS_MIN_WEIGHT <= CVMFS_AUTOCATALOGS_MAX_WEIGHT)

	// `cvmfs_server add-replica` always writes the Stratum 1 URL.
	if CVMFS_REPOSITORY_TYPE == "stratum1" {
		_rule_stratum1_requires_stratum1_url: true & (CVMFS_STRATUM1 != "")
	}

	// Only evaluated on replicas.
	if CVMFS_REPLICA_ACTIVE != null {
		_rule_replica_active_requires_stratum1: true & (CVMFS_REPOSITORY_TYPE == "stratum1")
	}

	// Partial replication dies without a spec file.
	if CVMFS_PARTIAL_REPLICATION == "true" {
		_rule_partial_replication_requires_spec: true & (CVMFS_PARTIAL_REPLICATION_SPEC != "")
	}
}
