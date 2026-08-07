// CernVM-FS client configuration schema

// Field forms:
//   NAME: T             required
//   NAME?: T            optional
//   NAME: T | *default  optional; carries a default because a dependency rule reads it
//                       ("", 0 and false stand for "unset"; null where an
//                       explicitly configured false must stay distinguishable)

package cvmfs

// Switch parameters are booleans; the validator accepts yes/on/1/true and
// no/off/0/false (case-insensitively) and maps them to true/false.

#FQRN: =~"^[a-z0-9][a-z0-9.-]*\\.[a-z0-9-]+$"

// Comma-separated list of fully qualified repository names.
#FQRNList: =~"^[a-z0-9][a-z0-9.-]*\\.[a-z0-9-]+(,[a-z0-9][a-z0-9.-]*\\.[a-z0-9-]+)*$"
#AbsPath:  =~"^/"

// Colon-separated list of absolute paths (files or directories).
#AbsPathList: =~"^/[^:]*(:/[^:]*)*$"

// Unsigned integer, 0 allowed.
#UInt: int & >=0

// Strictly positive integer.
#PosInt: int & >0

// Comma-separated list of unsigned integers.
#UIntList: =~"^[0-9]+(,[0-9]+)*$"

#IPv4: =~"^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])$"
#IPv6: =~"^(([0-9A-Fa-f]{1,4}:){7}[0-9A-Fa-f]{1,4}|([0-9A-Fa-f]{1,4}:){1,7}:|([0-9A-Fa-f]{1,4}:){1,6}:[0-9A-Fa-f]{1,4}|([0-9A-Fa-f]{1,4}:){1,5}(:[0-9A-Fa-f]{1,4}){1,2}|([0-9A-Fa-f]{1,4}:){1,4}(:[0-9A-Fa-f]{1,4}){1,3}|([0-9A-Fa-f]{1,4}:){1,3}(:[0-9A-Fa-f]{1,4}){1,4}|([0-9A-Fa-f]{1,4}:){1,2}(:[0-9A-Fa-f]{1,4}){1,5}|[0-9A-Fa-f]{1,4}:(:[0-9A-Fa-f]{1,4}){1,6}|:((:[0-9A-Fa-f]{1,4}){1,7}|:))$"
#IP:   #IPv4 | #IPv6

// RFC 1123 hostname (also matches IPv4 literals).
#Hostname: =~"^[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?(\\.[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*$"
#Host:     #Hostname | #IP

// TCP port, 1-65535.
#Port: int & >=1 & <=65535

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
#QuotaLimit: int & >=-1

// ===========================================================================
// Client configuration
// ===========================================================================

#ClientConfig: {
	// If set, use an alien cache at the given location.
	CVMFS_ALIEN_CACHE: #AbsPath | *""
	// If set to yes, use alternative root catalog path. Only required
	// for fixed catalogs (tag / hash) under the alternative path.
	CVMFS_ALT_ROOT_PATH: bool | *false
	// Automatically set by CVMFS to reflect the CPU architecture on
	// which the client runs (using uname -m). Allows to utilize variant
	// symlinks with cvmfs installations to auto-select the architecture.
	CVMFS_ARCH?: string
	// Client version string (e.g. "2.14.0"); read-only, injected by
	// `cvmfs2 -o parse`.
	CVMFS_VERSION?: string
	// Client version as a single comparable integer; read-only, injected
	// by `cvmfs2 -o parse`.
	CVMFS_VERSION_NUMERIC?: #UInt
	// If set to no, disables the automatic update of file catalogs.
	CVMFS_AUTO_UPDATE: bool | *false
	// Full path to an authz helper, overwrites the helper hint in the
	// catalog.
	CVMFS_AUTHZ_HELPER?: #AbsPath
	// Full path to the directory that contains the authz helpers.
	CVMFS_AUTHZ_SEARCH_PATH?: #AbsPath
	// Any other CVMFS_AUTHZ_<name> variable is forwarded to the spawned
	// external authz helper with the CVMFS_AUTHZ_ prefix stripped
	// (authz_fetch.cc: AuthzExternalFetcher's GetEnvironmentSubset call);
	// site-specific helper credentials/config go here.
	[=~"^CVMFS_AUTHZ_[A-Za-z0-9_]+$"]: string
	// Seconds for the maximum initial backoff when retrying to
	// download data.
	CVMFS_BACKOFF_INIT: #UInt | *2
	// Maximum backoff in seconds when retrying to download data.
	CVMFS_BACKOFF_MAX: #UInt | *10
	// File name of the blacklist that denies mounting any revision <
	// revision N. Format: <REPO N where REPO is the repository name, N
	// is the revision number, and the two parts are separated by
	// whitespace. Note: no extra characters are allowed after N, not
	// even whitespace.
	CVMFS_BLACKLIST?: #AbsPath
	// Try to release pinned catalogs when their number surpasses the
	// given watermark. Defaults to 1/4 CVMFS_NFILES; explicitly set by
	// shrinkwrap.
	CVMFS_CATALOG_WATERMARK?: #UInt
	// Deprecated, legacy parameter. Use CVMFS_ALIEN_CACHE instead.
	CVMFS_CACHE_ALIEN: #AbsPath | *""
	// Location (directory) of the CernVM-FS cache.
	CVMFS_CACHE_BASE: #AbsPath
	// Similar to CVMFS_CACHE_BASE, but automatically set by cvmfs. Only
	// might need manual overwriting when using libcvmfs.
	CVMFS_CACHE_DIR: #AbsPath | *""
	// Type of cache to use. By default it is posix. (see also Advanced
	// Cache Configuration)
	CVMFS_CACHE_PRIMARY?: =~"^[A-Za-z0-9_]+$"
	// If set to yes, deduplicate open file descriptors by refcounting.
	CVMFS_CACHE_REFCOUNT: bool | *false
	// Prefer evicting files that are not currently open; only effective
	// together with CVMFS_CACHE_REFCOUNT (mountpoint.cc).
	CVMFS_CACHE_CLEANUP_NONOPENLRU: bool | *false
	// Generic aliases of the default cache instance parameters
	// (mountpoint.cc: MkCacheParm); they take precedence over the
	// compatibility names CVMFS_SHARED_CACHE, CVMFS_QUOTA_LIMIT and
	// CVMFS_SERVER_CACHE_MODE.
	CVMFS_CACHE_SHARED?:      bool
	CVMFS_CACHE_QUOTA_LIMIT?: #QuotaLimit
	CVMFS_CACHE_SERVER_MODE?: bool
	CVMFS_CACHE_WORKSPACE?:   #AbsPath

	// --- Cache instance families: CVMFS_CACHE_<name>_<param> ----------
	// Instance names are 1-24 characters of [A-Za-z0-9_] (mountpoint.cc:
	// CheckInstanceName); parameters are named CVMFS_CACHE_<name>_<param>
	// (mountpoint.cc: MkCacheParm, SetupCacheMgr and friends).
	[=~"^CVMFS_CACHE_[A-Za-z0-9_]{1,24}_TYPE$"]: "posix" | "ram" | "tiered" | "external"
	// Posix cache instance parameters.
	[=~"^CVMFS_CACHE_[A-Za-z0-9_]{1,24}_(BASE|DIR|ALIEN|WORKSPACE)$"]: #AbsPath
	[=~"^CVMFS_CACHE_[A-Za-z0-9_]{1,24}_(SHARED|REFCOUNT|SERVER_MODE|CLEANUP_NONOPENLRU|SYMLINKS)$"]: bool
	[=~"^CVMFS_CACHE_[A-Za-z0-9_]{1,24}_QUOTA_LIMIT$"]: #QuotaLimit
	// RAM cache instance: size in MB, or a percentage of system memory.
	[=~"^CVMFS_CACHE_[A-Za-z0-9_]{1,24}_SIZE$"]:   (int & >=0) | =~"^[0-9]+%$"
	[=~"^CVMFS_CACHE_[A-Za-z0-9_]{1,24}_MALLOC$"]: "libc" | "heap"
	// Tiered cache instance: names of the upper/lower layer instances.
	[=~"^CVMFS_CACHE_[A-Za-z0-9_]{1,24}_(UPPER|LOWER)$"]:  =~"^[A-Za-z0-9_]{1,24}$"
	[=~"^CVMFS_CACHE_[A-Za-z0-9_]{1,24}_LOWER_READONLY$"]: bool
	// External cache plugin instance: socket locator and optional
	// comma-separated plugin command line.
	[=~"^CVMFS_CACHE_[A-Za-z0-9_]{1,24}_LOCATOR$"]: string
	[=~"^CVMFS_CACHE_[A-Za-z0-9_]{1,24}_CMDLINE$"]: string
	// If set to yes, enables symlink caching in the kernel.
	CVMFS_CACHE_SYMLINKS?: bool
	// If set to no, disable checking of file ownership and permissions
	// (open all files).
	CVMFS_CHECK_PERMISSIONS: bool
	// If set to yes, allows CernVM-FS to claim ownership of files and
	// directories.
	CVMFS_CLAIM_OWNERSHIP?: bool
	// CVMFS repository where a CVMFS client will get its config from.
	// The default configuration rpm cvmfs-config-default sets this
	// parameter to cvmfs-config.cern.ch
	CVMFS_CONFIG_REPOSITORY?: #FQRN
	// Comma-separated list to set CPU affinity for all cvmfs components.
	CVMFS_CPU_AFFINITY?: #UIntList
	// If set, run CernVM-FS in debug mode and write a verbose log to
	// the specified file.
	CVMFS_DEBUGLOG?: #AbsPath
	// The default domain will be automatically appended to repository
	// names when given without a domain.
	CVMFS_DEFAULT_DOMAIN?: #Hostname
	// Minimum effective TTL in seconds for DNS queries of proxy server
	// names (not Stratum 1s). Defaults to 1 minute.
	CVMFS_DNS_MIN_TTL: #UInt | *60
	// Maximum effective TTL in seconds for DNS queries of proxy server
	// names (not Stratum 1s). Defaults to 1 day.
	CVMFS_DNS_MAX_TTL: #UInt | *86400
	// Number of retries when resolving proxy names.
	CVMFS_DNS_RETRIES?: #UInt
	// IP of the DNS server CVMFS should use.
	CVMFS_DNS_SERVER?: #IP
	// Timeout in seconds when resolving proxy names.
	CVMFS_DNS_TIMEOUT?: #UInt
	// If true, watch /etc/resolv.conf for nameserver changes.
	CVMFS_DNS_ROAMING?: bool
	// Enforce POSIX ACLs stored in the repository. Requires libfuse 3.
	CVMFS_ENFORCE_ACLS?: bool
	// List of HTTP proxies similar to CVMFS_EXTERNAL_HTTP_PROXY. The
	// fallback proxies are added to the end of the normal proxies, and
	// disable DIRECT connections.
	CVMFS_EXTERNAL_FALLBACK_PROXY?: #ProxyChain
	// Chain of HTTP proxy groups to be used when CernVM-FS is accessing
	// external data.
	CVMFS_EXTERNAL_HTTP_PROXY?: #ProxyChain
	// Caps the list of external hosts to the given number (after
	// geo-sorting them).
	CVMFS_EXTERNAL_MAX_SERVERS?: #UInt
	// Semi-colon-separated chain of RFC6249-compliant servers to
	// locate webservers serving external data.
	CVMFS_EXTERNAL_METALINK?: #URLChain
	// Timeout in seconds for HTTP requests to an external-data server
	// with a proxy server.
	CVMFS_EXTERNAL_TIMEOUT?: #UInt
	// Timeout in seconds for HTTP requests to an external-data server
	// without a proxy server.
	CVMFS_EXTERNAL_TIMEOUT_DIRECT?: #UInt
	// Semicolon-separated chain of webservers serving external data
	// chunks.
	CVMFS_EXTERNAL_URL?: #URLChain
	// List of HTTP proxies similar to CVMFS_HTTP_PROXY. The fallback
	// proxies are added to the end of the normal proxies, and disable
	// DIRECT connections.
	CVMFS_FALLBACK_PROXY?: #ProxyChain
	// Disable fuse notify invalidation. By default disabled on macOS
	// to fix stability issues. On Linux systems, it is NOT recommended
	// to turn it off.
	CVMFS_FUSE_NOTIFY_INVALIDATION?: bool
	// Set max number of fuse threads (requires: libfuse3 > 3.12).
	CVMFS_FUSE3_MAX_THREADS?: #UInt
	// Set max number of idle fuse threads (requires: libfuse3 > 3.12).
	CVMFS_FUSE3_IDLE_THREADS?: #UInt
	// When set to yes, follow up to 4 HTTP redirects in requests.
	CVMFS_FOLLOW_REDIRECTS?: bool
	// If set to yes the client will not expose CernVM-FS specific
	// extended attributes.
	CVMFS_HIDE_MAGIC_XATTRS?: bool
	// See CVMFS_PROXY_RESET_AFTER, for server URLs.
	CVMFS_HOST_RESET_AFTER?: #UInt
	// Chain of HTTP proxy groups used by CernVM-FS. Necessary. Set to
	// DIRECT if you don't use proxies.
	CVMFS_HTTP_PROXY: #ProxyChain
	// Activates that a tracing header is attached to each CURL
	// request. Consists of uid, pid, and gid. Default is off.
	CVMFS_HTTP_TRACING: bool | *false
	// Adds additional static, user-defined tracing headers. Format:
	// key1:val1|key2:val2|key3:val3. Needs CVMFS_HTTP_TRACING to be set
	// to on.
	CVMFS_HTTP_TRACING_HEADERS: =~"^[^:|]+:[^|]*(\\|[^:|]+:[^|]*)*$" | *""
	// When set to yes, don't verify CernVM-FS file catalog signatures.
	// No effect; see _CVMFS_DEVEL_IGNORE_SIGNATURE_FAILURES below.
	CVMFS_IGNORE_SIGNATURE?: bool
	// Initial inode generation. Used for testing.
	CVMFS_INITIAL_GENERATION?: #UInt
	// When set to true gather performance statistics about the FUSE
	// callbacks. The results are displayed with cvmfs_talk internal
	// affairs.
	CVMFS_INSTRUMENT_FUSE?: bool
	// In NFS mode, use only inodes of the form b%a.
	CVMFS_NFS_INTERLEAVED_INODES: =~"^[0-9]+%[0-9]+$" | *""
	// Static fields always attached to the (absolute) output of the
	// InfluxDB Telemetry Aggregator.
	CVMFS_INFLUX_EXTRA_FIELDS?: string
	// Static tags always attached to the (absolute + delta) output of
	// the InfluxDB Telemetry Aggregator.
	CVMFS_INFLUX_EXTRA_TAGS?: string
	// Host name or IP address of the receiver of the InfluxDB
	// Telemetry Aggregator.
	CVMFS_INFLUX_HOST: #Host | *""
	// Name of the measurement of the InfluxDB Telemetry Aggregator.
	CVMFS_INFLUX_METRIC_NAME: string | *""
	// Port of the host (receiver) of the InfluxDB Telemetry Aggregator.
	CVMFS_INFLUX_PORT: #Port | *0
	// Which IP protocol to prefer when connecting to proxies. Can be
	// either 4 or 6.
	CVMFS_IPFAMILY_PREFER?: 4 | 6
	// If set to a non-empty value, CVMFS does not try to resolve IPv6
	// records.
	CVMFS_IPV4_ONLY?: string
	// Timeout in seconds for path names and file attributes in the
	// kernel file system buffers.
	CVMFS_KCACHE_TIMEOUT?: int
	// Directory containing *.pub files used as repository signing
	// keys. If set, this parameter has precedence over CVMFS_PUBLIC_KEY.
	CVMFS_KEYS_DIR?: #AbsPath
	// For standalone deployment. Allows cvmfs2 to discover libraries
	// libcvmfs_<...>.so that are not installed in one of standard
	// search paths.
	CVMFS_LIBRARY_PATH?: #AbsPath
	// Minimum transfer rate in bytes/second a server or proxy must
	// provide.
	CVMFS_LOW_SPEED_LIMIT?: #UInt
	// Allows to hide extended attributes to be listed. Options: always,
	// never, rootonly. rootonly means that the listing can only be
	// requested for /cvmfs/<repo>. For any other file, only a direct
	// request to a specific extended attribute will work. Compared
	// case-insensitively (mountpoint.cc).
	CVMFS_MAGIC_XATTRS_VISIBILITY?: =~"(?i)^(always|never|rootonly)$"
	// Limit the number of IP addresses a proxy names resolves into.
	// From all registered addresses, up to the limit are randomly
	// selected.
	CVMFS_MAX_IPADDR_PER_PROXY?: #UInt
	// Maximum number of retries for a given proxy/host combination.
	CVMFS_MAX_RETRIES?: #UInt
	// Limit the number of (geo sorted) stratum 1 servers that are
	// effectively used.
	CVMFS_MAX_SERVERS?: #UInt
	// Maximum file catalog TTL in minutes. Can overwrite the TTL
	// stored in the catalog.
	CVMFS_MAX_TTL?: #UInt
	// Size of the CernVM-FS metadata memory cache in Megabytes.
	CVMFS_MEMCACHE_SIZE?: #PosInt
	// Directory where CernVM-FS is mounted to. Default is /cvmfs and
	// cannot be overwritten.
	CVMFS_MOUNT_DIR: #AbsPath
	// Semi-colon-separated chain of RFC6249-compliant servers to
	// locate Stratum-1 servers.
	CVMFS_METALINK_URL?: #URLChain
	// See CVMFS_PROXY_RESET_AFTER, for metalink servers.
	CVMFS_METALINK_RESET_AFTER?: #UInt
	// Mount CernVM-FS as a read/write file system. Write operations
	// will fail but this option can workaround faulty open() flags.
	CVMFS_MOUNT_RW?: bool
	// Maximum number of open file descriptors that can be used by the
	// CernVM-FS process.
	CVMFS_NFILES: #PosInt
	// If set to yes, act as a source for the NFS daemon (NFS export).
	CVMFS_NFS_SOURCE: bool | *false
	// If set a path, used to store the NFS maps in an SQlite database,
	// instead of the usual LevelDB storage in the cache directory.
	CVMFS_NFS_SHARED: #AbsPath | *""
	// Chain of URLs pointing to PAC files with HTTP proxy configuration
	// information. The special entry "auto" triggers WPAD (wpad.cc).
	CVMFS_PAC_URLS?: =~"(?i)^(auto|https?://[^ \\t;]+)(;(auto|https?://[^ \\t;]+))*$"
	// Set the Linux kernel's out-of-memory killer priority for the
	// CernVM-FS client [-1000 - 1000].
	CVMFS_OOM_SCORE_ADJ?: int & >=-1000 & <=1000
	// Delay in seconds after which CernVM-FS will retry the primary
	// proxy group in case of a fail-over to another group.
	CVMFS_PROXY_RESET_AFTER?: #UInt
	// If set to yes, shard requests across all proxies within the
	// current load-balancing group using consistent hashing.
	CVMFS_PROXY_SHARD?: bool
	// Overwrite the default proxy template in Geo-API calls. Only
	// needed for debugging.
	CVMFS_PROXY_TEMPLATE?: string
	// Colon-separated list of repository signing keys.
	CVMFS_PUBLIC_KEY?: #AbsPathList
	// Set to "no" to use fusermount3 to mount cvmfs (may need the fuse
	// package providing fusermount3 to be installed manually).
	CVMFS_PREMOUNT_FUSE?: bool
	// Soft-limit of the cache in Megabyte.
	CVMFS_QUOTA_LIMIT: #QuotaLimit
	// Directory of the sockets used by the CernVM-FS loader to trigger
	// hotpatching/reloading.
	CVMFS_RELOAD_SOCKETS: #AbsPath
	// Comma-separated list of fully qualified repository names to
	// include in use of client utilities such as cvmfs_talk and
	// cvmfs_config. Does not limit which repositories may be mounted,
	// unless CVMFS_STRICT_MOUNT is set to yes.
	CVMFS_REPOSITORIES: #FQRNList | *""
	// A timestamp in ISO format (e.g. 2007-03-01T13:00:00Z). Selects
	// the repository state as of the given date.
	CVMFS_REPOSITORY_DATE: =~"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$" | *""
	// Select a named repository snapshot that should be mounted
	// instead of trunk.
	CVMFS_REPOSITORY_TAG: string | *""
	// If set to yes, no repository can be mounted unless the config
	// repository is available.
	CVMFS_CONFIG_REPO_REQUIRED?: bool
	// Hash of the root file catalog (sha1/rmd160, 40 hex characters);
	// implies CVMFS_AUTO_UPDATE=no.
	CVMFS_ROOT_HASH: =~"^[a-f0-9]{40}$" | *""
	// If set to yes, include the cvmfs path of downloaded data in HTTP
	// headers.
	CVMFS_SEND_INFO_HEADER?: bool
	// Enable special cache semantics for a client used as a
	// publisher's repository base line.
	CVMFS_SERVER_CACHE_MODE?: bool
	// Semicolon-separated chain of Stratum 1 servers.
	CVMFS_SERVER_URL: #URLChain
	// If set to no, makes a repository use an exclusive cache.
	CVMFS_SHARED_CACHE: bool
	// Caching time of statfs() in seconds (no caching by default).
	// Calling statfs() in high frequency can be expensive.
	CVMFS_STATFS_CACHE_TIMEOUT?: #UInt
	// If set to yes, use a download manager to download regular files
	// on read.
	CVMFS_STREAMING_CACHE?: bool
	// If set to yes, mount only repositories that are listed in
	// CVMFS_REPOSITORIES.
	CVMFS_STRICT_MOUNT: bool
	// If set to yes, enable suid magic on the mounted repository.
	// Requires mounting as root.
	CVMFS_SUID?: bool
	// If set to a number between 0 and 7, uses the corresponding
	// LOCALn facility for syslog messages.
	CVMFS_SYSLOG_FACILITY?: int & >=0 & <=7
	// Sets the syslog level for CernVM-FS messages: 1 = LOG_DEBUG, 2 =
	// LOG_INFO, 3 = LOG_NOTICE. 3 is also the built-in default, so it is
	// only meaningful for making the default explicit (logging.cc:
	// SetLogSyslogLevel falls back to LOG_NOTICE for any other value too).
	CVMFS_SYSLOG_LEVEL?: 1 | 2 | *3
	// Prefix for each CVMFS message in the syslog. By default it is
	// the repo name.
	CVMFS_SYSLOG_PREFIX?: string
	// If set to yes, modify the command line to @cvmfs2 ... in order
	// to act as a systemd lowlevel storage manager.
	CVMFS_SYSTEMD_NOKILL?: bool
	// Internal usage. Used for cvmfs_talk. Default socket is
	// /var/spool/cvmfs/<repo>/cvmfs_io.
	CVMFS_TALK_SOCKET?: #AbsPath
	// Internal usage. Used for cvmfs_talk. By default it is the repo
	// owner.
	CVMFS_TALK_OWNER?: #UserName
	// Rate in seconds for Telemetry Aggregator to send the telemetry.
	// Minimum send rate >= 5 sec (mountpoint.cc: kMinimumTelemetrySendRateSec).
	CVMFS_TELEMETRY_RATE: (int & >=5) | *0
	// ON to activate Telemetry Aggregator.
	CVMFS_TELEMETRY_SEND: bool | *false
	// Timeout in seconds for HTTP requests with a proxy server.
	CVMFS_TIMEOUT: #UInt
	// Timeout in seconds for HTTP requests without a proxy server.
	CVMFS_TIMEOUT_DIRECT: #UInt
	// Internal usage. Max number of entries of the tracebuffer.
	CVMFS_TRACEBUFFER: #PosInt | *0
	// Internal usage. Flush threshold after how many entries the
	// tracebuffer is flushed to file.
	CVMFS_TRACEBUFFER_THRESHOLD: #PosInt | *0
	// If set, enables the tracer and trace file system calls to the
	// given file.
	CVMFS_TRACEFILE: #AbsPath | *""
	// Request order of Stratum 1 servers and fallback proxies via
	// Geo-API.
	CVMFS_USE_GEOAPI?: bool
	// When connecting to an HTTPS endpoints, it will load the
	// certificates provided by the system.
	CVMFS_USE_SSL_SYSTEM_CA?: bool
	// Sets the gid and uid mount options. Don't touch or overwrite.
	CVMFS_USER: #UserName
	// All messages that normally are logged to syslog are re-directed
	// to the given file. This file can grow up to 500kB and there is
	// one step of log rotation. Required for microCernVM.
	CVMFS_USYSLOG?: #AbsPath
	// Comma-separated list of (main) group IDs that are allowed to
	// access the extended attributes listed by
	// CVMFS_XATTR_PROTECTED_XATTRS.
	CVMFS_XATTR_PRIVILEGED_GIDS?: #UIntList
	// Comma-separated list of extended attributes (full name, e.g.
	// user.fqrn) that are only accessible by root and the group IDs
	// listed by CVMFS_XATTR_PRIVILEGED_GIDS.
	CVMFS_XATTR_PROTECTED_XATTRS?: =~"^[A-Za-z0-9._-]+(,[A-Za-z0-9._-]+)*$"
	// Set the local directory for storing special files (defaults to
	// the cache directory).
	CVMFS_WORKSPACE?: #AbsPath
	// Override posix read permissions to make files in repository
	// globally readable.
	CVMFS_WORLD_READABLE?: bool

	// --- Tiered Cache Parameters -------------------------------------
	// Tiered cache (family): CVMFS_CACHE_<name>_UPPER - name of the
	// upper layer cache instance.
	// Tiered cache (family): CVMFS_CACHE_<name>_LOWER - name of the
	// lower layer cache instance.
	// Set to true to avoid populating the lower layer.
	CVMFS_CACHE_LOWER_READONLY?: bool

	// --- External Cache Plugin Parameters ----------------------------
	// External plugin (family): CVMFS_CACHE_<name>_CMDLINE - if the
	// client should start the plugin, the executable and command line
	// parameters of the plugin, separated by comma.
	// External plugin (family): CVMFS_CACHE_<name>_LOCATOR - the
	// address of the socket used for communication with the plugin.

	// --- In-memory Cache Plugin Parameters ---------------------------
	// If set, run CernVM-FS in debug mode and write a verbose log to
	// the specified file.
	CVMFS_CACHE_PLUGIN_DEBUGLOG?: #AbsPath
	// The address of the socket used for client communication.
	CVMFS_CACHE_PLUGIN_LOCATOR?: string
	// The amount of RAM in megabyte used by the plugin for caching.
	CVMFS_CACHE_PLUGIN_SIZE?: #PosInt

	// --- Undocumented / internal (not in apx-parameters) ----------
	// Fully qualified name of this repository.
	CVMFS_REPOSITORY_NAME?: #FQRN
	// Fully qualified repository name, derived automatically.
	CVMFS_FQRN?: #FQRN
	// Repositories that must not be mounted (loader.cc; undocumented).
	CVMFS_REPOSITORIES_NOMOUNT?: #FQRNList
	// Use the fuse kernel passthrough mode if available (loader.cc).
	CVMFS_FUSE_PASSTHROUGH?: bool
	// macOS only: use the macFUSE kernel extension.
	CVMFS_USE_MACFUSE_KEXT?: bool
	// Client profile selecting tuned option sets; only "single" is defined.
	CVMFS_CLIENT_PROFILE?: "single" | ""
	// Internal state of the cvmfs_config script, not a real parameter.
	CVMFS_PARMS?: string
	// Guard marker from default.conf; makes the base environment
	// (CVMFS_USER etc.) readonly exactly once. Present in every merged config.
	CVMFS_BASE_ENV?: "1"
	// Buffer size of the streaming cache (bytes; undocumented).
	CVMFS_STREAMING_CACHE_BUFFER_SIZE?: #PosInt
	// Proxy sharding policy; EXTERNAL is the only recognized value
	// (mountpoint.cc; undocumented).
	CVMFS_PROXY_SHARDING_POLICY?: "EXTERNAL"
	// Use the CDN configuration of the repository (cern.ch configs).
	CVMFS_USE_CDN?: bool
	// Keep failing over between hosts/proxies without giving up.
	CVMFS_FAILOVER_INDEFINITELY?: bool
	// Additional info header string (mountpoint.cc; undocumented).
	CVMFS_INFO_HEADER?: string
	// Internal helper of the config-repo default environment.
	CVMFS_CONFIG_REPO_DEFAULT_ENV?: string
	// Maximum file catalog TTL in seconds (mountpoint.cc; undocumented).
	CVMFS_MAX_TTL_SECS?: #UInt
	// Prefetch file bundles (cvmfs.cc; undocumented, experimental).
	CVMFS_PREFETCH_FILEBUNDLES?: bool
	// Size of the bundle-prefetch worker pool (bundle_mgr.cc; undocumented,
	// experimental). Overrides the built-in default only when >= 1;
	// unparsable or zero values are silently ignored.
	CVMFS_BUNDLE_POOL_SIZE?: #PosInt
	// Partial replica mode (mountpoint.cc; undocumented).
	CVMFS_PARTIAL_REPLICA_MODE?: =~"(?i)^(fail|failover)$" | false
	// Full Stratum 1 URL hint (mountpoint.cc; undocumented).
	CVMFS_FULL_STRATUM1_URL?: string
	// Notification system server URL (cvmfs.cc; undocumented).
	CVMFS_NOTIFICATION_SERVER?: #URL
	// Also send per-interval deltas (telemetry_aggregator_influx.cc).
	CVMFS_INFLUX_SEND_DELTA?: bool
	// The actual signature-bypass switch (mountpoint.cc; undocumented).
	"_CVMFS_DEVEL_IGNORE_SIGNATURE_FAILURES"?: bool

	// --- Dead: no longer read by any code -----------------------------
	// Legacy CernVM parameter. Only ever appears in cvmfs_config's
	// parm_list (showconfig display bookkeeping); no functional C++ or
	// shell reader was found anywhere in the repo.
	CERNVM_GRID_UI_VERSION?: string
	// The read site is entirely commented out in mountpoint.cc (a
	// legacy libcvmfs-only code path). Setting this parameter has no
	// effect.
	CVMFS_CWD_CACHE?: bool
	// Only ever appears in cvmfs_config's parm_list; no mountpoint.cc or
	// other reader references it, despite historical documentation
	// attributing it there. The functioning equivalent is the
	// separately-typed CVMFS_EXTERNAL_URL above.
	CVMFS_EXTERNAL_SERVER_URL?: #URLChain
	// Only ever appears in cvmfs_config's parm_list; no functional
	// reader found anywhere in the repo.
	CVMFS_TRUSTED_CERTS?: #AbsPath

	// ===================================================================
	// Cross-parameter dependency rules
	// ===================================================================
	// Boolean parameters are compared (X == true), never unified
	// (true & X): unification would pick the bool branch of a defaulted
	// field instead of testing the default.

	// Helper: alien cache configured via either the documented parameter
	// or its legacy alias (mountpoint.cc reads CVMFS_CACHE_ALIEN).
	_alienCacheSet: CVMFS_ALIEN_CACHE != "" || CVMFS_CACHE_ALIEN != ""
	// Helper: a fixed root catalog is selected (hash, tag or date).
	_fixedCatalog: CVMFS_ROOT_HASH != "" || CVMFS_REPOSITORY_TAG != "" || CVMFS_REPOSITORY_DATE != ""
	// CVMFS_CACHE_BASE and CVMFS_CACHE_DIR are mutually exclusive; both
	// set is a boot error (mountpoint.cc: CheckPosixCacheSettings,
	// CreateWorkspace).
	if CVMFS_CACHE_DIR != "" {
		_rule_cache_dir_conflicts_with_cache_base: true & (CVMFS_CACHE_BASE == "")
	}
	if _alienCacheSet {
		// Shared local disk cache and alien cache are mutually exclusive;
		// boot error (mountpoint.cc: CheckPosixCacheSettings).
		_rule_alien_cache_conflicts_with_shared_cache: true & (CVMFS_SHARED_CACHE == false)
		// Quota management and alien cache are mutually exclusive; the
		// quota limit must be turned off (mountpoint.cc:
		// CheckPosixCacheSettings; unset falls back to a managed default).
		_rule_alien_cache_requires_unmanaged_quota: true & (CVMFS_QUOTA_LIMIT <= 0)
	}

	// Shared NFS maps are only read in NFS export mode (mountpoint.cc:
	// CVMFS_NFS_SHARED is nested under the CVMFS_NFS_SOURCE branch).
	if CVMFS_NFS_SHARED != "" {
		_rule_nfs_shared_requires_nfs_source: true & (CVMFS_NFS_SOURCE == true)
	}

	// Tracing headers are only read when tracing is enabled
	// (mountpoint.cc: SetupHttpTuning).
	if CVMFS_HTTP_TRACING_HEADERS != "" {
		_rule_tracing_headers_require_http_tracing: true & (CVMFS_HTTP_TRACING == true)
	}

	// The telemetry aggregator refuses to start unless host, port and
	// metric name are all set (telemetry_aggregator_influx.cc).
	if CVMFS_TELEMETRY_SEND {
		_rule_telemetry_requires_influx_endpoint: true & (CVMFS_INFLUX_HOST != "" && CVMFS_INFLUX_PORT != 0 && CVMFS_INFLUX_METRIC_NAME != "")
	}

	// The telemetry rate is only read when telemetry is enabled
	// (mountpoint.cc: SetupBehavior).
	if CVMFS_TELEMETRY_RATE != 0 {
		_rule_telemetry_rate_requires_telemetry_send: true & (CVMFS_TELEMETRY_SEND == true)
	}

	// Root hash, tag and date pin the mounted revision; the root hash
	// silently overrides tag and date, and the tag silently overrides the
	// date (mountpoint.cc: DetermineRootHash). Require at most one.
	_rule_at_most_one_revision_pin: true & (!(CVMFS_ROOT_HASH != "" && CVMFS_REPOSITORY_TAG != "") && !(CVMFS_ROOT_HASH != "" && CVMFS_REPOSITORY_DATE != "") && !(CVMFS_REPOSITORY_TAG != "" && CVMFS_REPOSITORY_DATE != ""))
	// The alternative root path only applies to fixed catalogs
	// (mountpoint.cc: CVMFS_ALT_ROOT_PATH is read in the InitFixed branch).
	if CVMFS_ALT_ROOT_PATH {
		_rule_alt_root_path_requires_fixed_catalog: true & _fixedCatalog
	}

	// Interleaved inodes are only read when NFS maps are active
	// (mountpoint.cc: SetupNfsMaps).
	if CVMFS_NFS_INTERLEAVED_INODES != "" {
		_rule_nfs_interleaved_inodes_require_nfs_source: true & (CVMFS_NFS_SOURCE == true)
	}

	// A pinned revision always disables automatic catalog updates; an
	// explicit CVMFS_AUTO_UPDATE=yes contradicts it (mountpoint.cc:
	// fixed_catalog_).
	if _fixedCatalog {
		_rule_fixed_catalog_conflicts_with_auto_update: true & (CVMFS_AUTO_UPDATE == false)
	}

	// With strict mount, every mount fails unless the repository is
	// listed; an empty list is an error (mount.cvmfs.cc:
	// CheckStrictMount, "CVMFS_REPOSITORIES missing").
	if CVMFS_STRICT_MOUNT {
		_rule_strict_mount_requires_repositories: true & (CVMFS_REPOSITORIES != "")
	}

	// Sanity: initial retry backoff must not exceed the maximum (CUE-only
	// rule; cross-parameter comparisons aren't expressible in JSON Schema
	// draft-7).
	_rule_backoff_init_le_backoff_max: true & (CVMFS_BACKOFF_INIT <= CVMFS_BACKOFF_MAX)
	// Sanity: minimum DNS TTL must not exceed the maximum (CUE-only rule,
	// as above); unset values compare against the code defaults (60/86400).
	_rule_dns_min_ttl_le_dns_max_ttl: true & (CVMFS_DNS_MIN_TTL <= CVMFS_DNS_MAX_TTL)
	// The trace buffer parameters are only read when the tracer is
	// active (mountpoint.cc: CreateTracer).
	if CVMFS_TRACEBUFFER != 0 || CVMFS_TRACEBUFFER_THRESHOLD != 0 {
		_rule_tracebuffer_requires_tracefile: true & (CVMFS_TRACEFILE != "")
	}

	// Evicting non-open files first requires the refcounted cache mode;
	// silently ignored otherwise (mountpoint.cc:
	// DeterminePosixCacheSettings). Default cache instance only.
	if CVMFS_CACHE_CLEANUP_NONOPENLRU {
		_rule_cleanup_nonopenlru_requires_refcount: true & (CVMFS_CACHE_REFCOUNT == true)
	}
}
