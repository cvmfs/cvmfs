// CernVM-FS client configuration schema

// Field forms:
//   NAME: T             required
//   NAME?: T            optional
//   NAME: T | *default  optional; carries a default value

package cvmfs

#FQRN: =~"^[a-z0-9][a-z0-9.-]*\\.[a-z0-9-]+$"

// Comma-separated list of fully qualified repository names.
#FQRNList: =~"^[a-z0-9][a-z0-9.-]*\\.[a-z0-9-]+(,[a-z0-9][a-z0-9.-]*\\.[a-z0-9-]+)*$"
#AbsPath:  =~"^/(?:[^/]+/)*[^/]*$"

// Colon-separated list of absolute paths (files or directories).
#AbsPathList: =~"^/(?:[^/:]+/)*[^/:]*(?::/(?:[^/:]+/)*[^/:]*)*$"

// Unsigned integer, 0 allowed.
#UInt: int & >=0

// Strictly positive integer.
#PosInt: int & >0

// Comma-separated list of unsigned integers.
#UIntList: =~"^[0-9]+(,[0-9]+)*$"

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

// Chain of HTTP(S) URLs: ';' separates failover groups,
// '|' separates load-balanced members within a group.
#URLChain: =~"(?i)^https?://[^ \\t;|]+([;|]https?://[^ \\t;|]+)*$"

// Proxy chain: ';' separates failover groups, '|' load-balanced members.
// An entry is DIRECT (no proxy), auto (WPAD/PAC discovery) or a proxy
// address with optional scheme and port.
_#proxyEntry: "(DIRECT|auto|(https?://)?[A-Za-z0-9._-]+(:\(_#portRe))?)"
#ProxyChain:  =~"(?i)^\(_#proxyEntry)([;|]\(_#proxyEntry))*$"

// POSIX user name.
#UserName: =~"^[A-Za-z_][A-Za-z0-9._-]*$"

// Cache quota in MB; -1 means unlimited.
#QuotaLimit: int & >=-1

// Client configuration

#ClientConfig: {
	CVMFS_ALIEN_CACHE: #AbsPath | *"" @description("If set, use an alien cache at the given location.")
	CVMFS_ALT_ROOT_PATH: bool | *false @description("If set to yes, use alternative root catalog path. Only required for fixed catalogs (tag / hash) under the alternative path.")
	CVMFS_ARCH?: string @description("Automatically set by CVMFS to reflect the CPU architecture on which the client runs (using uname -m). Allows to utilize variant symlinks with cvmfs installations to auto-select the architecture.")
	CVMFS_VERSION?: string @description("Client version string (e.g. \"2.14.0\"); read-only, injected by `cvmfs2 -o parse`.")
	CVMFS_VERSION_NUMERIC?: #UInt @description("Client version as a single comparable integer; read-only, injected by `cvmfs2 -o parse`.")
	CVMFS_AUTO_UPDATE: bool | *null @description("If set to no, disables the automatic update of file catalogs. Updates are on unless this is set, so the default is null to distinguish \"unset\" from an explicit yes (mountpoint.cc: fixed_catalog_).")
	CVMFS_AUTHZ_HELPER?: #AbsPath @description("Full path to an authz helper, overwrites the helper hint in the catalog.")
	CVMFS_AUTHZ_SEARCH_PATH: #AbsPath | *"/usr/libexec/cvmfs/authz" @description("Full path to the directory that contains the authz helpers.")
	// Any other CVMFS_AUTHZ_<name> is passed to the authz helper with the
	// prefix stripped.
	[=~"^CVMFS_AUTHZ_[A-Za-z0-9_]+$"]: string
	CVMFS_BACKOFF_INIT: #UInt | *2 @description("Seconds for the maximum initial backoff when retrying to download data.")
	CVMFS_BACKOFF_MAX: #UInt | *10 @description("Maximum backoff in seconds when retrying to download data.")
	CVMFS_BLACKLIST: #AbsPath | *"/etc/cvmfs/blacklist" @description("File name of the blacklist that denies mounting any revision < revision N. Format: <REPO N where REPO is the repository name, N is the revision number, and the two parts are separated by whitespace. Note: no extra characters are allowed after N, not even whitespace.")
	CVMFS_CATALOG_WATERMARK?: #UInt @description("Try to release pinned catalogs when their number surpasses the given watermark. Defaults to 1/4 CVMFS_NFILES; explicitly set by shrinkwrap.")
	CVMFS_CACHE_ALIEN: #AbsPath | *"" @description("Deprecated, legacy parameter. Use CVMFS_ALIEN_CACHE instead.")
	CVMFS_CACHE_BASE: #AbsPath | *"" @description("Location (directory) of the CernVM-FS cache.")
	CVMFS_CACHE_DIR: #AbsPath | *"" @description("Similar to CVMFS_CACHE_BASE, but automatically set by cvmfs. Only might need manual overwriting when using libcvmfs.")
	CVMFS_CACHE_PRIMARY: =~"^[A-Za-z0-9_]+$" | *"default" @description("Type of cache to use. By default it is posix. (see also Advanced Cache Configuration)")
	CVMFS_CACHE_REFCOUNT: bool | *true @description("If set to no, disable deduplication of open file descriptors by refcounting. On by default (mountpoint.h: PosixCacheSettings::do_refcount).")
	CVMFS_CACHE_CLEANUP_NONOPENLRU: bool | *false @description("Prefer evicting files that are not currently open; only effective together with CVMFS_CACHE_REFCOUNT (mountpoint.cc).")
	CVMFS_CACHE_SHARED?:      bool @description("Alias of CVMFS_SHARED_CACHE for the default cache instance; takes precedence over that compatibility name (mountpoint.cc: MkCacheParm).")
	CVMFS_CACHE_QUOTA_LIMIT?: #QuotaLimit @description("Alias of CVMFS_QUOTA_LIMIT for the default cache instance; takes precedence over that compatibility name (mountpoint.cc: MkCacheParm).")
	CVMFS_CACHE_SERVER_MODE?: bool @description("Alias of CVMFS_SERVER_CACHE_MODE for the default cache instance; takes precedence over that compatibility name (mountpoint.cc: MkCacheParm).")
	CVMFS_CACHE_WORKSPACE?:   #AbsPath @description("Alias of CVMFS_WORKSPACE for the default cache instance: directory for the cache manager's own files.")

	// Cache instance families: CVMFS_CACHE_<name>_<param>
	// Instance names are 1-24 characters of [A-Za-z0-9_].
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
	CVMFS_CACHE_SYMLINKS?: bool @description("If set to yes, enables symlink caching in the kernel.")
	CVMFS_CHECK_PERMISSIONS?: bool @description("If set to no, disable checking of file ownership and permissions (open all files).")
	CVMFS_CLAIM_OWNERSHIP?: bool @description("If set to yes, allows CernVM-FS to claim ownership of files and directories.")
	CVMFS_CONFIG_REPOSITORY?: #FQRN @description("CVMFS repository where a CVMFS client will get its config from. The default configuration rpm cvmfs-config-default sets this parameter to cvmfs-config.cern.ch")
	CVMFS_CPU_AFFINITY?: #UIntList @description("Comma-separated list to set CPU affinity for all cvmfs components.")
	CVMFS_DEBUGLOG?: #AbsPath @description("If set, run CernVM-FS in debug mode and write a verbose log to the specified file.")
	CVMFS_DEFAULT_DOMAIN?: #Hostname @description("The default domain will be automatically appended to repository names when given without a domain.")
	CVMFS_DNS_MIN_TTL: #UInt | *60 @description("Minimum effective TTL in seconds for DNS queries of proxy server names (not Stratum 1s). Defaults to 1 minute.")
	CVMFS_DNS_MAX_TTL: #UInt | *86400 @description("Maximum effective TTL in seconds for DNS queries of proxy server names (not Stratum 1s). Defaults to 1 day.")
	CVMFS_DNS_RETRIES?: #UInt @description("Number of retries when resolving proxy names.")
	CVMFS_DNS_SERVER?: #IP @description("IP of the DNS server CVMFS should use.")
	CVMFS_DNS_TIMEOUT?: #UInt @description("Timeout in seconds when resolving proxy names.")
	CVMFS_DNS_ROAMING?: bool @description("If true, watch /etc/resolv.conf for nameserver changes.")
	CVMFS_ENFORCE_ACLS?: bool @description("Enforce POSIX ACLs stored in the repository. Requires libfuse 3.")
	CVMFS_EXTERNAL_FALLBACK_PROXY?: #ProxyChain @description("List of HTTP proxies similar to CVMFS_EXTERNAL_HTTP_PROXY. The fallback proxies are added to the end of the normal proxies, and disable DIRECT connections.")
	CVMFS_EXTERNAL_HTTP_PROXY?: #ProxyChain @description("Chain of HTTP proxy groups to be used when CernVM-FS is accessing external data.")
	CVMFS_EXTERNAL_MAX_SERVERS?: #UInt @description("Caps the list of external hosts to the given number (after geo-sorting them).")
	CVMFS_EXTERNAL_METALINK?: #URLChain @description("Semi-colon-separated chain of RFC6249-compliant servers to locate webservers serving external data.")
	CVMFS_EXTERNAL_TIMEOUT?: #UInt @description("Timeout in seconds for HTTP requests to an external-data server with a proxy server.")
	CVMFS_EXTERNAL_TIMEOUT_DIRECT?: #UInt @description("Timeout in seconds for HTTP requests to an external-data server without a proxy server.")
	CVMFS_EXTERNAL_URL?: #URLChain @description("Semicolon-separated chain of webservers serving external data chunks.")
	CVMFS_FALLBACK_PROXY?: #ProxyChain @description("List of HTTP proxies similar to CVMFS_HTTP_PROXY. The fallback proxies are added to the end of the normal proxies, and disable DIRECT connections.")
	CVMFS_FUSE_NOTIFY_INVALIDATION?: bool @description("Disable fuse notify invalidation. By default disabled on macOS to fix stability issues. On Linux systems, it is NOT recommended to turn it off.")
	CVMFS_FUSE3_MAX_THREADS?: #UInt @description("Set max number of fuse threads (requires: libfuse3 > 3.12).")
	CVMFS_FUSE3_IDLE_THREADS?: #UInt @description("Set max number of idle fuse threads (requires: libfuse3 > 3.12).")
	CVMFS_FOLLOW_REDIRECTS?: bool @description("When set to yes, follow up to 4 HTTP redirects in requests.")
	CVMFS_HIDE_MAGIC_XATTRS?: bool @description("If set to yes the client will not expose CernVM-FS specific extended attributes.")
	CVMFS_HOST_RESET_AFTER?: #UInt @description("See CVMFS_PROXY_RESET_AFTER, for server URLs.")
	CVMFS_HTTP_PROXY: #ProxyChain @description("Chain of HTTP proxy groups used by CernVM-FS. Necessary. Set to DIRECT if you don't use proxies.")
	CVMFS_HTTP_TRACING: bool | *false @description("Activates that a tracing header is attached to each CURL request. Consists of uid, pid, and gid. Default is off.")
	CVMFS_HTTP_TRACING_HEADERS: =~"^[^:|]+:[^|]*(\\|[^:|]+:[^|]*)*$" | *"" @description("Adds additional static, user-defined tracing headers. Format: key1:val1|key2:val2|key3:val3. Needs CVMFS_HTTP_TRACING to be set to on.")
	CVMFS_IGNORE_SIGNATURE?: bool @description("When set to yes, don't verify CernVM-FS file catalog signatures. No effect; see _CVMFS_DEVEL_IGNORE_SIGNATURE_FAILURES below.")
	CVMFS_INITIAL_GENERATION?: #UInt @description("Initial inode generation. Used for testing.")
	CVMFS_INSTRUMENT_FUSE?: bool @description("When set to true gather performance statistics about the FUSE callbacks. The results are displayed with cvmfs_talk internal affairs.")
	CVMFS_NFS_INTERLEAVED_INODES: =~"^[0-9]+%[0-9]+$" | *"" @description("In NFS mode, use only inodes of the form b%a.")
	CVMFS_INFLUX_EXTRA_FIELDS?: string @description("Static fields always attached to the (absolute) output of the InfluxDB Telemetry Aggregator.")
	CVMFS_INFLUX_EXTRA_TAGS?: string @description("Static tags always attached to the (absolute + delta) output of the InfluxDB Telemetry Aggregator.")
	CVMFS_INFLUX_HOST: #Host | *"" @description("Host name or IP address of the receiver of the InfluxDB Telemetry Aggregator.")
	CVMFS_INFLUX_METRIC_NAME: string | *"" @description("Name of the measurement of the InfluxDB Telemetry Aggregator.")
	CVMFS_INFLUX_PORT: #Port | *0 @description("Port of the host (receiver) of the InfluxDB Telemetry Aggregator.")
	CVMFS_IPFAMILY_PREFER?: 4 | 6 @description("Which IP protocol to prefer when connecting to proxies. Can be either 4 or 6.")
	CVMFS_IPV4_ONLY?: string @description("If set to a non-empty value, CVMFS does not try to resolve IPv6 records.")
	CVMFS_KCACHE_TIMEOUT: int | *60 @description("Timeout in seconds for path names and file attributes in the kernel file system buffers.")
	CVMFS_KEYS_DIR?: #AbsPath @description("Directory containing *.pub files used as repository signing keys. If set, this parameter has precedence over CVMFS_PUBLIC_KEY.")
	CVMFS_LIBRARY_PATH?: #AbsPath @description("For standalone deployment. Allows cvmfs2 to discover libraries libcvmfs_<...>.so that are not installed in one of standard search paths.")
	CVMFS_LOW_SPEED_LIMIT?: #UInt @description("Minimum transfer rate in bytes/second a server or proxy must provide.")
	CVMFS_MAGIC_XATTRS_VISIBILITY?: =~"(?i)^(always|never|rootonly)$" @description("Allows to hide extended attributes to be listed. Options: always, never, rootonly. rootonly means that the listing can only be requested for /cvmfs/<repo>. For any other file, only a direct request to a specific extended attribute will work. Compared case-insensitively (mountpoint.cc).")
	CVMFS_MAX_IPADDR_PER_PROXY?: #UInt @description("Limit the number of IP addresses a proxy names resolves into. From all registered addresses, up to the limit are randomly selected.")
	CVMFS_MAX_RETRIES: #UInt | *1 @description("Maximum number of retries for a given proxy/host combination.")
	CVMFS_MAX_SERVERS?: #UInt @description("Limit the number of (geo sorted) stratum 1 servers that are effectively used.")
	CVMFS_MAX_TTL?: #UInt @description("Maximum file catalog TTL in minutes. Can overwrite the TTL stored in the catalog.")
	CVMFS_MEMCACHE_SIZE: #PosInt | *16 @description("Size of the CernVM-FS metadata memory cache in Megabytes.")
	CVMFS_MOUNT_DIR: #AbsPath | *"/cvmfs" @description("Directory where CernVM-FS is mounted to. Default is /cvmfs and cannot be overwritten.")
	CVMFS_METALINK_URL?: #URLChain @description("Semi-colon-separated chain of RFC6249-compliant servers to locate Stratum-1 servers.")
	CVMFS_METALINK_RESET_AFTER?: #UInt @description("See CVMFS_PROXY_RESET_AFTER, for metalink servers.")
	CVMFS_MOUNT_RW?: bool @description("Mount CernVM-FS as a read/write file system. Write operations will fail but this option can workaround faulty open() flags.")
	CVMFS_NFILES: #PosInt | *8192 @description("Maximum number of open file descriptors that can be used by the CernVM-FS process.")
	CVMFS_NFS_SOURCE: bool | *false @description("If set to yes, act as a source for the NFS daemon (NFS export).")
	CVMFS_NFS_SHARED: #AbsPath | *"" @description("If set a path, used to store the NFS maps in an SQlite database, instead of the usual LevelDB storage in the cache directory.")
	CVMFS_PAC_URLS?: =~"(?i)^(auto|https?://[^ \\t;]+)(;(auto|https?://[^ \\t;]+))*$" @description("Chain of URLs pointing to PAC files with HTTP proxy configuration information. The special entry \"auto\" triggers WPAD (wpad.cc).")
	CVMFS_OOM_SCORE_ADJ?: int & >=-1000 & <=1000 @description("Set the Linux kernel's out-of-memory killer priority for the CernVM-FS client [-1000 - 1000].")
	CVMFS_PROXY_RESET_AFTER?: #UInt @description("Delay in seconds after which CernVM-FS will retry the primary proxy group in case of a fail-over to another group.")
	CVMFS_PROXY_SHARD?: bool @description("If set to yes, shard requests across all proxies within the current load-balancing group using consistent hashing.")
	CVMFS_PROXY_TEMPLATE?: string @description("Overwrite the default proxy template in Geo-API calls. Only needed for debugging.")
	CVMFS_PUBLIC_KEY?: #AbsPathList @description("Colon-separated list of repository signing keys.")
	CVMFS_PREMOUNT_FUSE?: bool @description("Set to \"no\" to use fusermount3 to mount cvmfs (may need the fuse package providing fusermount3 to be installed manually).")
	CVMFS_QUOTA_LIMIT: #QuotaLimit | *1024 @description("Soft-limit of the cache in Megabyte.")
	CVMFS_RELOAD_SOCKETS: #AbsPath | *"/var/run/cvmfs" @description("Directory of the sockets used by the CernVM-FS loader to trigger hotpatching/reloading.")
	CVMFS_REPOSITORIES: #FQRNList | *"" @description("Comma-separated list of fully qualified repository names to include in use of client utilities such as cvmfs_talk and cvmfs_config. Does not limit which repositories may be mounted, unless CVMFS_STRICT_MOUNT is set to yes.")
	CVMFS_REPOSITORY_DATE: =~"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$" | *"" @description("A timestamp in ISO format (e.g. 2007-03-01T13:00:00Z). Selects the repository state as of the given date.")
	CVMFS_REPOSITORY_TAG: string | *"" @description("Select a named repository snapshot that should be mounted instead of trunk.")
	CVMFS_CONFIG_REPO_REQUIRED?: bool @description("If set to yes, no repository can be mounted unless the config repository is available.")
	CVMFS_ROOT_HASH: =~"^[a-f0-9]{40}$" | *"" @description("Hash of the root file catalog (sha1/rmd160, 40 hex characters); implies CVMFS_AUTO_UPDATE=no.")
	CVMFS_SEND_INFO_HEADER?: bool @description("If set to yes, include the cvmfs path of downloaded data in HTTP headers.")
	CVMFS_SERVER_CACHE_MODE?: bool @description("Enable special cache semantics for a client used as a publisher's repository base line.")
	CVMFS_SERVER_URL: #URLChain @description("Semicolon-separated chain of Stratum 1 servers.")
	CVMFS_SHARED_CACHE: bool | *false @description("If set to no, makes a repository use an exclusive cache.")
	CVMFS_STATFS_CACHE_TIMEOUT?: #UInt @description("Caching time of statfs() in seconds (no caching by default). Calling statfs() in high frequency can be expensive.")
	CVMFS_STREAMING_CACHE?: bool @description("If set to yes, use a download manager to download regular files on read.")
	CVMFS_STRICT_MOUNT: bool | *false @description("If set to yes, mount only repositories that are listed in CVMFS_REPOSITORIES.")
	CVMFS_SUID?: bool @description("If set to yes, enable suid magic on the mounted repository. Requires mounting as root.")
	CVMFS_SYSLOG_FACILITY?: int & >=0 & <=7 @description("If set to a number between 0 and 7, uses the corresponding LOCALn facility for syslog messages.")
	CVMFS_SYSLOG_LEVEL: 1 | 2 | *3 @description("Sets the syslog level for CernVM-FS messages: 1 = LOG_DEBUG, 2 = LOG_INFO, 3 = LOG_NOTICE. 3 is also the built-in default, so it is only meaningful for making the default explicit (logging.cc: SetLogSyslogLevel falls back to LOG_NOTICE for any other value too).")
	CVMFS_SYSLOG_PREFIX?: string @description("Prefix for each CVMFS message in the syslog. By default it is the repo name.")
	CVMFS_SYSTEMD_NOKILL?: bool @description("If set to yes, modify the command line to @cvmfs2 ... in order to act as a systemd lowlevel storage manager.")
	CVMFS_TALK_SOCKET?: #AbsPath @description("Internal usage. Used for cvmfs_talk. Default socket is /var/spool/cvmfs/<repo>/cvmfs_io.")
	CVMFS_TALK_OWNER?: #UserName @description("Internal usage. Used for cvmfs_talk. By default it is the repo owner.")
	CVMFS_TELEMETRY_RATE: (int & >=5) | *0 @description("Rate in seconds for Telemetry Aggregator to send the telemetry. Minimum send rate >= 5 sec (mountpoint.cc: kMinimumTelemetrySendRateSec).")
	CVMFS_TELEMETRY_SEND: bool | *false @description("ON to activate Telemetry Aggregator.")
	CVMFS_TIMEOUT: #UInt | *5 @description("Timeout in seconds for HTTP requests with a proxy server.")
	CVMFS_TIMEOUT_DIRECT: #UInt | *5 @description("Timeout in seconds for HTTP requests without a proxy server.")
	CVMFS_TRACEBUFFER: #PosInt | *0 @description("Internal usage. Max number of entries of the tracebuffer.")
	CVMFS_TRACEBUFFER_THRESHOLD: #PosInt | *0 @description("Internal usage. Flush threshold after how many entries the tracebuffer is flushed to file.")
	CVMFS_TRACEFILE: #AbsPath | *"" @description("If set, enables the tracer and trace file system calls to the given file.")
	CVMFS_USE_GEOAPI?: bool @description("Request order of Stratum 1 servers and fallback proxies via Geo-API.")
	CVMFS_USE_SSL_SYSTEM_CA?: bool @description("When connecting to an HTTPS endpoints, it will load the certificates provided by the system.")
	CVMFS_USER: #UserName @description("Sets the gid and uid mount options. Don't touch or overwrite.")
	CVMFS_USYSLOG?: #AbsPath @description("All messages that normally are logged to syslog are re-directed to the given file. This file can grow up to 500kB and there is one step of log rotation. Required for microCernVM.")
	CVMFS_XATTR_PRIVILEGED_GIDS?: #UIntList @description("Comma-separated list of (main) group IDs that are allowed to access the extended attributes listed by CVMFS_XATTR_PROTECTED_XATTRS.")
	CVMFS_XATTR_PROTECTED_XATTRS?: =~"^[A-Za-z0-9._-]+(,[A-Za-z0-9._-]+)*$" @description("Comma-separated list of extended attributes (full name, e.g. user.fqrn) that are only accessible by root and the group IDs listed by CVMFS_XATTR_PRIVILEGED_GIDS.")
	CVMFS_WORKSPACE?: #AbsPath @description("Set the local directory for storing special files (defaults to the cache directory).")
	CVMFS_WORLD_READABLE?: bool @description("Override posix read permissions to make files in repository globally readable.")

	// Tiered Cache Parameters
	CVMFS_CACHE_LOWER_READONLY?: bool @description("Set to true to avoid populating the lower layer of a tiered cache.")

	// In-memory Cache Plugin Parameters
	CVMFS_CACHE_PLUGIN_DEBUGLOG?: #AbsPath @description("If set, run CernVM-FS in debug mode and write a verbose log to the specified file.")
	CVMFS_CACHE_PLUGIN_LOCATOR?: string @description("The address of the socket used for client communication.")
	CVMFS_CACHE_PLUGIN_SIZE?: #PosInt @description("The amount of RAM in megabyte used by the plugin for caching.")

	// Undocumented / internal (not in apx-parameters)
	CVMFS_REPOSITORY_NAME?: #FQRN @description("Fully qualified name of this repository.")
	CVMFS_FQRN?: #FQRN @description("Fully qualified repository name, derived automatically.")
	CVMFS_REPOSITORIES_NOMOUNT?: #FQRNList @description("Repositories that must not be mounted (loader.cc; undocumented).")
	CVMFS_FUSE_PASSTHROUGH?: bool @description("Use the fuse kernel passthrough mode if available (loader.cc).")
	CVMFS_USE_MACFUSE_KEXT?: bool @description("macOS only: use the macFUSE kernel extension.")
	CVMFS_CLIENT_PROFILE?: "single" | "" @description("Client profile selecting tuned option sets; only \"single\" is defined.")
	CVMFS_PARMS?: string @description("Internal state of the cvmfs_config script, not a real parameter.")
	CVMFS_BASE_ENV?: "1" @description("Guard marker from default.conf; makes the base environment (CVMFS_USER etc.) readonly exactly once. Present in every merged config.")
	CVMFS_STREAMING_CACHE_BUFFER_SIZE?: #PosInt @description("Buffer size of the streaming cache (bytes; undocumented).")
	CVMFS_PROXY_SHARDING_POLICY?: "EXTERNAL" @description("Proxy sharding policy; EXTERNAL is the only recognized value (mountpoint.cc; undocumented).")
	CVMFS_USE_CDN?: bool @description("Use the CDN configuration of the repository (cern.ch configs).")
	CVMFS_FAILOVER_INDEFINITELY?: bool @description("Keep failing over between hosts/proxies without giving up.")
	CVMFS_INFO_HEADER?: string @description("Additional info header string (mountpoint.cc; undocumented).")
	CVMFS_CONFIG_REPO_DEFAULT_ENV?: string @description("Internal helper of the config-repo default environment.")
	CVMFS_MAX_TTL_SECS?: #UInt @description("Maximum file catalog TTL in seconds (mountpoint.cc; undocumented).")
	CVMFS_PREFETCH_FILEBUNDLES?: bool @description("Prefetch file bundles (mountpoint.cc; undocumented, experimental).")
	CVMFS_BUNDLE_POOL_SIZE?: #PosInt @description("Size of the bundle-prefetch worker pool (bundle_mgr.cc; undocumented, experimental). Overrides the built-in default only when >= 1; unparsable or zero values are silently ignored.")
	CVMFS_PARTIAL_REPLICA_MODE?: =~"(?i)^(fail|failover)$" | false @description("Partial replica mode (mountpoint.cc; undocumented).")
	CVMFS_FULL_STRATUM1_URL?: string @description("Full Stratum 1 URL hint (mountpoint.cc; undocumented).")
	CVMFS_NOTIFICATION_SERVER?: #URL @description("Notification system server URL (cvmfs.cc; undocumented).")
	CVMFS_INFLUX_SEND_DELTA?: bool @description("Also send per-interval deltas (telemetry_aggregator_influx.cc).")
	// The actual signature-bypass switch; undocumented.
	"_CVMFS_DEVEL_IGNORE_SIGNATURE_FAILURES"?: bool

	// Dead: no longer read by any code
	CERNVM_GRID_UI_VERSION?: string @description("Legacy CernVM parameter. Only ever appears in cvmfs_config's parm_list (showconfig display bookkeeping); no functional C++ or shell reader was found anywhere in the repo.")
	CVMFS_CWD_CACHE?: bool @description("The read site is entirely commented out in mountpoint.cc (a legacy libcvmfs-only code path). Setting this parameter has no effect.")
	CVMFS_EXTERNAL_SERVER_URL?: #URLChain @description("Only ever appears in cvmfs_config's parm_list; no mountpoint.cc or other reader references it, despite historical documentation attributing it there. The functioning equivalent is the separately-typed CVMFS_EXTERNAL_URL above.")
	CVMFS_TRUSTED_CERTS?: #AbsPath @description("Only ever appears in cvmfs_config's parm_list; no functional reader found anywhere in the repo.")

	// Cross-parameter dependency rules

	// Alien cache set through the documented parameter or its legacy alias.
	_alienCacheSet: CVMFS_ALIEN_CACHE != "" || CVMFS_CACHE_ALIEN != ""
	// A fixed root catalog is selected (hash, tag or date).
	_fixedCatalog: CVMFS_ROOT_HASH != "" || CVMFS_REPOSITORY_TAG != "" || CVMFS_REPOSITORY_DATE != ""
	// CVMFS_CACHE_BASE and CVMFS_CACHE_DIR are mutually exclusive.
	if CVMFS_CACHE_DIR != "" {
		_rule_cache_dir_conflicts_with_cache_base: true & (CVMFS_CACHE_BASE == "")
	}
	if _alienCacheSet {
		// Alien cache cannot be shared.
		_rule_alien_cache_conflicts_with_shared_cache: true & (CVMFS_SHARED_CACHE == false)
		// Alien cache needs quota management off; unset means managed.
		_rule_alien_cache_requires_unmanaged_quota: true & (CVMFS_QUOTA_LIMIT <= 0)
	}

	// Shared NFS maps are only read in NFS export mode.
	if CVMFS_NFS_SHARED != "" {
		_rule_nfs_shared_requires_nfs_source: true & (CVMFS_NFS_SOURCE == true)
	}

	// Tracing headers are only read when tracing is on.
	if CVMFS_HTTP_TRACING_HEADERS != "" {
		_rule_tracing_headers_require_http_tracing: true & (CVMFS_HTTP_TRACING == true)
	}

	// The aggregator needs host, port and metric name.
	if CVMFS_TELEMETRY_SEND {
		_rule_telemetry_requires_influx_endpoint: true & (CVMFS_INFLUX_HOST != "" && CVMFS_INFLUX_PORT != 0 && CVMFS_INFLUX_METRIC_NAME != "")
	}

	// The rate is only read when telemetry is on.
	if CVMFS_TELEMETRY_RATE != 0 {
		_rule_telemetry_rate_requires_telemetry_send: true & (CVMFS_TELEMETRY_SEND == true)
	}

	// Hash, tag and date all pin the revision and silently override each
	// other, so require at most one.
	_rule_at_most_one_revision_pin: true & (!(CVMFS_ROOT_HASH != "" && CVMFS_REPOSITORY_TAG != "") && !(CVMFS_ROOT_HASH != "" && CVMFS_REPOSITORY_DATE != "") && !(CVMFS_REPOSITORY_TAG != "" && CVMFS_REPOSITORY_DATE != ""))
	// Only applies to fixed catalogs.
	if CVMFS_ALT_ROOT_PATH {
		_rule_alt_root_path_requires_fixed_catalog: true & _fixedCatalog
	}

	// Only read when NFS maps are active.
	if CVMFS_NFS_INTERLEAVED_INODES != "" {
		_rule_nfs_interleaved_inodes_require_nfs_source: true & (CVMFS_NFS_SOURCE == true)
	}

	// A pinned revision disables updates, so an explicit yes contradicts it.
	if _fixedCatalog && CVMFS_AUTO_UPDATE != null {
		_rule_fixed_catalog_conflicts_with_auto_update: true & (CVMFS_AUTO_UPDATE == false)
	}

	// Strict mount fails on an empty list.
	if CVMFS_STRICT_MOUNT {
		_rule_strict_mount_requires_repositories: true & (CVMFS_REPOSITORIES != "")
	}

	// Sanity checks. Cross-parameter comparisons like these are the reason
	// for CUE: JSON Schema draft-7 cannot express them.
	_rule_backoff_init_le_backoff_max: true & (CVMFS_BACKOFF_INIT <= CVMFS_BACKOFF_MAX)
	_rule_dns_min_ttl_le_dns_max_ttl: true & (CVMFS_DNS_MIN_TTL <= CVMFS_DNS_MAX_TTL)
	// The trace buffer is only read when the tracer runs.
	if CVMFS_TRACEBUFFER != 0 || CVMFS_TRACEBUFFER_THRESHOLD != 0 {
		_rule_tracebuffer_requires_tracefile: true & (CVMFS_TRACEFILE != "")
	}

	// Needs the refcounted cache; silently ignored otherwise. Default
	// instance only.
	if CVMFS_CACHE_CLEANUP_NONOPENLRU {
		_rule_cleanup_nonopenlru_requires_refcount: true & (CVMFS_CACHE_REFCOUNT == true)
	}
}
