// CernVM-FS client configuration generator

package cvmfs

// CUE has no min/max operators, only on lists
import "list"

// Contract for the JSON payload that Bash sends through Go
#Facts: {
	cache_avail_mb: #UInt | *16000
	memory_mb:      #PosInt | *1024
	nfiles_max:     #PosInt | *132096
	proxy_chain:    #ProxyChain | *"DIRECT"
	// true when the network advertises a proxy over WPAD
	wpad_found: bool | *false
	// zero when the network could not be measured
	rtt_ms: #UInt | *0
}

facts: #Facts

// The parameters to write, in this order.
tuned: {
	// Take a quarter of the free disk but no more than 20000MB
	CVMFS_QUOTA_LIMIT: list.Min([20000, quo(facts.cache_avail_mb, 4)])
	// Take the file-descriptor limit the cvmfs service runs under, leave 1024 spare, but no more than 131072
	CVMFS_NFILES: list.Min([facts.nfiles_max-1024, 131072])
	// 1/64 of RAM, at most 128 MB.
	CVMFS_MEMCACHE_SIZE: list.Min([quo(facts.memory_mb, 64), 128])
	if facts.wpad_found {
		// Where to look for proxy settings, auto means http://wpad/wpad.dat
		CVMFS_PAC_URLS: "auto"
		// Try the discovered proxy first, if it fails default to DIRECT
		CVMFS_HTTP_PROXY: "auto;DIRECT"
	}
	if !facts.wpad_found {
		// Keep what was fed into the JSON payload
		CVMFS_HTTP_PROXY: facts.proxy_chain
	}

	// Four Round-Trips in seconds, but no less than 5s
	CVMFS_TIMEOUT: list.Max([5, quo(facts.rtt_ms*4, 1000)])
	// Eight Round-Trips in seconds, but no less than 10s
	CVMFS_TIMEOUT_DIRECT: list.Max([10, quo(facts.rtt_ms*8, 1000)])
}

tunedConfig: #ClientConfig & tuned
