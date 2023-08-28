package config

import "time"

const DEFAULT_CREATELAYERS = true
const DEFAULT_CREATETHINIMAGE = false
const DEFAULT_CREATEPODMAN = true
const DEFAULT_CREATEFLAT = true

const DEFAULT_UPDATEINTERVAL = time.Hour
const MIN_UPDATEINTERVAL = 5 * time.Minute
const DEFAULT_WEBHOOKENABLED = false

const TMP_FILE_PATH = "/tmp/ducc"
const CVMFS_CHAINS_DIR = ".chains"

const REGISTRY_INITIAL_BACKOFF = 1 * time.Second
const REGISTRY_MAX_BACKOFF = 5 * time.Minute
const REGISTRY_MAX_CONCURRENT_REQUESTS = 5
