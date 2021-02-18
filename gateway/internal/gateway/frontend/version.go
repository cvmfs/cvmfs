package frontend

const (
	// APIProtocolVersion is the latest API protocol version understood by the
	// server
	APIProtocolVersion = 3
	// MinAPIProtocolVersion is the oldest API protocol version understood by the
	// server
	MinAPIProtocolVersion = 2
	// APIRoot is the current HTTP API root
	APIRoot = "/api/v1"
)

// MaxAPIVersion returns min(requestVersion, APIProtocolVersion)
func MaxAPIVersion(requestVersion int) int {
	maxVer := requestVersion
	if maxVer > APIProtocolVersion {
		maxVer = APIProtocolVersion
	}
	return maxVer
}
