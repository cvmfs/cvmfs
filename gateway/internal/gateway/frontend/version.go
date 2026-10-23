package frontend

const (
	// APIProtocolVersion is the latest supported protocol. Version 4 enables
	// receiver-side creation of missing lease ancestors.
	APIProtocolVersion = 4
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
