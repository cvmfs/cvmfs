package frontend

const (
	// APIProtocolVersion is the latest API protocol version understood by the
	// server
	APIProtocolVersion = 2
	// APIRoot is the current HTTP API root
	APIRoot = "/api/v1"
)

// MinAPIProtocolVersion return the minimal API protocol version that can be
// negociated during an exchange
func MinAPIProtocolVersion() int {
	return APIProtocolVersion
}
