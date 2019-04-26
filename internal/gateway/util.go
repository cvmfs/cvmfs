package gateway

// ContextKey is a type alias for additional Context keys
type ContextKey int

// List of different context keys in use
const (
	IDKey ContextKey = iota
	T0Key
)
