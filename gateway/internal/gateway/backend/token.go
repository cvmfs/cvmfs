package backend

import (
	"encoding/base32"
	"math/rand"
	"time"
)

var rng *rand.Rand

func init() {
	rng = rand.New(rand.NewSource(time.Now().UnixNano()))
}

func NewLeaseToken() string {
	tokenBytes := make([]byte, 32)
	rng.Read(tokenBytes)
	return base32.StdEncoding.EncodeToString(tokenBytes)
}
