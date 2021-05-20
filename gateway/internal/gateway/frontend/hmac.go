package frontend

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/hex"
)

// ComputeHMAC of a message using a specific key
func ComputeHMAC(message []byte, key string) []byte {
	mac := hmac.New(sha1.New, []byte(key))
	mac.Write(message)
	return []byte(hex.EncodeToString(mac.Sum(nil)))
}

// CheckHMAC of a message
func CheckHMAC(message, messageHMAC []byte, key string) bool {
	return hmac.Equal(messageHMAC, ComputeHMAC(message, key))
}
