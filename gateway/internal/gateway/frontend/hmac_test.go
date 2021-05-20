package frontend

import (
	"testing"
)

func TestHMAC(t *testing.T) {
	key := "Qui?"

	msg1 := []byte("Hello is it me you're looking for?")
	msg2 := msg1[1:]

	hmac := ComputeHMAC(msg1, key)

	if !CheckHMAC(msg1, hmac, key) {
		t.Errorf("HMAC verification failed")
	}

	// msg2 should produce a different HMAC
	if CheckHMAC(msg2, hmac, key) {
		t.Errorf("HMAC of msg2 should not be the same as for msg1")
	}
}
