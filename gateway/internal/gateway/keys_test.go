package gateway

import (
	"strings"
	"testing"
)

func TestValidKeys(t *testing.T) {
	t.Run("minimal", func(t *testing.T) {
		id, sec, err := loadKeyFromReader(strings.NewReader("plain_text id secret"))
		if err != nil {
			t.Fatalf("could not parse key: %v", err)
		}
		if id != "id" || sec != "secret" {
			t.Fatalf("key was parsed incorrectly: %v / %v", id, sec)
		}
	})
	t.Run("multiple spaces", func(t *testing.T) {
		id, sec, err := loadKeyFromReader(strings.NewReader("    plain_text   id   secret    "))
		if err != nil {
			t.Fatalf("could not parse key: %v", err)
		}
		if id != "id" || sec != "secret" {
			t.Fatalf("key was parsed incorrectly: %v / %v", id, sec)
		}
	})
	t.Run("tabs", func(t *testing.T) {
		id, sec, err := loadKeyFromReader(strings.NewReader("\tplain_text\tid\tsecret\t"))
		if err != nil {
			t.Fatalf("could not parse key: %v", err)
		}
		if id != "id" || sec != "secret" {
			t.Fatalf("key was parsed incorrectly: %v / %v", id, sec)
		}
	})
	t.Run("tabs spaces and newlines", func(t *testing.T) {
		id, sec, err := loadKeyFromReader(strings.NewReader(" \t   plain_text \t  id \t  secret \t \n  "))
		if err != nil {
			t.Fatalf("could not parse key: %v", err)
		}
		if id != "id" || sec != "secret" {
			t.Fatalf("key was parsed incorrectly: %v / %v", id, sec)
		}
	})
	t.Run("duplicate chars", func(t *testing.T) {
		id, sec, err := loadKeyFromReader(strings.NewReader("plain_text key111 sseeccrreett"))
		if err != nil {
			t.Fatalf("could not parse key: %v", err)
		}
		if id != "key111" || sec != "sseeccrreett" {
			t.Fatalf("key was parsed incorrectly: %v / %v", id, sec)
		}
	})
}

func TestInvalidKeys(t *testing.T) {
	t.Run("invalid key type", func(t *testing.T) {
		_, _, err := loadKeyFromReader(strings.NewReader("plane_text id secret"))
		if err == nil {
			t.Fatalf("invalid key accepted")
		}
	})
	t.Run("invalid format - missing tokens", func(t *testing.T) {
		_, _, err := loadKeyFromReader(strings.NewReader("plain_textid secret"))
		if err == nil {
			t.Fatalf("invalid key accepted")
		}
	})
	t.Run("invalid format - trailing garbage", func(t *testing.T) {
		_, _, err := loadKeyFromReader(strings.NewReader("plain_text id secret garbage"))
		if err == nil {
			t.Fatalf("invalid key accepted")
		}
	})
}
