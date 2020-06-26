package lib

import (
	"encoding/base32"
	"encoding/base64"
	"strings"

	"github.com/google/uuid"
)

//generates the file name for link dir in podman store
func generateID(l int) string {
	id := uuid.New()
	bytes, _ := id.MarshalText()
	s := base32.StdEncoding.EncodeToString(bytes)

	return s[:l]
}

//generates the file name for config file (compliant with libpod) in podman store.
func generateConfigFileName(digest string) (fname string, err error) {
	reader := strings.NewReader(digest)
	for reader.Len() > 0 {
		ch, size, err := reader.ReadRune()
		if err != nil || size != 1 {
			break
		}
		if ch != '.' && !(ch >= '0' && ch <= '9') && !(ch >= 'a' && ch <= 'z') {
			break
		}
	}
	if reader.Len() > 0 {
		fname = "=" + base64.StdEncoding.EncodeToString([]byte(digest))
	}
	return
}
