package lib

import (
	"crypto/rand"
	"encoding/base32"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
	"time"
	"golang.org/x/sys/unix"
)

func generateID(l int) string {
	const (
		maxretries = 9
		backoff    = time.Millisecond * 10
	)

	var (
		totalBackoff time.Duration
		count        int
		retries      int
		size         = (l*5 + 7) / 8
		u            = make([]byte, size)
	)

	for {
		b := time.Duration(retries) * backoff
		time.Sleep(b)
		totalBackoff += b

		n, err := io.ReadFull(rand.Reader, u[count:])
		if err != nil {
			if retryOnError(err) && retries < maxretries {
				count += n
				retries++
				fmt.Printf("error generating version 4 uuid, retrying: %v", err)
				continue
			}

			panic(fmt.Errorf("error reading random number generator, retried for %v: %v", totalBackoff.String(), err))
		}

		break
	}

	s := base32.StdEncoding.EncodeToString(u)

	return s[:l]
}

// retryOnError tries to detect whether or not retrying would be fruitful.
func retryOnError(err error) bool {
	switch err := err.(type) {
	case *os.PathError:
		return retryOnError(err.Err) // unpack the target error
	case syscall.Errno:
		if err == unix.EPERM {
			// EPERM represents an entropy pool exhaustion, a condition under
			// which we backoff and retry.
			return true
		}
	}

	return false
}

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
