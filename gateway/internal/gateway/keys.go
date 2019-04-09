package gateway

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/pkg/errors"
)

// LoadKey from the specified file; returns the key id, key secret and optional error
func LoadKey(keyFile string) (string, string, error) {
	f, err := os.Open(keyFile)
	if err != nil {
		return "", "", errors.Wrap(err, "could not open key file for reading")
	}
	defer f.Close()
	return loadKeyFromReader(f)
}

func loadKeyFromReader(rd io.Reader) (string, string, error) {
	sc := bufio.NewScanner(rd)
	sc.Scan()
	l := sc.Text()
	tokens := strings.Fields(l)
	if len(tokens) != 3 {
		return "", "", fmt.Errorf("invalid key file format: len(tokens) == %v", len(tokens))
	}
	switch tokens[0] {
	case "plain_text":
		return tokens[1], tokens[2], nil
	default:
		return "", "", fmt.Errorf("invalid key type found: %v", tokens[0])
	}
}
