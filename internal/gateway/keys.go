package gateway

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/pkg/errors"
)

// Key is a gateway API key composed of an ID string and a secret secret
type Key struct {
	ID     string
	Secret string
}

// LoadKey from the specified file
func LoadKey(keyFile string) (*Key, error) {
	f, err := os.Open(keyFile)
	if err != nil {
		return nil, errors.Wrap(err, "could not open key file for reading")
	}
	defer f.Close()
	return loadKeyFromReader(f)
}

func loadKeyFromReader(rd io.Reader) (*Key, error) {
	brd := bufio.NewReader(rd)
	l, err := brd.ReadString('\n')
	if err != nil {
		return nil, errors.Wrap(err, "could not read first line of key file")
	}
	tokens := strings.Fields(l)
	if len(tokens) != 3 {
		return nil, errors.Wrap(err, "invalid key file format (missing tokens)")
	}
	switch tokens[0] {
	case "plain_text":
		return &Key{ID: tokens[1], Secret: tokens[2]}, nil
	default:
		return nil, fmt.Errorf("invalid key type found: %v", tokens[0])
	}
}
