package gateway

import (
	"fmt"
	"os"
)

// ContextKey is a type alias for additional Context keys
type ContextKey int

// List of different context keys in use
const (
	IDKey ContextKey = iota
	T0Key
)

// WritePIDFile write the process id to the specified file
func WritePIDFile(fileName string) error {
	pidFile, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer pidFile.Close()

	pidFile.Write([]byte(fmt.Sprintf("%v", os.Getpid())))

	return nil
}
