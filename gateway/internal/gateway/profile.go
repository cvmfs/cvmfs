package gateway

import (
	"os"
	"runtime/pprof"
)

// EnableCPUProfiling and save output to the specified file
func EnableCPUProfiling(output string) error {
	f, err := os.Create(output)
	if err != nil {
		return err
	}
	pprof.StartCPUProfile(f)
	return nil
}
