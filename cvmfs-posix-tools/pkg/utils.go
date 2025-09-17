package pkg

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"runtime"
	"strings"
	"syscall"

	pathlib "github.com/chigopher/pathlib"
	"github.com/rs/zerolog/log"
)

func EscapeCVMFSURL(urlPath string) string {
	inputBytes := []byte(urlPath)
	var output strings.Builder
	output.Grow(len(inputBytes))

	for i := 0; i < len(inputBytes); i++ {
		inputChar := inputBytes[i]
		if ((inputChar >= '0') && (inputChar <= '9')) ||
			((inputChar >= 'A') && (inputChar <= 'Z')) ||
			((inputChar >= 'a') && (inputChar <= 'z')) ||
			(inputChar == '/') || (inputChar == ':') || (inputChar == '.') ||
			(inputChar == '@') ||
			(inputChar == '+') || (inputChar == '-') ||
			(inputChar == '_') || (inputChar == '~') ||
			(inputChar == '[') || (inputChar == ']') || (inputChar == ',') {
			output.WriteByte(inputChar)
		} else {
			output.WriteByte('%')
			output.WriteString(strings.ToUpper(hex.EncodeToString(inputBytes[i : i+1])))
		}
	}

	return output.String()
}

func EscapeLineFeedAndCarriageReturn(urlPath string) string {
	inputBytes := []byte(urlPath)
	var output strings.Builder
	output.Grow(len(inputBytes))

	for i := 0; i < len(inputBytes); i++ {
		inputChar := inputBytes[i]
		if inputChar == '\n' || inputChar == '\r' {
			output.WriteByte('%')
			output.WriteString(strings.ToUpper(hex.EncodeToString(inputBytes[i : i+1])))
		} else {
			output.WriteByte(inputChar)
		}
	}

	return output.String()
}

// It does a copy with a larger buffer to get around go's bad copy buffers
func CopyBuffer(dst io.Writer, src io.Reader, buf []byte) (written int64, err error) {
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw < 0 || nr < nw {
				nw = 0
				if ew == nil {
					ew = errors.New("invalid write result")
				}
			}
			written += int64(nw)
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return written, err
}

// Convert a bool to an int (true == 1, false == 0)
func BoolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func GetPathsFromFile(rootDir string, file string) ([]*pathlib.Path, error) {
	paths := []*pathlib.Path{}
	rootPath := pathlib.NewPath(rootDir)

	f, err := os.Open(file)
	if err != nil {
		log.Error().Err(err).Msg("Error reading file.")
		return nil, err
	}

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		paths = append(paths, rootPath.Join(scanner.Text()))
	}

	return paths, nil
}

func GetPathStringsFromFile(file string) ([]string, error) {
	pathStrings := []string{}

	f, err := os.Open(file)
	if err != nil {
		log.Error().Err(err).Msg("Error reading file.")
		return nil, err
	}

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		pathStrings = append(pathStrings, scanner.Text())
	}

	return pathStrings, nil
}

// From: https://stackoverflow.com/a/65865898
func isErrorAddressAlreadyInUse(err error) bool {
	var eOsSyscall *os.SyscallError
	if !errors.As(err, &eOsSyscall) {
		return false
	}
	var errErrno syscall.Errno // doesn't need a "*" (ptr) because it's already a ptr (uintptr)
	if !errors.As(eOsSyscall, &errErrno) {
		return false
	}
	if errErrno == syscall.EADDRINUSE {
		return true
	}
	return false
}

func SetupPprofWebserver() error {
	var (
		addr string
		ln   net.Listener
	)

	for port := 6060; ; port++ {
		var err error
		addr = fmt.Sprintf("0.0.0.0:%d", port)
		ln, err = net.Listen("tcp", addr)
		if err != nil {
			if isErrorAddressAlreadyInUse(err) {
				continue
			}
			log.Error().Err(err).Msg("pprof HTTP server failed")
			return err
		}
		break
	}

	log.Info().Str("Addr", addr).Msg("Pprof Webserver opened on this addr:")

	if err := http.Serve(ln, nil); err != nil {
		log.Error().Err(err).Msg("pprof HTTP server failed")
		return err
	}
	return nil
}

// Set the max number of cpus that can be executing simultaneously
func SetMaxProcs() int {
	numCpus := runtime.NumCPU()
	_, slurmJobExists := os.LookupEnv(SlurmJobCpusPerNode)
	if !slurmJobExists {
		// Adds a hard cap for number of cores taken if not specified with slurm
		numCpus = min(numCpus, MaxCoresTaken)
	}
	runtime.GOMAXPROCS(numCpus)
	return numCpus
}

func MD5HashURL(input string) string {
	hash := md5.Sum([]byte(input))
	return hex.EncodeToString(hash[:]) // Already lowercase by default
}
