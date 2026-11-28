package pkg

import (
	"fmt"
	"net"
	"runtime"
	"strconv"
	"testing"
)

func TestSendTelegrafMetrics(t *testing.T) {
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}

	svr, err := net.ListenUDP("udp", addr)
	if err != nil {
		panic(err)
	}
	defer svr.Close()

	numCpus := runtime.NumCPU()

	statStr := "ioFwWorkers=0,computeFwWorkers=0,fwHashers=0,uploadWorkers=0,uploadHashers=0,uploadUploaders=0,coreAllotment=0,numCpus=" + strconv.Itoa(numCpus) + ",fwDelta=0,fwSrcDirents=0,fwDestDirents,uploadFileCount=0,uploadDelta=0,uploadRate=0,uploadSize=0,graftDelta=0,numGraftFiles=0,numGraftDirs=0,numGraftLinks=0,numGraftDeletions=0"

	SendTelegrafStatistics(statStr, svr.LocalAddr().String())

	want := []byte(statStr)

	buf := make([]byte, 4096)
	n, err := svr.Read(buf)
	if err != nil {
		panic(err)
	}
	if len(want) != n || string(want) != string(buf[:n]) {
		fmt.Println(want)
		fmt.Println(buf[:n])
		t.Fatalf("incorrect buffer read")
	}
	// assert.NoError(false, err)
	// assert.Equal(false, len(want), n)
	// assert.Equal(false, want, buf[:n])
}
