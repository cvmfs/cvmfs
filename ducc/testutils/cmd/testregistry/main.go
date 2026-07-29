// Command testregistry runs an in-process OCI registry on a fixed address and
// seeds it with the crafted dangling-hardlink image (and any extra images
// requested).  It is used by integration tests so that `cvmfs_ducc convert`
// has a local image source without needing an external registry.
//
//	testregistry -addr :5000 -ref ducc-test/dangling-hardlink:latest
//
// The process serves until killed (SIGINT/SIGTERM).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/go-containerregistry/pkg/registry"

	"github.com/cvmfs/ducc/testutils"
)

func main() {
	addr := flag.String("addr", ":5000", "address to listen on")
	ref := flag.String("ref", "ducc-test/dangling-hardlink:latest",
		"repository:tag for the crafted dangling-hardlink image")
	flag.Parse()

	srv := &http.Server{Addr: *addr, Handler: registry.New()}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen on %s: %v", *addr, err)
	}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Fatalf("registry serve: %v", err)
		}
	}()

	// The push target uses the concrete host:port the listener bound to.
	host := *addr
	if strings.HasPrefix(host, ":") {
		host = "localhost" + host
	}
	fullRef := fmt.Sprintf("%s/%s", host, *ref)

	ctx := context.Background()
	if err := waitReady(host, 15*time.Second); err != nil {
		log.Fatalf("registry not ready: %v", err)
	}
	if err := testutils.PushDanglingHardlinkImage(ctx, fullRef); err != nil {
		log.Fatalf("push dangling-hardlink image: %v", err)
	}
	log.Printf("ready: pushed dangling-hardlink image to %s", fullRef)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

func waitReady(address string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, time.Second)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s", address)
}
