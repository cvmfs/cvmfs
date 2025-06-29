package testutils

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

var (
	serverOnce  sync.Once
	serverMutex sync.RWMutex
	serverReady bool
)

// startTestRegistryServer starts the registry server once
func StartTestRegistryServer() (TestRegistryServer *http.Server, TestRegistryPort int, err error) {
	var startErr error

	serverOnce.Do(func() {
		// Find an available port
		listener, err := net.Listen("tcp", ":0")
		if err != nil {
			startErr = fmt.Errorf("failed to find available port: %w", err)
			return
		}

		TestRegistryPort = listener.Addr().(*net.TCPAddr).Port
		listener.Close()

		// Create registry server
		registryHandler := registry.New()
		TestRegistryServer = &http.Server{
			Addr:    fmt.Sprintf(":%d", TestRegistryPort),
			Handler: registryHandler,
		}

		// Start server in background
		go func() {
			log.Printf("Starting test registry server on port %d", TestRegistryPort)
			if err := TestRegistryServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("Test registry server error: %v", err)
			}
		}()

		// Wait for server to be ready
		if err := waitForServerReady(fmt.Sprintf("localhost:%d", TestRegistryPort), 30*time.Second); err != nil {
			startErr = fmt.Errorf("server failed to become ready: %w", err)
			return
		}

		serverMutex.Lock()
		serverReady = true
		serverMutex.Unlock()

		log.Printf("Test registry server ready at localhost:%d", TestRegistryPort)
	})

	return TestRegistryServer, TestRegistryPort, startErr
}

// stopTestRegistryServer gracefully stops the registry server
func StopTestRegistryServer(TestRegistryServer *http.Server) {
	if TestRegistryServer != nil {
		log.Println("Stopping test registry server...")

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := TestRegistryServer.Shutdown(ctx); err != nil {
			log.Printf("Error stopping test registry server: %v", err)
		} else {
			log.Println("Test registry server stopped")
		}
	}
}

// waitForServerReady waits for the server to accept connections
func waitForServerReady(address string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for server to be ready")
		case <-ticker.C:
			conn, err := net.DialTimeout("tcp", address, 1*time.Second)
			if err == nil {
				conn.Close()
				return nil
			}
		}
	}
}

// Helper function to create test images
func CreateTestImageForTests(ctx context.Context, TestRegistryPort int, imageName string) error {
	img, err := createPlatformSpecificImage("linux", "amd64", "", "test")
	if err != nil {
		return fmt.Errorf("failed to create test image: %w", err)
	}

	tag, err := name.NewTag(fmt.Sprintf("localhost:%d/%s", TestRegistryPort, imageName))
	if err != nil {
		return fmt.Errorf("failed to create tag: %w", err)
	}

	return remote.Write(tag, img, remote.WithContext(ctx))
}

// getTestRegistryURL returns the test registry URL
func GetTestRegistryURL(TestRegistryPort int) string {
	serverMutex.RLock()
	defer serverMutex.RUnlock()

	if !serverReady {
		panic("test registry server not ready")
	}

	return fmt.Sprintf("localhost:%d", TestRegistryPort)
}
