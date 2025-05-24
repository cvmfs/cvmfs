package main

import (
    "context"
    "fmt"
    "log"
    "net"
    "net/http"
    "os"
    "sync"
    "testing"
    "time"

    "github.com/google/go-containerregistry/pkg/name"
    "github.com/google/go-containerregistry/pkg/registry"
    "github.com/google/go-containerregistry/pkg/v1/remote"
)

var (
    testRegistryServer *http.Server
    testRegistryPort   int
    serverOnce         sync.Once
    serverMutex        sync.RWMutex
    serverReady        bool
)

// TestMain sets up and tears down the test registry server
func TestMain(m *testing.M) {
    // Start the registry server
    if err := startTestRegistryServer(); err != nil {
        log.Fatalf("Failed to start test registry server: %v", err)
    }

    // Run tests
    code := m.Run()

    // Clean up
    stopTestRegistryServer()

    // Exit with the test result code
    os.Exit(code)
}

// startTestRegistryServer starts the registry server once
func startTestRegistryServer() error {
    var startErr error

    serverOnce.Do(func() {
        // Find an available port
        listener, err := net.Listen("tcp", ":0")
        if err != nil {
            startErr = fmt.Errorf("failed to find available port: %w", err)
            return
        }

        testRegistryPort = listener.Addr().(*net.TCPAddr).Port
        listener.Close()

        // Create registry server
        registryHandler := registry.New()
        testRegistryServer = &http.Server{
            Addr:    fmt.Sprintf(":%d", testRegistryPort),
            Handler: registryHandler,
        }

        // Start server in background
        go func() {
            log.Printf("Starting test registry server on port %d", testRegistryPort)
            if err := testRegistryServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
                log.Printf("Test registry server error: %v", err)
            }
        }()

        // Wait for server to be ready
        if err := waitForServerReady(fmt.Sprintf("localhost:%d", testRegistryPort), 30*time.Second); err != nil {
            startErr = fmt.Errorf("server failed to become ready: %w", err)
            return
        }

        serverMutex.Lock()
        serverReady = true
        serverMutex.Unlock()

        log.Printf("Test registry server ready at localhost:%d", testRegistryPort)
    })

    return startErr
}

// stopTestRegistryServer gracefully stops the registry server
func stopTestRegistryServer() {
    if testRegistryServer != nil {
        log.Println("Stopping test registry server...")

        ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        defer cancel()

        if err := testRegistryServer.Shutdown(ctx); err != nil {
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

// getTestRegistryURL returns the test registry URL
func getTestRegistryURL() string {
    serverMutex.RLock()
    defer serverMutex.RUnlock()

    if !serverReady {
        panic("test registry server not ready")
    }

    return fmt.Sprintf("localhost:%d", testRegistryPort)
}

// Helper function to create test images
func createTestImageForTests(ctx context.Context, imageName string) error {
    img, err := createPlatformSpecificImage("linux", "amd64", "", "test")
    if err != nil {
        return fmt.Errorf("failed to create test image: %w", err)
    }

    tag, err := name.NewTag(fmt.Sprintf("%s/%s", getTestRegistryURL(), imageName))
    if err != nil {
        return fmt.Errorf("failed to create tag: %w", err)
    }

    return remote.Write(tag, img, remote.WithContext(ctx))
}

// Example test functions
func TestRegistryServer(t *testing.T) {
    tests := []struct {
        name string
        test func(t *testing.T)
    }{
        {"TestPushAndPullImage", testPushAndPullImage},
        {"TestMultipleImages", testMultipleImages},
        {"TestImageLayers", testImageLayers},
    }

    for _, tt := range tests {
        t.Run(tt.name, tt.test)
    }
}

func testPushAndPullImage(t *testing.T) {
    ctx := context.Background()
    imageName := "test-image:v1.0.0"

    // Create and push test image
    if err := createTestImageForTests(ctx, imageName); err != nil {
        t.Fatalf("Failed to push test image: %v", err)
    }

    // Pull the image back
    tag, err := name.NewTag(fmt.Sprintf("%s/%s", getTestRegistryURL(), imageName))
    if err != nil {
        t.Fatalf("Failed to create tag: %v", err)
    }

    img, err := remote.Image(tag, remote.WithContext(ctx))
    if err != nil {
        t.Fatalf("Failed to pull image: %v", err)
    }

    // Verify image properties
    manifest, err := img.Manifest()
    if err != nil {
        t.Fatalf("Failed to get manifest: %v", err)
    }

    if len(manifest.Layers) == 0 {
        t.Error("Expected image to have layers")
    }

    t.Logf("Successfully pushed and pulled image with %d layers", len(manifest.Layers))
}

func testMultipleImages(t *testing.T) {
    ctx := context.Background()

    imageNames := []string{
        "app1:latest",
        "app2:v1.0",
        "app3:dev",
    }

    // Push multiple images
    for _, imageName := range imageNames {
        if err := createTestImageForTests(ctx, imageName); err != nil {
            t.Fatalf("Failed to push image %s: %v", imageName, err)
        }
    }

    // Verify all images can be pulled
    for _, imageName := range imageNames {
        tag, err := name.NewTag(fmt.Sprintf("%s/%s", getTestRegistryURL(), imageName))
        if err != nil {
            t.Fatalf("Failed to create tag for %s: %v", imageName, err)
        }

        _, err = remote.Image(tag, remote.WithContext(ctx))
        if err != nil {
            t.Fatalf("Failed to pull image %s: %v", imageName, err)
        }
    }

    t.Logf("Successfully handled %d different images", len(imageNames))
}

func testImageLayers(t *testing.T) {
    ctx := context.Background()
    imageName := "layered-image:test"

    // Create and push test image
    if err := createTestImageForTests(ctx, imageName); err != nil {
        t.Fatalf("Failed to push test image: %v", err)
    }

    // Pull and verify layers
    tag, err := name.NewTag(fmt.Sprintf("%s/%s", getTestRegistryURL(), imageName))
    if err != nil {
        t.Fatalf("Failed to create tag: %v", err)
    }

    img, err := remote.Image(tag, remote.WithContext(ctx))
    if err != nil {
        t.Fatalf("Failed to pull image: %v", err)
    }

    layers, err := img.Layers()
    if err != nil {
        t.Fatalf("Failed to get layers: %v", err)
    }

    expectedLayerCount := 5 // Based on createPlatformSpecificImage
    if len(layers) != expectedLayerCount {
        t.Errorf("Expected %d layers, got %d", expectedLayerCount, len(layers))
    }

    // Verify each layer has content
    for i, layer := range layers {
        size, err := layer.Size()
        if err != nil {
            t.Errorf("Failed to get size for layer %d: %v", i, err)
            continue
        }

        if size == 0 {
            t.Errorf("Layer %d has zero size", i)
        }

        t.Logf("Layer %d: %d bytes", i, size)
    }
}

// Benchmark example
func BenchmarkImageOperations(b *testing.B) {
    ctx := context.Background()

    b.ResetTimer()

    for i := 0; i < b.N; i++ {
        imageName := fmt.Sprintf("bench-image:%d", i)

        // Push image
        if err := createTestImageForTests(ctx, imageName); err != nil {
            b.Fatalf("Failed to push image: %v", err)
        }

        // Pull image
        tag, err := name.NewTag(fmt.Sprintf("%s/%s", getTestRegistryURL(), imageName))
        if err != nil {
            b.Fatalf("Failed to create tag: %v", err)
        }

        _, err = remote.Image(tag, remote.WithContext(ctx))
        if err != nil {
            b.Fatalf("Failed to pull image: %v", err)
        }
    }
}

// Parallel test example
func TestParallelOperations(t *testing.T) {
    t.Parallel()

    ctx := context.Background()

    // Run multiple operations in parallel
    t.Run("ParallelPushes", func(t *testing.T) {
        t.Parallel()

        for i := 0; i < 5; i++ {
            i := i // capture loop variable
            t.Run(fmt.Sprintf("Push%d", i), func(t *testing.T) {
                t.Parallel()

                imageName := fmt.Sprintf("parallel-image:%d", i)
                if err := createTestImageForTests(ctx, imageName); err != nil {
                    t.Errorf("Failed to push image %s: %v", imageName, err)
                }
            })
        }
    })
}

