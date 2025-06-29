package testutils

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

var (
	MyTestRegistryPort int
)

// TestMain sets up and tears down the test registry server
func TestMain(m *testing.M) {
	// Start the registry server
	var MyTestRegistryServer *http.Server
	var err error
	if MyTestRegistryServer, MyTestRegistryPort, err = StartTestRegistryServer(); err != nil {
		log.Fatalf("Failed to start test registry server: %v ", err)
	}

	// Run tests
	code := m.Run()

	// Clean up
	StopTestRegistryServer(MyTestRegistryServer)

	// Exit with the test result code
	os.Exit(code)
}

// Example test functions
func TestRegistryServerVarious(t *testing.T) {
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
	if err := CreateTestImageForTests(ctx, MyTestRegistryPort, imageName); err != nil {
		t.Fatalf("Failed to push test image: %v", err)
	}

	// Pull the image back
	tag, err := name.NewTag(fmt.Sprintf("%s/%s", GetTestRegistryURL(MyTestRegistryPort), imageName))
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
		if err := CreateTestImageForTests(ctx, MyTestRegistryPort, imageName); err != nil {
			t.Fatalf("Failed to push image %s: %v", imageName, err)
		}
	}

	// Verify all images can be pulled
	for _, imageName := range imageNames {
		tag, err := name.NewTag(fmt.Sprintf("%s/%s", GetTestRegistryURL(MyTestRegistryPort), imageName))
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
	if err := CreateTestImageForTests(ctx, MyTestRegistryPort, imageName); err != nil {
		t.Fatalf("Failed to push test image: %v", err)
	}

	// Pull and verify layers
	tag, err := name.NewTag(fmt.Sprintf("%s/%s", GetTestRegistryURL(MyTestRegistryPort), imageName))
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
		if err := CreateTestImageForTests(ctx, MyTestRegistryPort, imageName); err != nil {
			b.Fatalf("Failed to push image: %v", err)
		}

		// Pull image
		tag, err := name.NewTag(fmt.Sprintf("%s/%s", GetTestRegistryURL(MyTestRegistryPort), imageName))
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
				if err := CreateTestImageForTests(ctx, MyTestRegistryPort, imageName); err != nil {
					t.Errorf("Failed to push image %s: %v", imageName, err)
				}
			})
		}
	})
}
