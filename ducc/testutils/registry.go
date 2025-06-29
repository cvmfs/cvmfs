package testutils

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/google/go-containerregistry/pkg/v1/types"
	//    "github.com/google/go-containerregistry/pkg/v1/partial"
	//"github.com/google/go-containerregistry/pkg/v1/layout"
)

const (
	registryPort = 5000
)

func main() {
	// Create a context that we can cancel
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling for graceful shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signals
		log.Println("Shutting down...")
		cancel()
	}()

	// Start the registry server
	registryServer := registry.New()
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", registryPort),
		Handler: registryServer,
	}

	// Run the server in a goroutine
	go func() {
		log.Printf("Starting registry server on port %d...\n", registryPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Registry server error: %v", err)
		}
	}()

	// Wait a moment for the server to start
	time.Sleep(500 * time.Millisecond)

	// Create and push a test image
	if err := CreateAndPushTestImage(ctx); err != nil {
		log.Fatalf("Failed to create and push test image: %v", err)
	}

	// Create and push multi-architecture test image
	if err := CreateAndPushMultiArchTestImage(ctx, registryPort); err != nil {
		log.Fatalf("Failed to create and push multi-arch test image: %v", err)
	}

	// Print instructions for using the registry
	log.Printf("\n"+
		"Registry is running at localhost:%d\n"+
		"Test images available:\n"+
		"  - localhost:%d/test-image:latest (single arch)\n"+
		"  - localhost:%d/multi-arch-test:latest (multi-arch)\n"+
		"  - localhost:%d/multi-arch-test:amd64 (amd64 specific)\n"+
		"  - localhost:%d/multi-arch-test:arm64 (arm64 specific)\n"+
		"  - localhost:%d/multi-arch-test:arm64v7 (arm64v7 specific)\n"+
		"  - localhost:%d/multi-arch-test:arm64v8 (arm64v8 specific)\n"+
		"  - localhost:%d/multi-arch-test:unknown (unknown platform)\n\n"+
		"To pull the multi-arch image with Docker, run:\n"+
		"  docker pull localhost:%d/multi-arch-test:latest\n\n"+
		"Press Ctrl+C to stop the registry\n",
		registryPort, registryPort, registryPort, registryPort,
		registryPort, registryPort, registryPort, registryPort, registryPort)

	// Wait for cancellation signal
	<-ctx.Done()

	// Create a deadline for server shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Attempt graceful shutdown
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Error during server shutdown: %v", err)
	}

	log.Println("Registry server stopped")
}

// createTarLayer creates a tar archive layer containing a single file
func createTarLayer(filePath, content string) (v1.Layer, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	// Create the tar header for the file
	header := &tar.Header{
		Name: filePath,
		Mode: 0644,
		Size: int64(len(content)),
	}

	// Write the header
	if err := tw.WriteHeader(header); err != nil {
		return nil, fmt.Errorf("failed to write tar header: %w", err)
	}

	// Write the file content
	if _, err := tw.Write([]byte(content)); err != nil {
		return nil, fmt.Errorf("failed to write file content: %w", err)
	}

	// Close the tar writer
	if err := tw.Close(); err != nil {
		return nil, fmt.Errorf("failed to close tar writer: %w", err)
	}

	// Create a layer from the tar archive
	layer, err := tarball.LayerFromReader(&buf)
	if err != nil {
		return nil, fmt.Errorf("failed to create layer from tar: %w", err)
	}

	return layer, nil
}

func CreateAndPushTestImage(ctx context.Context) error {
	log.Println("Creating test image...")

	// Start with an empty image
	img := empty.Image

	// Create layers with filesystem content
	layerSpecs := []struct {
		content  string
		filePath string
	}{
		{content: "This represents the base filesystem layer", filePath: "base/README.txt"},
		{content: "Application dependencies layer", filePath: "usr/lib/deps.txt"},
		{content: `{"name": "test-app", "version": "1.0.0"}`, filePath: "app/package.json"},
		{content: "<html><body><h1>Test Application</h1></body></html>", filePath: "var/www/index.html"},
		{content: "#!/bin/sh\necho 'Starting application...'\nexec \"$@\"\n", filePath: "usr/local/bin/entrypoint.sh"},
	}

	// Collect all layers first
	var layers []v1.Layer
	for i, spec := range layerSpecs {
		log.Printf("Creating layer %d: %s", i+1, spec.filePath)

		layer, err := createTarLayer(spec.filePath, spec.content)
		if err != nil {
			return fmt.Errorf("failed to create layer for %s: %w", spec.filePath, err)
		}

		layers = append(layers, layer)
	}

	// Add all layers to the image at once
	var err error
	img, err = mutate.AppendLayers(img, layers...)
	if err != nil {
		return fmt.Errorf("failed to append layers: %w", err)
	}

	// Verify the image has the expected number of layers
	manifest, err := img.Manifest()
	if err != nil {
		return fmt.Errorf("failed to get manifest: %w", err)
	}

	log.Printf("Image created with %d layers", len(manifest.Layers))

	// Set image configuration
	cfg, err := img.ConfigFile()
	if err != nil {
		return fmt.Errorf("failed to get config file: %w", err)
	}

	// Create new config if nil
	if cfg == nil {
		cfg = &v1.ConfigFile{
			Config: v1.Config{},
		}
	}

	// Update configuration
	cfg.Created = v1.Time{Time: time.Now()}
	cfg.Author = "Local Registry Multi-Layer Example"
	cfg.Config.Entrypoint = []string{"/usr/local/bin/entrypoint.sh"}
	cfg.Config.Cmd = []string{"echo", "Hello from multi-layer test image"}
	cfg.Config.Env = []string{
		"PATH=/usr/local/bin:/usr/bin:/bin",
		"TEST_ENV=multi-layer",
		"APP_VERSION=1.0.0",
	}
	cfg.Config.WorkingDir = "/app"

	// Apply the configuration to the image
	img, err = mutate.ConfigFile(img, cfg)
	if err != nil {
		return fmt.Errorf("failed to update config: %w", err)
	}

	// Tag and push the image to local registry
	tag, err := name.NewTag(fmt.Sprintf("localhost:%d/test-image:latest", registryPort))
	if err != nil {
		return fmt.Errorf("failed to create tag: %w", err)
	}

	log.Printf("Pushing multi-layer image to %s...", tag.String())
	if err := remote.Write(tag, img, remote.WithContext(ctx)); err != nil {
		return fmt.Errorf("failed to push image: %w", err)
	}

	log.Printf("Multi-layer test image pushed successfully with %d layers", len(manifest.Layers))
	return nil
}

func CreateAndPushMultiArchTestImage(ctx context.Context, TestRegistryPort int) error {
	log.Println("Creating multi-architecture test image...")

	// Define platforms with their specific characteristics
	platforms := []struct {
		os           string
		architecture string
		variant      string
		suffix       string
		layerPrefix  string
	}{
		{"linux", "amd64", "", "amd64", "x86_64"},
		{"linux", "arm64", "", "arm64", "aarch64"},
		{"linux", "arm64", "v7", "arm64v7", "arm64v7l"},
		{"linux", "arm64", "v8", "arm64v8", "arm64v8l"},
		{"unknown", "unknown", "", "unknown", "mystery"},
	}

	var manifestDescriptors []v1.Descriptor
	var images []v1.Image

	// Create platform-specific images
	for _, platform := range platforms {
		log.Printf("Creating image for %s/%s%s", platform.os, platform.architecture,
			func() string {
				if platform.variant != "" {
					return "/" + platform.variant
				}
				return ""
			}())

		img, err := createPlatformSpecificImage(platform.os, platform.architecture, platform.variant, platform.layerPrefix)
		if err != nil {
			return fmt.Errorf("failed to create image for %s/%s: %w", platform.os, platform.architecture, err)
		}

		// Push platform-specific image
		platformTag, err := name.NewTag(fmt.Sprintf("localhost:%d/multi-arch-test:%s", TestRegistryPort, platform.suffix))
		if err != nil {
			return fmt.Errorf("failed to create platform tag: %w", err)
		}

		log.Printf("Pushing platform image: %s", platformTag.String())
		if err := remote.Write(platformTag, img, remote.WithContext(ctx)); err != nil {
			return fmt.Errorf("failed to push platform image %s: %w", platform.suffix, err)
		}

		// Get the digest and size for the manifest list
		digest, err := img.Digest()
		if err != nil {
			return fmt.Errorf("failed to get digest for %s: %w", platform.suffix, err)
		}

		size, err := img.Size()
		if err != nil {
			return fmt.Errorf("failed to get size for %s: %w", platform.suffix, err)
		}

		manifest, err := img.Manifest()
		if err != nil {
			return fmt.Errorf("failed to get manifest for %s: %w", platform.suffix, err)
		}

		// Create descriptor for this platform
		descriptor := v1.Descriptor{
			MediaType: manifest.MediaType,
			Size:      size,
			Digest:    digest,
			Platform: &v1.Platform{
				OS:           platform.os,
				Architecture: platform.architecture,
				Variant:      platform.variant,
			},
		}

		manifestDescriptors = append(manifestDescriptors, descriptor)
		images = append(images, img)
	}

	// Create manifest list (index)
	manifestList := &v1.IndexManifest{
		SchemaVersion: 2,
		MediaType:     types.OCIImageIndex,
		Manifests:     manifestDescriptors,
	}

	// Create an index from the manifest list using the new constructor
	index, err := newManifestIndex(manifestList, images)
	if err != nil {
		return fmt.Errorf("failed to create manifest index: %w", err)
	}

	// Push the manifest list as the main tag
	mainTag, err := name.NewTag(fmt.Sprintf("localhost:%d/multi-arch-test:latest", TestRegistryPort))
	if err != nil {
		return fmt.Errorf("failed to create main tag: %w", err)
	}

	log.Printf("Pushing manifest list: %s", mainTag.String())
	if err := remote.WriteIndex(mainTag, index, remote.WithContext(ctx)); err != nil {
		return fmt.Errorf("failed to push manifest list: %w", err)
	}

	log.Printf("Multi-architecture image pushed successfully with %d platforms", len(platforms))
	return nil
}

func createPlatformSpecificImage(os, arch, variant, layerPrefix string) (v1.Image, error) {
	// Start with empty image
	img := empty.Image

	// Create platform-specific layers
	layerSpecs := []struct {
		content  string
		filePath string
	}{
		{
			content:  fmt.Sprintf("Base layer for %s/%s%s", os, arch, variant),
			filePath: fmt.Sprintf("opt/%s/base.txt", layerPrefix),
		},
		{
			content:  fmt.Sprintf("Runtime libraries compiled for %s", arch),
			filePath: fmt.Sprintf("usr/lib/%s/runtime.so", layerPrefix),
		},
		{
			content:  fmt.Sprintf(`{"platform": "%s/%s", "variant": "%s", "optimized": true}`, os, arch, variant),
			filePath: fmt.Sprintf("app/%s-config.json", layerPrefix),
		},
		{
			content:  fmt.Sprintf("#!/bin/sh\necho 'Running on %s/%s%s'\nuname -m\n", os, arch, variant),
			filePath: fmt.Sprintf("usr/local/bin/%s-startup.sh", layerPrefix),
		},
		{
			content:  fmt.Sprintf("Binary optimized for %s architecture", arch),
			filePath: fmt.Sprintf("usr/bin/app-%s", layerPrefix),
		},
	}

	// Create and collect layers
	var layers []v1.Layer
	for i, spec := range layerSpecs {
		log.Printf("  Creating layer %d for %s: %s", i+1, layerPrefix, spec.filePath)

		layer, err := createTarLayer(spec.filePath, spec.content)
		if err != nil {
			return nil, fmt.Errorf("failed to create layer for %s: %w", spec.filePath, err)
		}

		layers = append(layers, layer)
	}

	// Add all layers to image
	var err error
	img, err = mutate.AppendLayers(img, layers...)
	if err != nil {
		return nil, fmt.Errorf("failed to append layers: %w", err)
	}

	// Set platform-specific configuration
	cfg, err := img.ConfigFile()
	if err != nil {
		return nil, fmt.Errorf("failed to get config file: %w", err)
	}

	if cfg == nil {
		cfg = &v1.ConfigFile{
			Config: v1.Config{},
		}
	}

	// Update configuration with platform info
	cfg.Created = v1.Time{Time: time.Now()}
	cfg.Author = fmt.Sprintf("Multi-Arch Example (%s/%s%s)", os, arch, variant)
	cfg.OS = os
	cfg.Architecture = arch
	if variant != "" {
		cfg.Variant = variant
	}

	cfg.Config.Entrypoint = []string{fmt.Sprintf("/usr/local/bin/%s-startup.sh", layerPrefix)}
	cfg.Config.Cmd = []string{"echo", fmt.Sprintf("Hello from %s/%s", os, arch)}
	cfg.Config.Env = []string{
		"PATH=/usr/local/bin:/usr/bin:/bin",
		fmt.Sprintf("PLATFORM=%s/%s", os, arch),
		fmt.Sprintf("ARCH=%s", arch),
		fmt.Sprintf("VARIANT=%s", variant),
	}
	cfg.Config.WorkingDir = "/app"

	// Apply configuration
	img, err = mutate.ConfigFile(img, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to update config: %w", err)
	}

	return img, nil
}

// manifestIndex implements v1.ImageIndex for our manifest list
type manifestIndex struct {
	manifestList *v1.IndexManifest
	images       []v1.Image
	rawManifest  []byte
}

func newManifestIndex(manifestList *v1.IndexManifest, images []v1.Image) (*manifestIndex, error) {
	// Marshal the manifest list to get raw bytes
	rawManifest, err := json.Marshal(manifestList)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal manifest list: %w", err)
	}

	return &manifestIndex{
		manifestList: manifestList,
		images:       images,
		rawManifest:  rawManifest,
	}, nil
}

func (m *manifestIndex) MediaType() (types.MediaType, error) {
	return types.OCIImageIndex, nil
}

func (m *manifestIndex) Digest() (v1.Hash, error) {
	h, _, err := v1.SHA256(bytes.NewReader(m.rawManifest))
	return h, err
}

func (m *manifestIndex) Size() (int64, error) {
	return int64(len(m.rawManifest)), nil
}

func (m *manifestIndex) IndexManifest() (*v1.IndexManifest, error) {
	return m.manifestList, nil
}

func (m *manifestIndex) RawManifest() ([]byte, error) {
	return m.rawManifest, nil
}

func (m *manifestIndex) Image(h v1.Hash) (v1.Image, error) {
	for _, img := range m.images {
		digest, err := img.Digest()
		if err != nil {
			continue
		}
		if digest == h {
			return img, nil
		}
	}
	return nil, fmt.Errorf("image not found for digest %s", h)
}

func (m *manifestIndex) ImageIndex(h v1.Hash) (v1.ImageIndex, error) {
	return nil, fmt.Errorf("nested image indices not supported")
}
