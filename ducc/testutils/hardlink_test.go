package testutils

import "testing"

// TestCraftedLayerHasDanglingHardlink verifies that the crafted layer actually
// reproduces the bug condition: a hardlink whose target is absent from the
// layer's own tar.  This is hermetic (no registry/network needed).
func TestCraftedLayerHasDanglingHardlink(t *testing.T) {
	layer, err := CreateDanglingHardlinkLayer()
	if err != nil {
		t.Fatalf("CreateDanglingHardlinkLayer: %v", err)
	}
	dangling, err := layerHasDanglingHardlink(layer, DanglingHardlinkTargetPath)
	if err != nil {
		t.Fatalf("scan layer: %v", err)
	}
	if !dangling {
		t.Fatalf("crafted layer does not contain a dangling hardlink to %q",
			DanglingHardlinkTargetPath)
	}
}

func TestCraftedImageBuilds(t *testing.T) {
	img, err := CreateDanglingHardlinkImage()
	if err != nil {
		t.Fatalf("CreateDanglingHardlinkImage: %v", err)
	}
	layers, err := img.Layers()
	if err != nil {
		t.Fatalf("img.Layers: %v", err)
	}
	if len(layers) != 1 {
		t.Fatalf("expected 1 layer, got %d", len(layers))
	}
}
