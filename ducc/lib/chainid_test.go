package lib

import (
	"testing"
)

func TestGetDiffIDs(t *testing.T) {
	imageName := "https://registry.hub.docker.com/library/redis:@sha256:b33e5a3c00e5794324fad2fab650eadba0f65e625cc915e4e57995590502c269"
	img, err := ParseImage(imageName)
	if err != nil {
		t.Error("Unable to parse the image")
	}
	diffIDs, err := img.GetDiffIDs()
	if err != nil {
		t.Error("Unable to get the diffID")
	}

	expectedDiffIDs := []string{
		"sha256:6744ca1b11903f4db4d5e26145f6dd20f9a6d321a7f725f1a0a7a45a4174c579",
		"sha256:4f442ee57ce8d8bd2cd0a84b07404f1bfb982999f568c46ea8883d0164179d25",
		"sha256:0a5fa8924bd600cdf8167cd211dc595206c1da11c4f232b1cd39e81c8267ddbc",
		"sha256:23cfd55841513b43cbc862c6304684a7759d415be303ba443bf52a8e076fedc6",
		"sha256:036b23f466cade4db1ca12629b98204ed5e88599fe1f3d571e8832e920f7c93d",
		"sha256:0ea23dbb18ab8cf7683d80f4526033d096e43deee4c68abd1e11ab32cddd6262",
	}

	if len(diffIDs) != len(expectedDiffIDs) {
		t.Errorf("Returned a different number of DiffIDs than expected %d vs %d", len(diffIDs), len(expectedDiffIDs))
	}
	for i, diffID := range diffIDs {
		if diffID.String() != expectedDiffIDs[i] {
			t.Errorf("Wrong DiffID returned in position %d. \n %s vs %s", i, diffID, expectedDiffIDs[i])
		}
	}
}

func TestGetChainIDs(t *testing.T) {
	imageName := "https://registry.hub.docker.com/library/redis:@sha256:b33e5a3c00e5794324fad2fab650eadba0f65e625cc915e4e57995590502c269"
	img, err := ParseImage(imageName)
	if err != nil {
		t.Error("Unable to parse the image")
	}
	diffIDs, err := img.GetDiffIDs()
	if err != nil {
		t.Error("Unable to get the diffID")
	}
	chain := ChainIDFromLayers(diffIDs)
	for i, baseID := range chain.BaseLayers {
		if baseID != diffIDs[i] {
			t.Errorf("Wrong base layer in position %d. %s vs %s", i, baseID, diffIDs[i])
		}
	}
	expectedChainID, _ := MapStringToDigest([]string{
		"sha256:6744ca1b11903f4db4d5e26145f6dd20f9a6d321a7f725f1a0a7a45a4174c579",
		"sha256:b3bcf5ea8345bc3bff9cc11fe0b0ffaf89edf2dc085ce8fb8e020075d192004a",
		"sha256:9c6b6e118e74cba1ab3978e46bdfbcca56d0738a24ebab4b3e75d4a06b931386",
		"sha256:640d7d5e90b3829c5393959bf1d7320fffbd4cab213298424410a206a190a6a8",
		"sha256:52c069fff757e5af1e9e7f09ed0bfa69ba880ffa5a0abeb44a9d9f49599dbd65",
		"sha256:f5c82cd22e3348bb4a4fcc5bfedbd9f3bc0ae2834a6a2361aff608aa2310dfa1",
	})
	for i, chainID := range chain.Chain {
		if chainID != expectedChainID[i] {
			t.Errorf("Wrong ChainID in position %d. %s vs %s", i, chainID.String(), expectedChainID[i])
		}
	}
}
