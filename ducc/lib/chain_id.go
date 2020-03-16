package lib

import (

	//_ "crypto/sha256"
	"github.com/opencontainers/go-digest"
)

type ChainID struct {
	BaseLayers []digest.Digest
	Chain      []digest.Digest
	ID         digest.Digest
}

func NewChainID(id digest.Digest) ChainID {
	return ChainID{
		[]digest.Digest{id},
		[]digest.Digest{id},
		id,
	}
}

func (chain ChainID) AddLayer(layer digest.Digest) ChainID {
	if chain.ID == "" {
		return NewChainID(layer)
	}
	ID := digest.FromString(chain.ID.String() + " " + layer.String())
	baseLayers := append(chain.BaseLayers, layer)
	chainD := append(chain.Chain, ID)
	return ChainID{
		baseLayers,
		chainD,
		ID,
	}
}

func ChainIDFromLayers(layers []digest.Digest) ChainID {
	chain := ChainID{}
	for _, l := range layers {
		chain = chain.AddLayer(l)
	}
	return chain
}

func MapStringToDigest(layers []string) ([]digest.Digest, error) {
	var digests []digest.Digest
	for _, layer := range layers {
		digest, err := digest.Parse(layer)
		if err != nil {
			return nil, err
		}
		digests = append(digests, digest)
	}
	return digests, nil
}
