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

func newChainID(id digest.Digest) ChainID {
	return ChainID{
		[]digest.Digest{id},
		[]digest.Digest{id},
		id,
	}
}

func (chain ChainID) addLayer(layer digest.Digest) ChainID {
	if chain.ID == "" {
		return newChainID(layer)
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
		chain = chain.addLayer(l)
	}
	return chain
}
