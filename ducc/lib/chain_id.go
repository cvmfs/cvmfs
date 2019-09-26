package lib

import (
	_ "crypto/sha256"
	"github.com/opencontainers/go-digest"
)

type ChainID struct {
	BaseLayers []string
	Chain      []digest.Digest
	ID         digest.Digest
}

func NewChainID(base string) ChainID {
	ID := digest.FromBytes([]byte(base))
	return ChainID{
		[]string{base},
		[]digest.Digest{ID},
		ID,
	}
}

func (chain ChainID) AddLayer(layer string) ChainID {
	ID := digest.FromBytes([]byte(chain.ID.String() + " " + layer))
	baseLayers := append(chain.BaseLayers, layer)
	chainD := append(chain.Chain, ID)
	return ChainID{
		baseLayers,
		chainD,
		ID,
	}
}
