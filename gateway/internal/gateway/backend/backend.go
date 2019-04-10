package backend

import (
	gw "github.com/cvmfs/gateway/internal/gateway"
	"github.com/pkg/errors"
)

// Services is a container for the various
// backend services
type Services struct {
	Access AccessConfig
}

// Start initializes the various backend services
func Start(cfg *gw.Config) (*Services, error) {
	ac := NewAccessConfig()
	if err := ac.Load(cfg.AccessConfigFile); err != nil {
		return nil, errors.Wrap(
			err, "loading repository access configuration failed")
	}

	return &Services{Access: ac}, nil
}
