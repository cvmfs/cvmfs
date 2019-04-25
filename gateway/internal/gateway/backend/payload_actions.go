package backend

import (
	"io"
)

// SubmitPayload to be unpacked into the repository
func SubmitPayload(s *Services, token string, payload io.Reader, digest string, headerSize int) error {
	leasePath, lease, err := s.Leases.GetLease(token)
	if err != nil {
		return err
	}

	if err := CheckToken(token, lease.Token.Secret); err != nil {
		return err
	}

	if err := s.Pool.SubmitPayload(leasePath, payload, digest, headerSize); err != nil {
		return err
	}

	return nil
}
