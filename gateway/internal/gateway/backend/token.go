package backend

import (
	"crypto/rand"
	"fmt"
	"time"

	jwt "github.com/dgrijalva/jwt-go"
	"github.com/pkg/errors"
)

const (
	tokenSecretLength = 32
)

// LeaseToken holds the signed token string, the secret used to sign the token and
// the expiration of the token
type LeaseToken struct {
	TokenStr   string
	Secret     []byte
	Expiration time.Time
}

// LeaseClaims encode the parameters of the lease (expiration time, repository path)
type LeaseClaims struct {
	jwt.StandardClaims
	Path string `json:"path"`
}

// ExpiredTokenError is returned by the CheckToken function for an expired token
type ExpiredTokenError struct{}

func (e ExpiredTokenError) Error() string {
	return "token expired"
}

// InvalidTokenError is returned by the CheckToken function when the token cannot be
// parsed or the signature is invalid
type InvalidTokenError struct{}

func (e InvalidTokenError) Error() string {
	return "invalid token"
}

// NewLeaseToken generates a new lease token for the given repository
// path, valid for maxLeaseTime. Returns the signed encoded token string, the secret
// used to sign the token and the expiration time of the token
func NewLeaseToken(repoPath string, maxLeaseDuration time.Duration) (*LeaseToken, error) {
	if repoPath == "" {
		return nil, fmt.Errorf("repository path should not be empty")
	}

	secret := make([]byte, tokenSecretLength)
	if n, err := rand.Read(secret); n != tokenSecretLength || err != nil {
		return nil, fmt.Errorf("could not generate token secret")
	}

	expiration := time.Now().Add(maxLeaseDuration)

	claims := LeaseClaims{
		StandardClaims: jwt.StandardClaims{
			ExpiresAt: expiration.UnixNano()},
		Path: repoPath}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	tokenStr, err := token.SignedString(secret)
	if err != nil {
		return nil, errors.Wrap(err, "could not sign token")
	}

	return &LeaseToken{TokenStr: tokenStr, Secret: secret, Expiration: expiration}, nil
}

// CheckToken with the given secret. Return ExpiredError if the token is
// expired, or InvalidTokenError if the token cannot be validated
func CheckToken(tokenStr string, secret []byte) error {
	token, err := jwt.ParseWithClaims(
		tokenStr, &LeaseClaims{}, func(t *jwt.Token) (interface{}, error) {
			return secret, nil
		})
	if err != nil {
		return InvalidTokenError{}
	}

	if claims, ok := token.Claims.(*LeaseClaims); ok {
		if claims.ExpiresAt <= time.Now().UnixNano() {
			return ExpiredTokenError{}
		}
		return nil
	}

	return InvalidTokenError{}
}
