package backend

import (
	"context"
	"fmt"
	"os"
	"path"

	gw "github.com/cvmfs/gateway/internal/gateway"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

// NotificationMessage is an alias for a UTF-8 string
type NotificationMessage string

// SubscriberHandle is the writable end of a channel of notification messages
type SubscriberHandle chan NotificationMessage

// SubscriberSet is a set of subscriber handles (implemented as a map handler -> void)
type SubscriberSet map[SubscriberHandle]struct{}

// SubscriberMap holds a set of subscriber handles for each repository
type SubscriberMap map[string]SubscriberSet

// NotificationSystem encapsulates the functionality of the repository
// activity notification system
type NotificationSystem struct {
	Subscribers SubscriberMap
	WorkDir     string
	Store       *bolt.DB
}

// NewNotificationSystem is a constructor function for the NotificationSystem type
func NewNotificationSystem(workDir string) (*NotificationSystem, error) {
	workDir = path.Join(workDir, "notify")
	if err := os.MkdirAll(workDir, 0777); err != nil {
		return nil, errors.Wrap(err, "could not create directory for backing store")
	}
	store, err := bolt.Open(workDir+"/messages.db", 0666, nil)
	if err != nil {
		return nil, errors.Wrap(err, "could not open backing store")
	}

	gw.Log("notify", gw.LogInfo).
		Msgf("database opened (work dir: %v)", workDir)

	ns := &NotificationSystem{
		Subscribers: make(SubscriberMap),
		WorkDir:     workDir,
		Store:       store,
	}
	return ns, nil
}

// Publish a new repository manifest to the notification system
func (ns *NotificationSystem) Publish(
	ctx context.Context, repository string, message []byte) error {
	subsForRepo, present := ns.Subscribers[repository]
	if present {
		for s := range subsForRepo {
			s <- NotificationMessage(message)
		}
	}

	gw.LogC(ctx, "notify", gw.LogDebug).
		Str("repository", repository).
		Msg("manifest published")

	return nil
}

// Subscribe the handle to messages for the given repository
func (ns *NotificationSystem) Subscribe(
	ctx context.Context, repository string, handle SubscriberHandle) {

	added := false
	subsForRepo, present := ns.Subscribers[repository]
	if present {
		_, pres := subsForRepo[handle]
		if !pres {
			subsForRepo[handle] = struct{}{}
			added = true
		}
	} else {
		subsForRepo = make(SubscriberSet)
		subsForRepo[handle] = struct{}{}
		added = true
		ns.Subscribers[repository] = subsForRepo
	}

	if added {
		gw.LogC(ctx, "notify", gw.LogDebug).
			Str("repository", repository).
			Msg("subscription added")
	}
}

// Unsubscribe from messages for the given repository
func (ns *NotificationSystem) Unsubscribe(
	ctx context.Context, repository string, handle SubscriberHandle) error {

	subsForRepo, hasSubs := ns.Subscribers[repository]
	if !hasSubs {
		return fmt.Errorf("no_subscriptions_for_repository")
	}

	_, found := subsForRepo[handle]
	if !found {
		return fmt.Errorf("invalid_handle")
	}

	delete(subsForRepo, handle)

	gw.LogC(ctx, "notify", gw.LogDebug).
		Str("repository", repository).
		Msg("subscription removed")

	return nil
}
