package backend

import (
	"context"
	"fmt"
	"sync"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

// NotificationMessage is an alias for a UTF-8 string
type NotificationMessage string

// SubscriberHandle is the writable end of a channel of notification messages
type SubscriberHandle chan NotificationMessage

// SubscriberSet is a set of subscriber handles (implemented as a map handler -> void)
type SubscriberSet map[SubscriberHandle]struct{}

// SubscriberMap holds a set of subscriber handles for each repository
type SubscriberMap map[string]SubscriberSet

type NotificationStore map[string]NotificationMessage

// NotificationSystem encapsulates the functionality of the repository
// activity notification system
type NotificationSystem struct {
	Subscribers    SubscriberMap
	SubscriberLock sync.RWMutex
	Store          NotificationStore
}

// NewNotificationSystem is a constructor function for the NotificationSystem type
func NewNotificationSystem(workDir string) (*NotificationSystem, error) {
	gw.Log("notify", gw.LogInfo).
		Msgf("database opened (work dir: %v)", workDir)

	ns := &NotificationSystem{
		Subscribers:    make(SubscriberMap),
		SubscriberLock: sync.RWMutex{},
		Store:          make(NotificationStore),
	}

	return ns, nil
}

// Publish a new repository manifest to the notification system
func (ns *NotificationSystem) Publish(
	ctx context.Context, repository string, message NotificationMessage) {

	existing := ns.getMessage(repository)
	if existing == "" || message != existing {
		ns.setMessage(repository, message)
		ns.notify(repository, message)
	}

	gw.LogC(ctx, "notify", gw.LogDebug).
		Str("repository", repository).
		Msg("manifest published")
}

// Subscribe the handle to messages for the given repository
func (ns *NotificationSystem) Subscribe(
	ctx context.Context, repository string, handle SubscriberHandle) {

	added := false
	func() {
		ns.SubscriberLock.Lock()
		defer ns.SubscriberLock.Unlock()
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
	}()

	if added {
		message := ns.getMessage(repository)

		if message != "" {
			ns.notify(repository, message)
		}

		gw.LogC(ctx, "notify", gw.LogDebug).
			Str("repository", repository).
			Msg("subscription added")
	}
}

// Unsubscribe from messages for the given repository. Closes the subscriber handle (chan)
func (ns *NotificationSystem) Unsubscribe(
	ctx context.Context, repository string, handle SubscriberHandle) error {

	ns.SubscriberLock.Lock()
	defer ns.SubscriberLock.Unlock()
	subsForRepo, hasSubs := ns.Subscribers[repository]
	if !hasSubs {
		return fmt.Errorf("no_subscriptions_for_repository")
	}

	_, found := subsForRepo[handle]
	if !found {
		return fmt.Errorf("invalid_handle")
	}

	delete(subsForRepo, handle)
	close(handle)

	gw.LogC(ctx, "notify", gw.LogDebug).
		Str("repository", repository).
		Msg("subscription removed")

	return nil
}

func (ns *NotificationSystem) notify(repository string, message NotificationMessage) {
	ns.SubscriberLock.RLock()
	defer ns.SubscriberLock.RUnlock()
	subsForRepo, present := ns.Subscribers[repository]
	if present {
		for s := range subsForRepo {
			s <- message
		}
	}
}

func (ns *NotificationSystem) getMessage(repository string) NotificationMessage {
	return ns.Store[repository]
}

func (ns *NotificationSystem) setMessage(repository string, message NotificationMessage) {
	ns.Store[repository] = message
}
