package backend

import (
	"context"
	"fmt"
	"path"
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
}

// NewNotificationSystem is a constructor function for the NotificationSystem type
func NewNotificationSystem(workDir string) (*NotificationSystem, error) {
	ns := &NotificationSystem{
		Subscribers: make(SubscriberMap),
		WorkDir:     path.Join(workDir, "notify"),
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
	return nil
}

// Subscribe the handle to messages for the given repository
func (ns *NotificationSystem) Subscribe(
	ctx context.Context, repository string, handle SubscriberHandle) {

	subsForRepo, present := ns.Subscribers[repository]
	if present {
		_, pres := subsForRepo[handle]
		if !pres {
			subsForRepo[handle] = struct{}{}
		}
	} else {
		subsForRepo = make(SubscriberSet)
		subsForRepo[handle] = struct{}{}
		ns.Subscribers[repository] = subsForRepo
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

	return nil
}
