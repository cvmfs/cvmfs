package backend

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path"
	"sync"

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
	Subscribers    SubscriberMap
	SubscriberLock sync.RWMutex
	WorkDir        string
	Store          *bolt.DB
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

	// Create a bucket in the backing store to hold the last message published for
	// each repository
	store.Update(func(txn *bolt.Tx) error {
		_, err := txn.CreateBucketIfNotExists([]byte("last_message"))
		if err != nil {
			return errors.Wrap(err, "could not create bucket: 'last_message'")
		}

		return nil
	})

	gw.Log("notify", gw.LogInfo).
		Msgf("database opened (work dir: %v)", workDir)

	ns := &NotificationSystem{
		Subscribers:    make(SubscriberMap),
		SubscriberLock: sync.RWMutex{},
		WorkDir:        workDir,
		Store:          store,
	}

	return ns, nil
}

// Publish a new repository manifest to the notification system
func (ns *NotificationSystem) Publish(
	ctx context.Context, repository string, message []byte) error {

	shouldNotify := false
	ns.Store.Update(func(txn *bolt.Tx) error {
		existing := getLastMessage(txn, repository)
		if existing == nil || !bytes.Equal(message, existing) {
			if err := setMessage(txn, repository, message); err != nil {
				return err
			}
			shouldNotify = true
		}

		return nil
	})

	if shouldNotify {
		ns.notify(repository, message)
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
		message := make([]byte, 0)
		ns.Store.View(func(txn *bolt.Tx) error {
			message = getLastMessage(txn, repository)
			return nil
		})

		if message != nil {
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

func (ns *NotificationSystem) notify(repository string, message []byte) {
	ns.SubscriberLock.RLock()
	defer ns.SubscriberLock.RUnlock()
	subsForRepo, present := ns.Subscribers[repository]
	if present {
		for s := range subsForRepo {
			s <- NotificationMessage(message)
		}
	}
}

func getLastMessage(txn *bolt.Tx, repository string) []byte {
	b := txn.Bucket([]byte("last_message"))
	return b.Get([]byte(repository))
}

func setMessage(txn *bolt.Tx, repository string, message []byte) error {
	b := txn.Bucket([]byte("last_message"))
	return b.Put([]byte(repository), message)
}
