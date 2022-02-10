package backend

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
)

func TestNotificationSystem(t *testing.T) {
	tmp, err := ioutil.TempDir("", "test_notifications")
	if err != nil {
		t.Fatalf("could not create temp dir")
	}
	defer os.RemoveAll(tmp)

	ns, err := NewNotificationSystem(tmp)
	if err != nil {
		t.Fatalf("could not create notification system")
	}

	hd := make(chan NotificationMessage, 1000)

	ctx := context.TODO()
	repo := "test.repo.org"

	ns.Subscribe(ctx, repo, hd)

	ns.Publish(ctx, repo, NotificationMessage("msg1"))
	ns.Publish(ctx, repo, NotificationMessage("msg2"))

	ns.Unsubscribe(ctx, repo, hd)

	ns.Publish(ctx, repo, NotificationMessage("msg3"))

	messages := make([]NotificationMessage, 0)
	for m := range hd {
		messages = append(messages, m)
	}

	if len(messages) != 2 || messages[0] != "msg1" || messages[1] != "msg2" {
		t.Fatalf("Unexpected received message pattern: %v", messages)
	}
}

func TestNotificationSystemLateSubscription(t *testing.T) {
	tmp, err := ioutil.TempDir("", "test_notifications")
	if err != nil {
		t.Fatalf("could not create temp dir")
	}
	defer os.RemoveAll(tmp)

	ns, err := NewNotificationSystem(tmp)
	if err != nil {
		t.Fatalf("could not create notification system")
	}

	hd := make(chan NotificationMessage, 1000)

	ctx := context.TODO()
	repo := "test.repo.org"

	ns.Publish(ctx, repo, NotificationMessage("msg1"))
	ns.Publish(ctx, repo, NotificationMessage("msg2"))

	ns.Subscribe(ctx, repo, hd)
	ns.Unsubscribe(ctx, repo, hd)

	messages := make([]NotificationMessage, 0)
	for m := range hd {
		messages = append(messages, m)
	}

	if len(messages) != 1 || messages[0] != "msg2" {
		t.Fatalf("Unexpected received message pattern: %v", messages)
	}
}
