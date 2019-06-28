package backend

import (
	"context"
	"time"
)

// PublishManifest publishes a repository manifest to the notification system
func (s *Services) PublishManifest(ctx context.Context, repository string, message []byte) error {
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "publish_manifest", &outcome, t0)

	err := s.Notifications.Publish(ctx, repository, message)

	if err != nil {
		outcome = err.Error()
	}

	return err
}

// SubscribeToNotifications for a repository
func (s *Services) SubscribeToNotifications(ctx context.Context, repository string) SubscriberHandle {
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "subscribe_to_notifications", &outcome, t0)

	source := make(chan NotificationMessage, 1000)
	s.Notifications.Subscribe(ctx, repository, source)
	return source
}

// UnsubscribeFromNotifications for a repository
func (s *Services) UnsubscribeFromNotifications(
	ctx context.Context, repository string, handle SubscriberHandle) error {
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "unsubscribe_from_notifications", &outcome, t0)

	err := s.Notifications.Unsubscribe(ctx, repository, handle)

	if err != nil {
		outcome = err.Error()
	}

	return err
}
