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

	return s.Notifications.Publish(ctx, repository, message)
}

// SubscribeToNotifications to activity messages for a repository
func (s *Services) SubscribeToNotifications(ctx context.Context, repository string) <-chan NotificationMessage {
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "subscribe_to_notifications", &outcome, t0)

	source := make(chan NotificationMessage, 1000)
	s.Notifications.Subscribe(ctx, repository, source)
	return source
}
