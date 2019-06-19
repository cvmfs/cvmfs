package backend

import "context"

// PublishManifest publishes a repository manifest to the notification system
func (s *Services) PublishManifest(ctx context.Context, repository string, message []byte) error {
	return s.Notifications.Publish(ctx, repository, message)
}
