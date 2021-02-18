package lib

import (
	"io/ioutil"

	"github.com/cvmfs/ducc/notification"
)

var NotificationService *notification.NotificationService

var NotificationFile string

func SetupNotification() {
	if NotificationFile != "" {
		n, err := notification.NewNotificationService(NotificationFile)
		if err == nil {
			NotificationService = &n
			return
		}
	}
	n := notification.NewNotificationServiceFromWriter(ioutil.Discard)
	NotificationService = &n
}

func StopNotification() {
	if NotificationService != nil {
		NotificationService.Stop()
	}
}
