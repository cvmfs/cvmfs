package lib

import (
	"io/ioutil"
	"os"
	"strconv"
	"time"

	"github.com/cvmfs/ducc/notification"
)

var NotificationService *notification.NotificationService
var NotificationFile string
var startTime *time.Time

func SetupNotification() {
	defer notifyStart()
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

func notifyStart() {
	start := notification.NewNotification(NotificationService)
	start.Action("start_ducc").
		AddField("pid", strconv.Itoa(os.Getpid())).
		Send()
	t := time.Now()
	startTime = &t
}

func StopNotification() {
	if NotificationService != nil {
		end := notification.NewNotification(NotificationService)
		end.Action("end_ducc").
			AddField("pid", strconv.Itoa(os.Getpid())).
			Elapsed(*startTime).
			Send()
		NotificationService.Stop()
	}
}
