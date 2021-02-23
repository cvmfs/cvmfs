package notification

import (
	"bufio"
	"os"
	"testing"
	"time"
)

func TestNotification(t *testing.T) {
	path := "/tmp/ducc_notification_test"
	defer os.RemoveAll(path)
	service, err := NewNotificationService(path)
	if err != nil {
		t.Fatal("Impossible to create the service")
	}
	if _, err := os.Stat(path); err != nil {
		t.Error("No file where it should be")
	}
	notification := NewNotification(&service)
	notification = notification.AddField("test", "test1")
	service.Notify(notification)

	service.Stop()
}

func TestNotificationWeCanReadThem(t *testing.T) {
	path := "/tmp/ducc_notification_test"
	//defer os.RemoveAll(path)
	service, err := NewNotificationService(path)
	if err != nil {
		t.Fatal("Impossible to create the service")
	}
	if _, err := os.Stat(path); err != nil {
		t.Error("No file where it should be")
	}
	notification := NewNotification(&service)
	n1 := notification.AddField("n1", "1")
	n2 := n1.AddField("n2", "2")
	n3 := n1.AddField("n3", "3")
	service.Notify(n1)
	time.Sleep(1 * time.Millisecond)
	service.Notify(n2)
	time.Sleep(1 * time.Millisecond)
	service.Notify(n3)

	service.Stop()

	f, err := os.Open(path)
	if err != nil {
		t.Error("Impossible to open the notification file")
	}
	defer f.Close()
	readed := make([]Notification, 0)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		byt := scanner.Bytes()
		nn, err := ParseNotification(byt)
		if err != nil {
			t.Error("Error in parsing the notification")
		}
		readed = append(readed, nn)
	}
	if len(readed) != 3 {
		t.Error("Did not read all the notification")
	}
	if readed[0].Get("n1") != "1" {
		t.Error("First notification does not have the correct value for `n1`")
	}
	if readed[0].Get("n2") != "" || readed[0].Get("n3") != "" {
		t.Error("First notification has values from the other two")
	}
	if readed[1].Get("n1") != "1" {
		t.Error("Second notification does not have the correct value for `n1`")
	}
	if readed[1].Get("n2") != "2" {
		t.Error("Second notification does not have the correct value for `n2`")
	}
	if readed[2].Get("n2") != "" {
		t.Error("Third notification has value from the second one")
	}
	if readed[2].Get("n3") != "3" {
		t.Error("Third notification does not have the correct valie for `n3`")
	}
	if readed[0].Get("time") == "" {
		t.Error("First notification does not have the time")
	}
	if readed[1].Get("time") == "" {
		t.Error("Second notification does not have the time")
	}
	if readed[0].Get("time") == readed[1].Get("time") {
		t.Error("First and second notification have the same time")
	}
}
