package notification

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	TimeFormat = "_2 Jan 2006 - 15:04:05.000"
)

type NotificationService struct {
	ch   chan Notification
	wg   *sync.WaitGroup
	file *os.File
}

func NewNotificationService(filePath string) (NotificationService, error) {
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0660)
	if err != nil {
		return NotificationService{}, err
	}

	n := NewNotificationServiceFromWriter(f)
	n.file = f
	return n, nil
}

func NewNotificationServiceFromWriter(w io.Writer) NotificationService {
	nc := make(chan Notification, 100)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for n := range nc {
			s := n.ToString()
			io.WriteString(w, s+"\n")
		}
	}()
	return NotificationService{ch: nc, wg: &wg}
}

func (n *NotificationService) Notify(nn *Notification) {
	// we add the timing ourself if it is not present
	if nn.Get("time") == "" {
		t := time.Now()
		nn = nn.AddField("time", t.Format(TimeFormat))
	}
	n.ch <- *nn
}

func (n *NotificationService) Stop() {
	close(n.ch)
	n.wg.Wait()
	if n.file != nil {
		n.file.Close()
	}
}

type Notification struct {
	fields  []string
	service *NotificationService
}

func NewNotification(s *NotificationService) *Notification {
	return &Notification{fields: make([]string, 0), service: s}
}

func ParseNotification(s []byte) (Notification, error) {
	var m map[string]string
	n := NewNotification(nil)
	err := json.Unmarshal(s, &m)
	if err != nil {
		return *n, err
	}
	for key, value := range m {
		n = n.AddField(key, value)
	}
	return *n, nil
}

func (n *Notification) AddField(key string, value string) *Notification {
	f := n.fields
	f = append(f, key)
	f = append(f, value)
	return &Notification{fields: f, service: n.service}
}

func (n *Notification) Elapsed(t time.Time) *Notification {
	ts := fmt.Sprintf("%f", time.Now().Sub(t).Seconds())
	return n.AddField("time_elapsed_s", ts)
}

/// Identifier are useful to match notification related between each other
/// for instance the beginning of an operation and its end
func (n *Notification) AddId() *Notification {
	return n.AddIdentifier("id")
}

func (n *Notification) AddIdentifier(key string) *Notification {
	uuid := uuid.New()
	n1 := n.AddField(key, uuid.String())
	return n1
}

func (n *Notification) Error(err error) *Notification {
	if err == nil {
		return n.AddField("status", "ok")
	}
	return n.AddField("status", "error").AddField("error", err.Error())
}

func (n *Notification) Action(action string) *Notification {
	return n.AddField("action", action)
}

func (n *Notification) SizeBytes(size int64) *Notification {
	s := fmt.Sprintf("%d", size)
	return n.AddField("size_bytes", s)
}

func (n *Notification) ToString() string {
	m := make(map[string]string)
	for i := 0; i < len(n.fields); i += 2 {
		key := n.fields[i]
		value := n.fields[i+1]
		m[key] = value
	}
	bytes, _ := json.Marshal(m)
	return string(bytes)
}

func (n *Notification) Get(key string) string {
	for i, k := range n.fields {
		if i%2 == 0 {
			if k == key {
				return n.fields[i+1]
			}
		}
	}
	return ""
}

func (n *Notification) Send() {
	n.service.Notify(n)
}
