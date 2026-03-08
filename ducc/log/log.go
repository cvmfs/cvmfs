package log

import (
	log "github.com/sirupsen/logrus"
)

func LogE(err error) *log.Entry {
	return log.WithFields(log.Fields{"error": err})
}

func Log() *log.Entry {
	return log.WithFields(log.Fields{})
}

func Ensure(entry *log.Entry) *log.Entry {
	if entry != nil {
		return entry
	}
	return Log()
}

func WithImage(image string) *log.Entry {
	if image == "" {
		return Log()
	}
	return Log().WithField("image", image)
}

func WithImageE(err error, image string) *log.Entry {
	if image == "" {
		return LogE(err)
	}
	return LogE(err).WithField("image", image)
}
