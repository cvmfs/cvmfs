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
