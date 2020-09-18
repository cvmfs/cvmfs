package cvmfs

import (
	"errors"
	"fmt"
)

var WaitForExpiredError = errors.New("Error, the event you want to wait for already fired")

type WaitForExpiredErr struct {
	err error
}

func NewWaitForExpiredErr(target, lower uint64) WaitForExpiredErr {
	return WaitForExpiredErr{
		fmt.Errorf("%s target event: %d next event: %d", WaitForExpiredError, target, lower)}
}

func (w WaitForExpiredErr) Error() string {
	return w.err.Error()
}

func (w WaitForExpiredErr) Unwrap() error {
	return WaitForExpiredError
}

var WaitForNotScheduledError = errors.New("Error, the event you want to wait for has not been scheduled yet")

type WaitForNotScheduledErr struct {
	err error
}

func NewWaitForNotScheduledErr(target, upper uint64) WaitForNotScheduledErr {
	return WaitForNotScheduledErr{fmt.Errorf("%s target event: %d last scheduled event: %d", WaitForNotScheduledError, target, upper)}
}

func (w WaitForNotScheduledErr) Error() string {
	return w.err.Error()
}

func (w WaitForNotScheduledErr) Unwrap() error {
	return WaitForNotScheduledError
}
