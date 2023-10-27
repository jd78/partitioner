package partitioner

import (
	"time"
)

// Handler high order function to be executed
// nit err for completed, otherwise infinite retry
type Handler func() error

type retryErrorEvent func(attempts int, err error) bool
type maxRetryDiscardEvent func()

func retry(f Handler, r retryErrorEvent, maxWaitRetry time.Duration, maxAttempts int,
	maxRetryDiscardEvent maxRetryDiscardEvent, exitLoopFn func()) {
	waiting := 20 * time.Millisecond
	attempts := 0
	for err := f(); err != nil; err = f() {
		if r(attempts, err) {
			break
		}
		time.Sleep(time.Duration(waiting))
		if waiting < maxWaitRetry {
			waiting = waiting * 2
		} else {
			waiting = maxWaitRetry
		}

		attempts++
		if maxAttempts > 0 && attempts >= maxAttempts {
			maxRetryDiscardEvent()
			break
		}
	}
	exitLoopFn()
}
