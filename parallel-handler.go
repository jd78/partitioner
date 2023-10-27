package partitioner

import (
	"sync"
	"sync/atomic"
	"time"
)

type RoundRobinHandler struct {
	sync.Mutex
	nPart                uint32
	messageChannel       chan Handler
	maxWaitingRetry      time.Duration
	maxAttempts          int
	buffer               int
	retryErrorEvent      retryErrorEvent
	maxRetryDiscardEvent maxRetryDiscardEvent
	messagesInFlight     int64
	debounceTimers       map[string]*time.Timer
	debounceWindow       time.Duration
	debounceResetTimer   bool
}

// PartitionBuilder build new partitioner
type RoundRobinHandlerBuilder struct {
	roundRobinHandler *RoundRobinHandler
}

// NewRoundRobinHandler Partition builder
func NewRoundRobinHandler(partitions uint32, maxWaitingRetry time.Duration) *RoundRobinHandlerBuilder {
	return &RoundRobinHandlerBuilder{
		&RoundRobinHandler{
			nPart:                partitions,
			maxWaitingRetry:      maxWaitingRetry,
			retryErrorEvent:      func(attempts int, err error) bool { return false },
			maxRetryDiscardEvent: func() {},
			debounceTimers:       make(map[string]*time.Timer),
			debounceWindow:       100 * time.Millisecond,
			debounceResetTimer:   true,
		},
	}
}

// NewSingleThreadHandler Partition builder
func NewSingleThreadHandler(maxWaitingRetry time.Duration) *RoundRobinHandlerBuilder {
	return NewRoundRobinHandler(1, maxWaitingRetry)
}

// WithMaxAttempts max attempts before discarding a message in error, not assigned or 0 = infinite retry
func (p *RoundRobinHandlerBuilder) WithMaxAttempts(maxAttempts int) *RoundRobinHandlerBuilder {
	p.roundRobinHandler.maxAttempts = maxAttempts
	return p
}

// WithBuffer It's the capacity of the buffer. default is 0, meaning no buffered channel will be used.
func (p *RoundRobinHandlerBuilder) WithBuffer(buffer int) *RoundRobinHandlerBuilder {
	p.roundRobinHandler.buffer = buffer
	return p
}

// WithRetryErrorEvent pass a function useful to log the errors and eventually discard the event
// If the high order function will return true, the event will be discarded.
func (p *RoundRobinHandlerBuilder) WithRetryErrorEvent(fn func(attempts int, err error) bool) *RoundRobinHandlerBuilder {
	p.roundRobinHandler.retryErrorEvent = fn
	return p
}

// WithRetryErrorEvent pass a function useful to log the errors
func (p *RoundRobinHandlerBuilder) WithMaxRetryDiscardEvent(fn func()) *RoundRobinHandlerBuilder {
	p.roundRobinHandler.maxRetryDiscardEvent = fn
	return p
}

// WithDebounceWindow pass a duration window that will be used in HandleDebounced
// this is the time window where messages will be dropped and only the last one executed
// default: 100 Milliseconds
func (p *RoundRobinHandlerBuilder) WithDebounceWindow(d time.Duration) *RoundRobinHandlerBuilder {
	p.roundRobinHandler.debounceWindow = d
	return p
}

// WithDebounceResetTimer if disabled will execute the first received message for a given key when the time window expires.
// New messages for the same key are going to be discarded during this time.
// default: true
func (p *RoundRobinHandlerBuilder) WithDebounceResetTimer(resetTimer bool) *RoundRobinHandlerBuilder {
	p.roundRobinHandler.debounceResetTimer = resetTimer
	return p
}

// Build builds the partitioner
func (p *RoundRobinHandlerBuilder) Build() *RoundRobinHandler {
	npart := p.roundRobinHandler.nPart
	p.roundRobinHandler.messageChannel = func() chan Handler {
		if p.roundRobinHandler.buffer > 0 {
			return make(chan Handler, npart)
		}
		return make(chan Handler)
	}()

	for i := uint32(0); i < npart; i++ {
		go func() {
			for f := range p.roundRobinHandler.messageChannel {
				retry(f, p.roundRobinHandler.retryErrorEvent, p.roundRobinHandler.maxWaitingRetry, p.roundRobinHandler.maxAttempts,
					p.roundRobinHandler.maxRetryDiscardEvent, func() { atomic.AddInt64(&p.roundRobinHandler.messagesInFlight, -1) })
			}
		}()
	}

	return p.roundRobinHandler
}

// HandleInRoundRobin handles the handler high order function in round robin
// handler: high order function to execute
func (p *RoundRobinHandler) Handle(handler Handler) {
	p.messageChannel <- handler
	atomic.AddInt64(&p.messagesInFlight, 1)
}

// HandleDebounced debounced handler handles messages with the same key within a time window only once,
// only the last message will processed and the rest will be dropped
// handler: high order function to execute
// key: message key
func (p *RoundRobinHandler) HandleDebounced(handler Handler, key string) {
	// Backoff if currentInFlight > nPart * maxMessagesPerPartition
	// this means messages are in error and/or buffer is full
	buffer := func() int64 {
		if p.buffer > 0 {
			return int64(p.buffer)
		}
		return int64(p.nPart)
	}()
	if p.buffer > 0 {
		for atomic.LoadInt64(&p.messagesInFlight) >= buffer {
			time.Sleep(2 * time.Millisecond)
		}
	}

	p.Lock()
	defer p.Unlock()
	timer, found := p.debounceTimers[key]
	if found {
		if !p.debounceResetTimer {
			return
		}
		timer.Stop()
	}

	newTimer := time.AfterFunc(p.debounceWindow, func() {
		atomic.AddInt64(&p.messagesInFlight, 1)
		p.messageChannel <- handler

		p.Lock()
		delete(p.debounceTimers, key)
		p.Unlock()
	})

	p.debounceTimers[key] = newTimer
}

// GetNumberOfMessagesInFlight get the number of messages not yet consumed
func (p *RoundRobinHandler) GetNumberOfMessagesInFlight() int64 {
	return atomic.LoadInt64(&p.messagesInFlight)
}
