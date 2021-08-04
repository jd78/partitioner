package partitioner

import (
	"sync"
	"sync/atomic"
	"time"
)

// Handler high order function to be executed
// nit err for completed, otherwise infinite retry
type Handler func() error

type Partition struct {
	sync.Mutex
	nPart                   uint32
	partitions              []chan Handler
	roundRobinKey           uint32
	maxWaitingRetry         time.Duration
	maxAttempts             int
	maxMessagesPerPartition int
	retryErrorEvent         func(attempts int, err error) bool
	maxRetryDiscardEvent    func()
	messagesInFlight        int64
	debounceTimers          map[string]*time.Timer
	debounceWindow          time.Duration
}

// Partitioner interface to be passed in HandleInSequence
type Partitioner interface {
	GetPartition() uint32
}

//PartitionBuilder build new partitioner
type PartitionBuilder struct {
	p *Partition
}

//New Partition builder
func New(partitions uint32, maxWaitingRetry time.Duration) *PartitionBuilder {
	return &PartitionBuilder{
		&Partition{
			nPart:                   partitions,
			maxWaitingRetry:         maxWaitingRetry,
			maxMessagesPerPartition: 10000,
			retryErrorEvent:         func(attempts int, err error) bool { return false },
			maxRetryDiscardEvent:    func() {},
			debounceTimers:          make(map[string]*time.Timer),
			debounceWindow:          100 * time.Millisecond,
		},
	}
}

//WithMaxAttempts max attempts before discarding a message in error, not assigned or 0 = infinite retry
func (p *PartitionBuilder) WithMaxAttempts(maxAttempts int) *PartitionBuilder {
	p.p.maxAttempts = maxAttempts
	return p
}

//WithMaxMessagesPerPartition default is 10000, it's the max capacity of the buffer
func (p *PartitionBuilder) WithMaxMessagesPerPartition(maxMessages int) *PartitionBuilder {
	p.p.maxMessagesPerPartition = maxMessages
	return p
}

//WithRetryErrorEvent pass a function useful to log the errors and eventually discard the event
//If the high order function will return true, the event will be discarded.
func (p *PartitionBuilder) WithRetryErrorEvent(fn func(attempts int, err error) bool) *PartitionBuilder {
	p.p.retryErrorEvent = fn
	return p
}

//WithRetryErrorEvent pass a function useful to log the errors
func (p *PartitionBuilder) WithMaxRetryDiscardEvent(fn func()) *PartitionBuilder {
	p.p.maxRetryDiscardEvent = fn
	return p
}

func (p *PartitionBuilder) WithDebounceWindow(d time.Duration) *PartitionBuilder {
	p.p.debounceWindow = d
	return p
}

//Build builds the partitioner
func (p *PartitionBuilder) Build() *Partition {
	npart := p.p.nPart
	partitions := make([]chan Handler, npart, npart)
	for i := uint32(0); i < npart; i++ {
		partitions[i] = make(chan Handler, p.p.maxMessagesPerPartition)
	}

	p.p.partitions = partitions

	for i := uint32(0); i < npart; i++ {
		go func(partId uint32) {
			for {
				f := <-partitions[partId]
				waiting := 20 * time.Millisecond
				attempts := 0
				for err := f(); err != nil; err = f() {
					if p.p.retryErrorEvent(attempts, err) {
						break
					}
					time.Sleep(time.Duration(waiting))
					if waiting < p.p.maxWaitingRetry {
						waiting = waiting * 2
					} else {
						waiting = p.p.maxWaitingRetry
					}

					attempts++
					if p.p.maxAttempts > 0 && attempts >= p.p.maxAttempts {
						p.p.maxRetryDiscardEvent()
						break
					}
				}
				atomic.AddInt64(&p.p.messagesInFlight, -1)
			}
		}(i)
	}

	return p.p
}

// HandleInSequence handles the handler high order function in sequence based on the resolved partitionId
// handler: high order function to execute
// partitionId: Partitioner interface to get an uint32 partition
func (p *Partition) HandleInSequence(handler Handler, partitionID Partitioner) {
	partition := partitionID.GetPartition() % p.nPart
	p.partitions[partition] <- handler
	atomic.AddInt64(&p.messagesInFlight, 1)
}

// HandleInRoundRobin handles the handler high order function in round robin
// handler: high order function to execute
func (p *Partition) HandleInRoundRobin(handler Handler) {
	partition := atomic.AddUint32(&p.roundRobinKey, 1) % p.nPart
	p.partitions[partition] <- handler
	atomic.AddInt64(&p.messagesInFlight, 1)
}

// HandleDebounced debounce the handler high order function in round robin
// handler: high order function to execute
func (p *Partition) HandleDebounced(handler Handler, key string) {
	// Backoff if currentInFlight > nPart * maxMessagesPerPartition
	// this means messages are in error and or buffer is full
	currentInFlight := atomic.LoadInt64(&p.messagesInFlight)
	for currentInFlight >= int64(p.nPart)*int64(p.maxMessagesPerPartition) {
		time.Sleep(2 * time.Millisecond)
	}

	timer, found := p.debounceTimers[key]
	if found {
		atomic.AddInt64(&p.messagesInFlight, -1)
		timer.Stop()
	}

	newTimer := time.AfterFunc(p.debounceWindow, func() {
		atomic.AddInt64(&p.messagesInFlight, 1)
		partition := atomic.AddUint32(&p.roundRobinKey, 1) % p.nPart
		p.partitions[partition] <- handler

		p.Lock()
		delete(p.debounceTimers, key)
		p.Unlock()
	})

	p.Lock()
	p.debounceTimers[key] = newTimer
	p.Unlock()
}

// GetNumberOfMessagesInFlight get the number of messages not yet consumed
func (p *Partition) GetNumberOfMessagesInFlight() int64 {
	return atomic.LoadInt64(&p.messagesInFlight)
}
