package partitioner

import (
	"sync"
	"sync/atomic"
	"time"
)

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
	debounceResetTimer      bool
}

// Partitioner interface to be passed in HandleInSequence
type Partitioner interface {
	GetPartition() uint32
}

//PartitionBuilder build new partitioner
type PartitionBuilder struct {
	partition *Partition
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
			debounceResetTimer:      true,
		},
	}
}

//WithMaxAttempts max attempts before discarding a message in error, not assigned or 0 = infinite retry
func (p *PartitionBuilder) WithMaxAttempts(maxAttempts int) *PartitionBuilder {
	p.partition.maxAttempts = maxAttempts
	return p
}

//WithMaxMessagesPerPartition default is 10000, it's the max capacity of the buffer
func (p *PartitionBuilder) WithMaxMessagesPerPartition(maxMessages int) *PartitionBuilder {
	p.partition.maxMessagesPerPartition = maxMessages
	return p
}

//WithRetryErrorEvent pass a function useful to log the errors and eventually discard the event
//If the high order function will return true, the event will be discarded.
func (p *PartitionBuilder) WithRetryErrorEvent(fn func(attempts int, err error) bool) *PartitionBuilder {
	p.partition.retryErrorEvent = fn
	return p
}

//WithRetryErrorEvent pass a function useful to log the errors
func (p *PartitionBuilder) WithMaxRetryDiscardEvent(fn func()) *PartitionBuilder {
	p.partition.maxRetryDiscardEvent = fn
	return p
}

//WithDebounceWindow pass a duration window that will be used in HandleDebounced
//this is the time window where messages will be dropped and only the last one executed
//default: 100 Milliseconds
func (p *PartitionBuilder) WithDebounceWindow(d time.Duration) *PartitionBuilder {
	p.partition.debounceWindow = d
	return p
}

// WithDebounceResetTimer if disabled will execute the first received message for a given key when the time window expires.
// New messages for the same key are going to be discarded during this time.
//default: true
func (p *PartitionBuilder) WithDebounceResetTimer(resetTimer bool) *PartitionBuilder {
	p.partition.debounceResetTimer = resetTimer
	return p
}

//Build builds the partitioner
func (p *PartitionBuilder) Build() *Partition {
	npart := p.partition.nPart
	partitions := make([]chan Handler, npart, npart)
	for i := uint32(0); i < npart; i++ {
		partitions[i] = make(chan Handler, p.partition.maxMessagesPerPartition)
	}

	p.partition.partitions = partitions

	for i := uint32(0); i < npart; i++ {
		go func(partId uint32) {
			for {
				f := <-partitions[partId]
				waiting := 20 * time.Millisecond
				attempts := 0
				for err := f(); err != nil; err = f() {
					if p.partition.retryErrorEvent(attempts, err) {
						break
					}
					time.Sleep(time.Duration(waiting))
					if waiting < p.partition.maxWaitingRetry {
						waiting = waiting * 2
					} else {
						waiting = p.partition.maxWaitingRetry
					}

					attempts++
					if p.partition.maxAttempts > 0 && attempts >= p.partition.maxAttempts {
						p.partition.maxRetryDiscardEvent()
						break
					}
				}
				atomic.AddInt64(&p.partition.messagesInFlight, -1)
			}
		}(i)
	}

	return p.partition
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

// HandleDebounced debounced handler handles messages with the same key within a time window only once,
// only the last message will processed and the rest will be dropped
// handler: high order function to execute
// key: message key
func (p *Partition) HandleDebounced(handler Handler, key string) {
	// Backoff if currentInFlight > nPart * maxMessagesPerPartition
	// this means messages are in error and or buffer is full
	for atomic.LoadInt64(&p.messagesInFlight) >= int64(p.nPart)*int64(p.maxMessagesPerPartition) {
		time.Sleep(2 * time.Millisecond)
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
		partition := atomic.AddUint32(&p.roundRobinKey, 1) % p.nPart
		p.partitions[partition] <- handler

		p.Lock()
		delete(p.debounceTimers, key)
		p.Unlock()
	})

	p.debounceTimers[key] = newTimer
}

// GetNumberOfMessagesInFlight get the number of messages not yet consumed
func (p *Partition) GetNumberOfMessagesInFlight() int64 {
	return atomic.LoadInt64(&p.messagesInFlight)
}
