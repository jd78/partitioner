package partitioner

import (
	"sync/atomic"
	"time"
)

// Handler high order function to be executed
// nit err for completed, otherwise infinite retry
type Handler func() error

type Partition struct {
	nPart                   int
	partitions              []chan Handler
	roundRobinKey           int32
	maxWaitingRetry         time.Duration
	maxAttempts             int
	maxMessagesPerPartition int
	retryErrorEvent         func(attempts int, err error) bool
	maxRetryDiscardEvent    func()
	messagesInFlight        int64
}

// Partitioner interface to be passed in HandleInSequence
type Partitioner interface {
	GetPartition() int64
}

//PartitionBuilder build new partitioner
type PartitionBuilder struct {
	p *Partition
}

//New Partition builder
func New(partitions int, maxWaitingRetry time.Duration) *PartitionBuilder {
	return &PartitionBuilder{
		&Partition{
			nPart:                   partitions,
			maxWaitingRetry:         maxWaitingRetry,
			maxMessagesPerPartition: 10000,
			retryErrorEvent:         func(attempts int, err error) bool { return false },
			maxRetryDiscardEvent:    func() {},
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

//Build builds the partitioner
func (p *PartitionBuilder) Build() *Partition {
	npart := p.p.nPart
	partitions := make([]chan Handler, npart, npart)
	for i := 0; i < npart; i++ {
		partitions[i] = make(chan Handler, p.p.maxMessagesPerPartition)
	}

	p.p.partitions = partitions

	for i := 0; i < npart; i++ {
		go func(partId int) {
			for {
				f := <-partitions[partId]
				waiting := 20 * time.Millisecond
				attempts := 0
				for err := f(); err != nil; err = f() {
					if p.p.retryErrorEvent(attempts, err) {
						atomic.AddInt64(&p.p.messagesInFlight, -1)
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
						atomic.AddInt64(&p.p.messagesInFlight, -1)
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
// partitionId: Partitioner interface to get an int64 partition
func (p *Partition) HandleInSequence(handler Handler, partitionID Partitioner) {
	partition := func() int64 {
		p := partitionID.GetPartition() % int64(p.nPart)
		if p < 0 {
			return -p
		}
		return p
	}()
	p.partitions[partition] <- handler
	atomic.AddInt64(&p.messagesInFlight, 1)
}

// HandleInRoundRobin handles the handler high order function in round robin
// handler: high order function to execute
func (p *Partition) HandleInRoundRobin(handler Handler) {
	partition := func() int32 {
		p := atomic.AddInt32(&p.roundRobinKey, 1) % int32(p.nPart)
		if p < 0 {
			return -p
		}
		return p
	}()
	p.partitions[partition] <- handler
	atomic.AddInt64(&p.messagesInFlight, 1)
}

// HasMessagesInFlight get the number of messages not yet consumed
func (p *Partition) GetNumberOfMessagesInFlight() int64 {
	return p.messagesInFlight
}
