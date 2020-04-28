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
	return &PartitionBuilder{&Partition{nPart: partitions, maxWaitingRetry: maxWaitingRetry, maxMessagesPerPartition: 10000}}
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
				for f() != nil {
					println(time.Duration(waiting) * time.Millisecond)
					time.Sleep(time.Duration(waiting))
					if waiting < p.p.maxWaitingRetry {
						waiting = waiting * 2
					} else {
						waiting = p.p.maxWaitingRetry
					}

					attempts++
					if p.p.maxAttempts > 0 && attempts >= p.p.maxAttempts {
						break
					}
				}
			}
		}(i)
	}

	return p.p
}

// HandleInSequence handles the handler high order function in sequence based on the resolved partitionId
// handler: high order function to execute
// partitionId: Partitioner interface to get an int64 partition
func (p *Partition) HandleInSequence(handler Handler, partitionID Partitioner) {
	partition := partitionID.GetPartition() % int64(p.nPart)
	p.partitions[partition] <- handler
}

func (p *Partition) HandleInRoundRobin(handler Handler) {
	partition := atomic.AddInt32(&p.roundRobinKey, 1) % int32(p.nPart)
	p.partitions[partition] <- handler
}
