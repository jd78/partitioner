package partitioner

import (
	"sync/atomic"
	"time"
)

// Handler high order function to be executed
// nit err for completed, otherwise infinite retry
type Handler func() error

type Partition struct {
	nPart         int
	partitions    []chan Handler
	roundRobinKey int32
}

// Partitioner interface to be passed in HandleInSequence
type Partitioner interface {
	GetPartition() int64
}

// New create a new partition object
// partitions: number of partitions
// maxWaitingRetry: max waiting time between retries
func New(partitions int, maxWaitingRetry time.Duration) *Partition {
	p := make([]chan Handler, partitions, partitions)
	for i := 0; i < len(p); i++ {
		p[i] = make(chan Handler, 1000)
	}

	for i := 0; i < partitions; i++ {
		go func(partId int) {
			for {
				f := <-p[partId]
				waiting := 20 * time.Millisecond
				for f() != nil {
					time.Sleep(time.Duration(waiting) * time.Millisecond)
					if waiting < maxWaitingRetry {
						waiting = waiting * 2
					} else {
						waiting = maxWaitingRetry
					}
				}
			}
		}(i)
	}

	return &Partition{partitions, p, 0}
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
