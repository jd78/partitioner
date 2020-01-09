package partitioner

import (
	"time"
)

// Handler high order function to be executed
// done chan bool: confirmation channel, true for completed, false for error and infinite retry
type Handler func(done chan bool)

type Partition struct {
	nPart      int
	partitions []chan Handler
}

// Partitioner interface to be passed in HandleInSequence
type Partitioner interface {
	GetPartition() int64
}

// Create create a new partition object
// partitions: number of partitions
// maxWaitingRetry: max waiting time between retries
func Create(partitions int, maxWaitingRetry time.Duration) *Partition {
	p := make([]chan Handler, partitions, partitions)
	for i := 0; i < len(p); i++ {
		p[i] = make(chan Handler, 1000)
	}

	for i := 0; i < partitions; i++ {
		go func(partId int) {
			for {
				f := <-p[partId]
				done := make(chan bool, 1)
				result := false
				waiting := 20 * time.Millisecond
				for !result {
					f(done)
					result = <-done
					if !result {
						time.Sleep(time.Duration(waiting) * time.Millisecond)
						if waiting < maxWaitingRetry {
							waiting = waiting * 2
						} else {
							waiting = maxWaitingRetry
						}
					}
				}
			}
		}(i)
	}

	return &Partition{partitions, p}
}

// HandleInSequence handles the handler high order function in sequence based on the resolved partitionId
// handler: high order function to execute
// partitionId: Partitioner interface to get an int64 partition
func (p *Partition) HandleInSequence(handler Handler, partitionID Partitioner) {
	partition := partitionID.GetPartition() % int64(p.nPart)
	p.partitions[partition] <- handler
}
