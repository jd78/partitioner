package partitioner

import (
	"time"
)

// Handler high order function to be executed
// done chan bool: confirmation channel, true for completed, false for error and infinite retry
type Handler func(done chan bool)

type partitioner struct {
	nPart      int
	partitions []chan Handler
}

// PartitionId interface to be passed in HandleInSequence
type PartitionId interface {
	GetPartition() int64
}

// Partitioner create a new partition object
// partitions: number of partitions
// maxWaitingRetryMilliseconds: max waiting time between retries
func Partitioner(partitions, maxWaitingRetryMilliseconds int) partitioner {
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
				waiting := 20
				for !result {
					f(done)
					result = <-done
					if !result {
						time.Sleep(time.Duration(waiting) * time.Millisecond)
						if waiting < maxWaitingRetryMilliseconds {
							waiting = waiting * 2
						} else {
							waiting = maxWaitingRetryMilliseconds
						}
					}
				}
			}
		}(i)
	}

	return partitioner{partitions, p}
}

// HandleInSequence handles the handler high order function in sequence based on the resolved partitionId
// handler: high order function to execute
// partitionId: PartitionId interface to get an int64 partition
func (p partitioner) HandleInSequence(handler Handler, partitionId PartitionId) {
	partition := partitionId.GetPartition() % int64(p.nPart)
	p.partitions[partition] <- handler
}
