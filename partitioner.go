package partitioner

import (
	"sync/atomic"
)

var roundRobin int32 = 0

type Handler func()

type partitioner struct {
	nPart      int
	partitions []chan Handler
}

func Partitioner(partitions int) partitioner {
	p := make([]chan Handler, partitions, partitions)
	for i := 0; i < len(p); i++ {
		p[i] = make(chan Handler, 1000)
	}

	for i := 0; i < partitions; i++ {
		go func(partId int) {
			for {
				f := <-p[partId]
				f()
			}
		}(i)
	}

	return partitioner{partitions, p}
}

func (p partitioner) HandleInSequence(handler Handler, partitionId int64) {
	partition := partitionId % int64(p.nPart)

	p.partitions[partition] <- handler
}

func (p partitioner) HandleRoundRobin(handler Handler) {
	i := atomic.AddInt32(&roundRobin, 1)
	partition := int(i) % p.nPart
	p.partitions[partition] <- handler
}
