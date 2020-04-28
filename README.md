# Partitioner

[![Build Status](https://travis-ci.org/jd78/partitioner.svg?branch=master)](https://travis-ci.org/jd78/partitioner)

Partitioner executes high order functions in sequence given a partition key. Functions with the same partition key will be executed only by one partition.

#### Examples 

```go
//Blocking Partition example
package main

import (
	"fmt"
	"github.com/jd78/partitioner"
	"sync/atomic"
	"time"
)

type message1 struct {
	id   int
	test int
}

type message2 struct {
	identifier int
}

var roundrobin int64 = 0

type partition struct {
	message interface{}
}

func (p partition) GetPartition() int64 {
	switch p.message.(type) {
	case message1:
		return int64(p.message.(message1).id)
	case message2:
		return int64(p.message.(message2).identifier)
	}

	return atomic.AddInt64(&roundrobin, 1)
}

func main() {
	p := partitioner.New(30, 5*time.Second).Build() //Creates 30 partition and a max retry time interval of 5000 ms

	for i := 0; i < 100; i++ {
		m1 := message1{1, i} //will go on the same partition
		p.HandleInSequence(func() error {
			fmt.Printf("message1: %d\n", m1.test)
			time.Sleep(300 * time.Millisecond)
			return nil
		}, partition{m1})

		m2 := message2{i}
		p.HandleInSequence(func() error {
			fmt.Printf("message2: %d\n", m2.identifier)
			time.Sleep(300 * time.Millisecond)
			return nil
		}, partition{m2})

		k := i
		p.HandleInSequence(func() error {
			fmt.Printf("round robin: %d\n", k)
			time.Sleep(300 * time.Millisecond)
			return nil
		}, partition{}) //Round robin example
	}

	fmt.Scanln()
}

```

```go
//Round Robin Example
package main

import (
	"fmt"
	"github.com/jd78/partitioner"
	"time"
)

type message1 struct {
	id   int
	test int
}

func main() {
	p := partitioner.New(30, 5*time.Second).Build() //Creates 30 partition and a max retry time interval of 5000 ms

	for i := 0; i < 100; i++ {
		m1 := message1{1, i}
		p.HandleInRoundRobin(func() error {
			fmt.Printf("message1: %d\n", m1.test)
			time.Sleep(300 * time.Millisecond)
			return nil
		})

	fmt.Scanln()
}

```
