# Partitioner

[![Build Status](https://travis-ci.com/jd78/partitioner.svg?branch=master)](https://app.travis-ci.com/github/jd78/partitioner)

Partitioner executes high order functions in sequence given a partition key. Functions with the same partition key will be executed only by one partition. 
You can also opt-in a round robin executor and even using the more optimized RoundRobinHandler.

A debounced consumer is also available to delay the execution of a task for a given message key and deduplicate messages.

## Constructors

```go
New(partitions uint32, maxWaitingRetry time.Duration)
NewRoundRobinHandler(partitions uint32, maxWaitingRetry time.Duration) // improved round robin handler
```

## Options

```go
//WithMaxAttempts max attempts before discarding a message in error, not assigned or 0 = infinite retry
WithMaxAttempts(maxAttempts int)

//WithBuffer It's the capacity of the buffer. default is 0, meaning no buffered channel will be used.
WithBuffer(buffer int)

//WithRetryErrorEvent pass a function useful to log the errors and eventually discard the event
//If the high order function will return true, the event will be discarded.
WithRetryErrorEvent(fn func(attempts int, err error) bool)

//WithRetryErrorEvent pass a function useful to log the errors
WithMaxRetryDiscardEvent(fn func())

//WithDebounceWindow pass a duration window that will be used in HandleDebounced
//this is the time window where messages will be dropped and only the last one executed
//default: 100 Milliseconds
WithDebounceWindow(d time.Duration)

// WithDebounceResetTimer if disabled will execute the first received message for a given key when the time window expires.
// New messages for the same key are going to be discarded during this time.
//default: true
WithDebounceResetTimer(resetTimer bool)
```

## Examples 

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

var roundrobin uint32

type partition struct {
	message interface{}
}

func (p partition) GetPartition() uint32 {
	switch p.message.(type) {
	case message1:
		return uint32(p.message.(message1).id)
	case message2:
		return uint32(p.message.(message2).identifier)
	}

	return atomic.AddUint32(&roundrobin, 1)
}

func main() {
	p := partitioner.New(30, 5*time.Second).Build() //Creates 30 partitions and a max retry time interval of 5000 ms

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
	p := partitioner.New(30, 5*time.Second).Build() //Creates 30 partitions and a max retry time interval of 5000 ms
    // OR a more performant one:
    // p := partitioner.NewRoundRobinHandler(30, 5*time.Second).Build()

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

```go
//Debounce example
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
	p := partitioner.NewRoundRobinHandler(30, 5*time.Second).
        WithDebounceWindow(500 * time.Millisecond).
        Build()

	for i := 0; i < 2; i++ {
		m1 := message1{1, i}
		p.HandleDebounced(func() error {
			fmt.Printf("message1: %d\n", m1.test)
			return nil
		}, "same key")

        //We send 2 messages, the first one will be discarded and the second one executed after 500ms.
    }

	fmt.Scanln()
}

```

```go
//Debounce without resetting the timer example
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
	p := partitioner.NewRoundRobinHandler(30, 5*time.Second).
        WithDebounceWindow(500 * time.Millisecond).
        WithDebounceResetTimer(false).
        Build()

	for i := 0; i < 2; i++ {
		m1 := message1{1, i}
		p.HandleDebounced(func() error {
			fmt.Printf("message1: %d\n", m1.test)
			return nil
		}, "same key")

        //We send 2 messages, the first one will be executed after 500 ms and second one discarded.
    }

	fmt.Scanln()
}

```
