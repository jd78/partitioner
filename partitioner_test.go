package partitioner

import (
	"testing"
	"time"
)

type partition struct {
	partition int64
}

func (p partition) GetPartition() int64 {
	return p.partition
}

func Test_partitioner_HandleInSequence(t *testing.T) {
	p := Create(30, 5*time.Second)

	firstExecuted := false
	secondExecuted := false

	f1 := func(done chan bool) {
		firstExecuted = true
		time.Sleep(2 * time.Second)
		done <- true
	}

	f2 := func(done chan bool) {
		secondExecuted = true
		time.Sleep(2 * time.Second)
		done <- true
	}

	p.HandleInSequence(f1, partition{1})
	p.HandleInSequence(f2, partition{1})
	time.Sleep(20 * time.Millisecond)

	if !firstExecuted || secondExecuted {
		t.Error("Functions executed not in the right sequence")
	}
}

func Test_partitioner_HandleConcurrently(t *testing.T) {
	p := Create(30, 5*time.Second)

	firstExecuted := false
	secondExecuted := false

	f1 := func(done chan bool) {
		firstExecuted = true
		time.Sleep(2 * time.Second)
		done <- true
	}

	f2 := func(done chan bool) {
		secondExecuted = true
		time.Sleep(2 * time.Second)
		done <- true
	}

	p.HandleInSequence(f1, partition{1})
	p.HandleInSequence(f2, partition{2})
	time.Sleep(20 * time.Millisecond)

	if !firstExecuted || !secondExecuted {
		t.Error("Both functions not executed")
	}
}

func Test_partitioner_HandleInfiniteRetries(t *testing.T) {
	p := Create(30, 5*time.Second)

	secondExecuted := false

	f1 := func(done chan bool) {
		done <- false
	}

	f2 := func(done chan bool) {
		secondExecuted = true
		done <- true
	}

	p.HandleInSequence(f1, partition{1})
	p.HandleInSequence(f2, partition{1})
	time.Sleep(20 * time.Millisecond)

	if secondExecuted {
		t.Error("Second function should not be executed")
	}
}
