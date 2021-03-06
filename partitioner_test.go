package partitioner

import (
	"errors"
	"testing"
	"time"
)

type partition struct {
	partition uint32
}

func (p partition) GetPartition() uint32 {
	return p.partition
}

func Test_partitioner_HandleInSequence(t *testing.T) {
	p := New(30, 5*time.Second).Build()

	firstExecuted := false
	secondExecuted := false

	f1 := func() error {
		firstExecuted = true
		time.Sleep(2 * time.Second)
		return nil
	}

	f2 := func() error {
		secondExecuted = true
		time.Sleep(2 * time.Second)
		return nil
	}

	p.HandleInSequence(f1, partition{1})
	p.HandleInSequence(f2, partition{1})
	time.Sleep(20 * time.Millisecond)

	if !firstExecuted || secondExecuted {
		t.Error("Functions executed not in the right sequence")
	}
}

func Test_partitioner_HandleConcurrently(t *testing.T) {
	p := New(30, 5*time.Second).Build()

	firstExecuted := false
	secondExecuted := false

	f1 := func() error {
		firstExecuted = true
		time.Sleep(2 * time.Second)
		return nil
	}

	f2 := func() error {
		secondExecuted = true
		time.Sleep(2 * time.Second)
		return nil
	}

	p.HandleInSequence(f1, partition{1})
	p.HandleInSequence(f2, partition{2})
	time.Sleep(20 * time.Millisecond)

	if !firstExecuted || !secondExecuted {
		t.Error("Both functions not executed")
	}
}

func Test_partitioner_HandleInfiniteRetries(t *testing.T) {
	p := New(30, 5*time.Second).Build()

	secondExecuted := false

	f1 := func() error {
		return errors.New("Error")
	}

	f2 := func() error {
		secondExecuted = true
		return nil
	}

	p.HandleInSequence(f1, partition{1})
	p.HandleInSequence(f2, partition{1})
	time.Sleep(500 * time.Millisecond)

	if secondExecuted {
		t.Error("Second function should not be executed")
	}
}

func Test_partitioner_HandleInRoundRobin(t *testing.T) {
	p := New(30, 5*time.Second).Build()

	firstExecuted := false
	secondExecuted := false

	f1 := func() error {
		firstExecuted = true
		time.Sleep(2 * time.Second)
		return nil
	}

	f2 := func() error {
		secondExecuted = true
		return nil
	}

	p.HandleInRoundRobin(f1)
	p.HandleInRoundRobin(f2)
	time.Sleep(1 * time.Second)

	if !firstExecuted || !secondExecuted {
		t.Error("Functions not executed in Round Robin")
	}
}

func Test_partitioner_HandleMaxAttempts(t *testing.T) {
	called := false
	discardEvent := func() { called = true }
	p := New(30, 500*time.Millisecond).
		WithMaxAttempts(3).
		WithMaxRetryDiscardEvent(discardEvent).
		Build()

	firstCalled := 0
	secondExecuted := false

	f1 := func() error {
		firstCalled++
		return errors.New("Error")
	}

	f2 := func() error {
		secondExecuted = true
		return nil
	}

	p.HandleInSequence(f1, partition{1})
	p.HandleInSequence(f2, partition{1})
	time.Sleep(2 * time.Second)

	if firstCalled != 3 {
		t.Errorf("Expected %d but got %d", 3, firstCalled)
	}

	if !called {
		t.Errorf("Expected %t but got %t", true, called)
	}

	if !secondExecuted {
		t.Error("Second function should have been executed")
	}
}

func Test_partitioner_ForceDiscardOnError(t *testing.T) {
	p := New(30, 500*time.Millisecond).
		WithRetryErrorEvent(func(a int, err error) bool { return true }).
		Build()

	firstCalled := 0
	secondExecuted := false

	f1 := func() error {
		firstCalled++
		return errors.New("Error")
	}

	f2 := func() error {
		secondExecuted = true
		return nil
	}

	p.HandleInSequence(f1, partition{1})
	p.HandleInSequence(f2, partition{1})
	time.Sleep(2 * time.Second)

	if firstCalled != 1 {
		t.Errorf("Expected %d but got %d", 1, firstCalled)
	}

	if !secondExecuted {
		t.Error("Second function should have been executed")
	}
}

func Test_partitioner_MessagesInFlight(t *testing.T) {
	p := New(30, 5*time.Second).Build()

	f1 := func() error {
		time.Sleep(1 * time.Second)
		return nil
	}

	p.HandleInRoundRobin(f1)
	p.HandleInRoundRobin(f1)
	p.HandleInRoundRobin(f1)

	if p.GetNumberOfMessagesInFlight() != 3 {
		t.Error("Was supposed to have messages in flight")
	}

	time.Sleep(2 * time.Second)

	if p.GetNumberOfMessagesInFlight() != 0 {
		t.Error("Was supposed to not have messages in flight")
	}
}
