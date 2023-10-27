package partitioner

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
	time.Sleep(100 * time.Millisecond)

	assert.True(t, firstExecuted)
	assert.False(t, secondExecuted)
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

	assert.True(t, firstExecuted)
	assert.True(t, secondExecuted)
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

	assert.False(t, secondExecuted)
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

	assert.True(t, firstExecuted)
	assert.True(t, secondExecuted)
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

	assert.Equal(t, 3, firstCalled)
	assert.True(t, called)
	assert.True(t, secondExecuted)
	assert.Equal(t, int64(0), p.GetNumberOfMessagesInFlight())
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

	assert.Equal(t, 1, firstCalled)
	assert.True(t, secondExecuted)
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

	assert.Equal(t, int64(3), p.GetNumberOfMessagesInFlight())

	time.Sleep(2 * time.Second)

	assert.Equal(t, int64(0), p.GetNumberOfMessagesInFlight())
}

func Test_partitioner_HandleDebounceBackoff(t *testing.T) {
	p := New(1, 5*time.Second).
		WithMaxMessagesPerPartition(1).Build()

	secondExecuted := false

	f1 := func() error {
		return errors.New("Error")
	}

	f2 := func() error {
		secondExecuted = true
		return nil
	}

	p.HandleDebounced(f1, "1")
	time.Sleep(500 * time.Millisecond)
	go p.HandleDebounced(f2, "2")
	time.Sleep(500 * time.Millisecond)

	assert.False(t, secondExecuted)
}

func Test_partitioner_HandleDebounceWaitToExecute(t *testing.T) {
	p := New(1, 5*time.Second).
		WithDebounceWindow(1 * time.Minute).Build()

	executed := false

	f1 := func() error {
		executed = true
		return nil
	}

	p.HandleDebounced(f1, "1")
	time.Sleep(500 * time.Millisecond)

	assert.False(t, executed)
}

func Test_partitioner_HandleDebounceExecuted(t *testing.T) {
	p := New(1, 5*time.Second).Build()

	executed := false

	f1 := func() error {
		executed = true
		return nil
	}

	p.HandleDebounced(f1, "1")
	time.Sleep(500 * time.Millisecond)

	assert.True(t, executed)
}

func Test_partitioner_HandleOverwriteExecution(t *testing.T) {
	p := New(1, 5*time.Second).Build()

	firstExecuted := false
	secondExecuted := false

	f1 := func() error {
		firstExecuted = true
		return nil
	}

	f2 := func() error {
		secondExecuted = true
		return nil
	}

	p.HandleDebounced(f1, "1")
	p.HandleDebounced(f2, "1")
	time.Sleep(500 * time.Millisecond)

	assert.False(t, firstExecuted)
	assert.True(t, secondExecuted)
}

func Test_partitioner_handler_HandleDebounceDoNotExecuteIfNewMessagesAreComing(t *testing.T) {
	p := New(1, 5*time.Second).
		WithDebounceWindow(100 * time.Millisecond).Build()

	executed := false

	f1 := func() error {
		executed = true
		return nil
	}

	go func() {
		for {
			p.HandleDebounced(f1, "1")
		}
	}()
	time.Sleep(500 * time.Millisecond)

	assert.False(t, executed)
}

func Test_partitioner_handler_HandleDebounceDoNotResetTimer(t *testing.T) {
	p := New(1, 5*time.Second).
		WithDebounceResetTimer(false).
		WithDebounceWindow(100 * time.Millisecond).Build()

	executed := false

	f1 := func() error {
		executed = true
		return nil
	}

	go func() {
		for {
			p.HandleDebounced(f1, "1")
		}
	}()
	time.Sleep(500 * time.Millisecond)
	assert.True(t, executed)
}
