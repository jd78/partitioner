package partitioner

import (
	"errors"
	"testing"
	"time"
)

func Test_roundrobin_handler_HandleInRoundRobin(t *testing.T) {
	p := NewRoundRobinHandler(30, 5*time.Second).Build()

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

func Test_roundrobin_handler_HandleMaxAttempts(t *testing.T) {
	called := false
	discardEvent := func() { called = true }
	p := NewRoundRobinHandler(30, 500*time.Millisecond).
		WithMaxAttempts(3).
		WithMaxRetryDiscardEvent(discardEvent).
		WithBuffer(1).
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

	p.HandleInRoundRobin(f1)
	p.HandleInRoundRobin(f2)
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

	if p.GetNumberOfMessagesInFlight() != 0 {
		t.Error("Message in flight should be 0")
	}
}

func Test_roundrobin_handler_ForceDiscardOnError(t *testing.T) {
	p := NewRoundRobinHandler(30, 500*time.Millisecond).
		WithRetryErrorEvent(func(a int, err error) bool { return true }).
		WithBuffer(1).
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

	p.HandleInRoundRobin(f1)
	p.HandleInRoundRobin(f2)
	time.Sleep(2 * time.Second)

	if firstCalled != 1 {
		t.Errorf("Expected %d but got %d", 1, firstCalled)
	}

	if !secondExecuted {
		t.Error("Second function should have been executed")
	}
}

func Test_roundrobin_handler_MessagesInFlight(t *testing.T) {
	p := NewRoundRobinHandler(30, 5*time.Second).Build()

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

func Test_roundrobin_handler_HandleDebounceBackoff(t *testing.T) {
	p := NewRoundRobinHandler(1, 5*time.Second).
		WithBuffer(1).Build()

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

	if secondExecuted {
		t.Error("Second function should not be executed")
	}
}

func Test_roundrobin_handler_HandleDebounceWaitToExecute(t *testing.T) {
	p := New(1, 5*time.Second).
		WithDebounceWindow(1 * time.Minute).Build()

	executed := false

	f1 := func() error {
		executed = true
		return nil
	}

	p.HandleDebounced(f1, "1")
	time.Sleep(500 * time.Millisecond)

	if executed {
		t.Error("function should not be executed")
	}
}

func Test_roundrobin_handler_HandleDebounceExecuted(t *testing.T) {
	p := NewRoundRobinHandler(1, 5*time.Second).Build()

	executed := false

	f1 := func() error {
		executed = true
		return nil
	}

	p.HandleDebounced(f1, "1")
	time.Sleep(500 * time.Millisecond)

	if !executed {
		t.Error("function should be executed")
	}
}

func Test_roundrobin_handler_HandleOverwriteExecution(t *testing.T) {
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

	if firstExecuted {
		t.Error("first function should not be executed")
	}

	if !secondExecuted {
		t.Error("second function should be executed")
	}
}
