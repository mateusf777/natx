package test

import (
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	Subject = "test"
	Message = "test"
)

func TestNormalUsage(t *testing.T) {

	// Configuration
	natsTest, err := NewNatsConnection()
	if err != nil {
		t.Error(err)
	}
	defer natsTest.Terminate()
	nc := natsTest.NatsConn

	expected := 10
	received := 0

	// When subscribing to a subject
	if _, err = nc.Subscribe(Subject, func(m *nats.Msg) {
		received++
		_ = m.Ack()
	}); err != nil {
		t.Error()
	}

	// If we write 10 messages
	prodGroup := sync.WaitGroup{}
	prodGroup.Add(1)
	go func() {
		defer prodGroup.Done()
		for i := 0; i < expected; i++ {
			if err := nc.Publish(Subject, []byte(Message)); err != nil {
				t.Error(err)
			}
		}
	}()

	// Wait for publish all messages
	prodGroup.Wait()
	// Wait for draining all subscriber
	_ = nc.Drain()
	for nc.IsDraining() {
	}

	// It should receive 10 messages
	if received != expected {
		t.Errorf("waiting %d, got %d", expected, received)
	}
}

func TestUnsubscribeResubscribe(t *testing.T) {
	// Configuration
	natsTest, err := NewNatsConnection()
	if err != nil {
		t.Error(err)
	}
	defer natsTest.Terminate()
	nc := natsTest.NatsConn

	sent := 0
	received := 0

	// When subscribing to a subject
	subs, err := nc.Subscribe(Subject, func(m *nats.Msg) {
		received++
	})
	if err != nil {
		t.Error()
	}

	// Unsubscribe
	go func() {
		timer := time.NewTimer(300 * time.Millisecond)
		for range timer.C {
			_ = subs.Unsubscribe()
		}
	}()

	// Resubscribe
	resGroup := sync.WaitGroup{}
	resGroup.Add(1)
	go func() {
		timer := time.NewTimer(500 * time.Millisecond)
		for range timer.C {
			_, err := nc.Subscribe(Subject, func(m *nats.Msg) {
				received++
			})
			if err != nil {
				t.Error()
			}
			resGroup.Done()
		}
	}()

	pubGroup := sync.WaitGroup{}
	pubGroup.Add(1)
	// If we write 10 messages (1 each 100ms)
	go func() {
		defer pubGroup.Done()
		ticker := time.NewTicker(100 * time.Millisecond)
		for range ticker.C {
			sent++
			if err := nc.Publish(Subject, []byte(Message)); err != nil {
				t.Error(err)
			}
			// only send 10 messages
			if sent == 10 {
				break
			}
		}
	}()

	// Wait for resubscribe
	resGroup.Wait()
	// Wait for pub
	pubGroup.Wait()
	// Wait for draining all subscriber
	_ = nc.Drain()
	for nc.IsDraining() {
	}

	// It receive less than 10 messages because there was no subscriber for ~200ms
	if received >= sent {
		t.Errorf("waiting lens than %d, got %d", sent, received)
	}
}
