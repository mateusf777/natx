package test

import (
	"sync"
	"testing"

	"github.com/nats-io/nats.go"
)

const (
	Subject = "test"
	Message = "test"
)

func TestGeneral(t *testing.T) {

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
