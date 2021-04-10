package test

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestStreamUnsubscribeResubscribe(t *testing.T) {
	// Configuration
	natsTest := NewNatsConnection()
	defer natsTest.Terminate()
	nc := natsTest.NatsConn

	// Get jetStream
	js, _ := nc.JetStream()
	// Add stream
	_, _ = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"TEST.*"},
		// Discards all acknowledge messages
		Retention: nats.WorkQueuePolicy,
	})
	// Add consumer
	_, _ = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable: "CONS_TEST",
		// Acknowledge all messages received by subscribers
		AckPolicy: nats.AckAllPolicy,
	})

	sent := 0
	received1 := 0
	received2 := 0

	// When subscribing to a subject
	log.Println()
	subs, err := js.Subscribe("TEST.*", func(m *nats.Msg) {
		log.Printf("Received_1: %s", m.Data)
		received1++
	}, nats.Durable("CONS_TEST"))
	if err != nil {
		t.Error(err)
	}

	// Simulate failure at 300ms
	go func() {
		timer := time.NewTimer(300 * time.Millisecond)
		for range timer.C {
			_ = subs.Drain()
			_ = subs.Unsubscribe()
		}
	}()

	// Simulate recovery at 800ms
	resGroup := sync.WaitGroup{}
	resGroup.Add(1)
	go func() {
		timer := time.NewTimer(800 * time.Millisecond)
		for range timer.C {
			log.Println()
			_, err := js.Subscribe("TEST.*", func(m *nats.Msg) {
				log.Printf("Received_2: %s", m.Data)
				received2++
			}, nats.Durable("CONS_TEST"))
			if err != nil {
				t.Error(err)
			}
			resGroup.Done()
		}
	}()

	pubGroup := sync.WaitGroup{}
	pubGroup.Add(1)
	// Publish messages concurrently with everything else
	go func() {
		defer pubGroup.Done()
		ticker := time.NewTicker(50 * time.Millisecond)
		for range ticker.C {
			sent++
			if _, err := js.Publish("TEST.message", []byte(fmt.Sprintf("Message %d\n", sent))); err != nil {
				t.Error(err)
			}
			// only send 20 messages
			if sent == 20 {
				break
			}
		}
	}()

	// Wait for the recovery
	resGroup.Wait()
	// Wait for all messages to be published
	pubGroup.Wait()
	// Make sure we receive all published messages
	_ = nc.Drain()
	for nc.IsDraining() {
	}

	received := received1 + received2
	if received != sent {
		t.Errorf("waiting %d, got %d", sent, received)
	}
	log.Printf("Sent: %d, Received_1: %d, Received_2: %d\n", sent, received1, received2)
}
