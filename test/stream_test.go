package test

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestStreamUnsubscribeResubscribe(t *testing.T) {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Probably nats is not running\nRun: docker run --network host -p 4222:4222 nats -js\nerror: %v", err)
	}

	// Get jetStream
	js, _ := jetstream.New(nc)
	// Add stream
	_, _ = js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"TEST.*"},
		// Discards all acknowledge messages
		Retention: jetstream.WorkQueuePolicy,
	})
	// Add consumer
	cons, err := js.CreateOrUpdateConsumer(context.Background(), "TEST", jetstream.ConsumerConfig{
		Durable:       "CONS_TEST",
		FilterSubject: "TEST.message",
		// Acknowledge all messages received by subscribers
		AckPolicy: jetstream.AckAllPolicy,
	})
	if err != nil {
		panic(err)
	}

	sent := 0
	received1 := 0
	received2 := 0

	go func() {
		log.Println()

		for received1 < 5 {
			msg, _ := cons.Next()
			log.Printf("Received_1: %s", msg.Data())
			_ = msg.Ack()
			received1++
		}

		fmt.Println("first finished")
	}()

	subGroup := sync.WaitGroup{}
	subGroup.Add(1)
	go func() {
		timer := time.NewTimer(800 * time.Millisecond)
		for range timer.C {
			for received1+received2 < 20 {
				msg, _ := cons.Next()
				log.Printf("Received_1: %s", msg.Data())
				_ = msg.Ack()
				received2++

			}
			fmt.Println("second finished")
			subGroup.Done()
		}
	}()

	pubGroup := sync.WaitGroup{}
	pubGroup.Add(1)
	// Publish messages concurrently with everything else
	go func() {
		defer pubGroup.Done()
		ticker := time.NewTicker(10 * time.Millisecond)
		for range ticker.C {
			sent++
			if _, err := js.Publish(context.Background(), "TEST.message", []byte(fmt.Sprintf("Message %d\n", sent))); err != nil {
				t.Error(err)
			}
			// only send 20 messages
			if sent == 20 {
				break
			}
		}
	}()

	// Wait for all messages to be published
	pubGroup.Wait()
	// Make sure we receive all published messages
	subGroup.Wait()
	received := received1 + received2
	if received != sent {
		t.Errorf("waiting %d, got %d", sent, received)
	}
	log.Printf("Sent: %d, Received_1: %d, Received_2: %d\n", sent, received1, received2)
}
