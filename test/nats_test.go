package test

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	Subject = "test"
)

func TestNormalUsage(t *testing.T) {
	// Configuration
	//natsTest := NewNatsConnection()
	//defer natsTest.Terminate()
	nc, err := nats.Connect(nats.DefaultURL, nats.UserInfo(os.Getenv("NATS_USER"), os.Getenv("NATS_PASSWORD")))
	if err != nil {
		t.Error(err)
		return
	}

	expected := 10
	received := 0

	// When subscribing to a subject
	if _, err := nc.Subscribe(Subject, func(m *nats.Msg) {
		log.Printf("Received: %s", m.Data)
		received++
	}); err != nil {
		t.Error()
	}

	// And Published 10 messages
	for i := 0; i < expected; i++ {
		if err := nc.Publish(Subject, []byte(fmt.Sprintf("Message %d\n", i+1))); err != nil {
			t.Error(err)
		}
	}

	_ = nc.Drain()
	for nc.IsDraining() {
	}

	// It should receive 10 messages
	if received != expected {
		t.Errorf("waiting %d, got %d", expected, received)
	}
}

func TestRequestReply(t *testing.T) {
	// Configuration
	//natsTest := NewNatsConnection()
	//defer natsTest.Terminate()
	nc, err := nats.Connect(nats.DefaultURL, nats.UserInfo(os.Getenv("NATS_USER"), os.Getenv("NATS_PASSWORD")))
	if err != nil {
		t.Error(err)
		return
	}

	expected := 10
	received := 0

	// When subscribing to a subject
	if _, err := nc.Subscribe(Subject, func(m *nats.Msg) {
		log.Printf("Received Request: %s", m.Data)
		resp := len(m.Data)
		_ = m.Respond([]byte(fmt.Sprintf("%d", resp)))
		received++
	}); err != nil {
		t.Error()
	}

	// And Published 10 messages
	for i := 0; i < expected; i++ {
		resp, err := nc.RequestWithContext(context.Background(), Subject, []byte(fmt.Sprintf("Message %d\n", i+1)))
		if err != nil {
			t.Error(err)
		}
		t.Log(string(resp.Data))
	}

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
	//natsTest := NewNatsConnection()
	//defer natsTest.Terminate()
	//nc := natsTest.NatsConn
	nc, err := nats.Connect(nats.DefaultURL, nats.UserInfo(os.Getenv("NATS_USER"), os.Getenv("NATS_PASSWORD")))
	if err != nil {
		t.Error(err)
		return
	}

	sent := 0
	received1 := 0
	received2 := 0

	// When subscribing to a subject
	log.Println()
	subs, err := nc.Subscribe(Subject, func(m *nats.Msg) {
		log.Printf("Received_1: %s", m.Data)
		received1++
	})
	if err != nil {
		t.Error(err)
	}

	// Simulate subscriber failure at 300ms
	go func() {
		timer := time.NewTimer(300 * time.Millisecond)
		for range timer.C {
			_ = subs.Drain()
			_ = subs.Unsubscribe()
		}
	}()

	// Simulate subscriber recovery at 800ms
	resGroup := sync.WaitGroup{}
	resGroup.Add(1)
	go func() {
		timer := time.NewTimer(800 * time.Millisecond)
		for range timer.C {
			log.Println()
			_, err := nc.Subscribe(Subject, func(m *nats.Msg) {
				log.Printf("Received_2: %s", m.Data)
				received2++
			})
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
			if err := nc.Publish(Subject, []byte(fmt.Sprintf("Message %d\n", sent))); err != nil {
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

	// Make sure we receive all published messages
	_ = nc.Drain()
	for nc.IsDraining() {
	}

	received := received1 + received2
	// It receive less than 10 messages because there was no subscriber for ~200ms
	if received >= sent {
		t.Errorf("waiting lens than %d, got %d", sent, received)
	}
	log.Printf("Sent: %d, Received_1: %d, Received_2: %d\n", sent, received1, received2)
}
