package main

import (
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

func main() {
	instance, _ := uuid.NewUUID()
	log.Println("Instance:", instance)

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	js, _ := nc.JetStream()

	// Add stream
	_, _ = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"TEST.*"},
		// Discards all acknowledge messages
		Retention: nats.InterestPolicy,
	})
	// Add consumer
	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable: "CONS_TEST",
		// Acknowledge all messages received by subscribers
		AckPolicy: nats.AckExplicitPolicy,
	})
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	sub, err := js.PullSubscribe("TEST.message", "CONS_TEST")
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	timer := time.NewTicker(time.Second)
	for range timer.C {
		for {
			messages, err := sub.Fetch(1)
			if err != nil {
				log.Println("sub fetch: ", err)
				break
			}
			for _, msg := range messages {
				log.Printf("Received: %s, instance: %s", msg.Data, instance)
				_ = msg.Ack()
			}
		}
	}

	sig := make(chan os.Signal, 1)
	log.Println("running...")
	<-sig
	log.Println("stop")
}
