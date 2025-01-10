package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	js, _ := jetstream.New(nc)

	_, _ = js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:      "TEST",
		Subjects:  []string{"TEST.*"},
		Retention: jetstream.InterestPolicy,
	})

	_, err = js.CreateOrUpdateConsumer(context.Background(), "TEST", jetstream.ConsumerConfig{
		Durable:       "CONS_TEST",
		FilterSubject: "TEST.message",
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	sent := 0
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		log.Println("publishing message ", sent)
		if _, err := js.Publish(context.Background(), "TEST.message", []byte(fmt.Sprintf("Message %d", sent))); err != nil {
			log.Println(err)
		}
		sent++
	}
}
