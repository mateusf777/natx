package main

import (
	"context"
	"log"
	"os"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	instance, _ := uuid.NewUUID()
	log.Println("Instance:", instance)

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

	consumer, _ := js.CreateOrUpdateConsumer(context.Background(), "TEST", jetstream.ConsumerConfig{
		Durable:       "CONS_TEST",
		FilterSubject: "TEST.message",
		AckPolicy:     jetstream.AckAllPolicy,
	})

	_, _ = consumer.Consume(func(msg jetstream.Msg) {
		log.Printf("Received: %s, instance: %s", msg.Data(), instance)
		_ = msg.Ack()
	})

	sig := make(chan os.Signal, 1)
	log.Println("running...")
	<-sig
	log.Println("stop")
}
