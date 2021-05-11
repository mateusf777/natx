package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

var instance string

func main() {
	instance = os.Args[1]

	nc, err := nats.Connect(nats.DefaultURL, nats.UserInfo(os.Getenv("NATS_USER"), os.Getenv("NATS_PASSWORD")))
	if err != nil {
		panic(err)
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
		panic(err)
	}

	//_, err = js.Subscribe("TEST.message", func(msg *nats.Msg) {
	//	fmt.Println(string(msg.Data), " instance: ", instance)
	//	_ = msg.Ack()
	//}, nats.Durable("CONS_TEST"))
	//if err != nil {
	//	panic(err)
	//}

	sub, err := js.PullSubscribe("TEST.message", "CONS_TEST")
	if err != nil {
		log.Println("pull sub: ", err)
	}

	timer := time.NewTicker(time.Second)
	for range timer.C {
		for {
			msgs, err := sub.Fetch(1, nats.Context(context.Background()))
			if err != nil {
				log.Println("sub fetch: ", err)
				break
			}
			for _, msg := range msgs {
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
