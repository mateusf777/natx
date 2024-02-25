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
