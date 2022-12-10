package main

import (
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	js, _ := nc.JetStream()

	sent := 0
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		log.Println("publishing message ", sent)
		if _, err := js.Publish("TEST.message", []byte(fmt.Sprintf("Message %d", sent))); err != nil {
			log.Println(err)
		}
		sent++
	}
}
