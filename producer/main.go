package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	nc, err := nats.Connect(nats.DefaultURL, nats.UserInfo(os.Getenv("NATS_USER"), os.Getenv("NATS_PASSWORD")))
	if err != nil {
		panic(err)
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
