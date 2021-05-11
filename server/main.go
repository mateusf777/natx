package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/mateusf7777/natx/common"

	"github.com/nats-io/nats.go"
)

var store map[string]string
var instance string

func main() {
	instance = os.Args[1]

	nc, err := nats.Connect(nats.DefaultURL, nats.UserInfo(os.Getenv("NATS_USER"), os.Getenv("NATS_PASSWORD")))
	if err != nil {
		panic(err)
	}

	store = make(map[string]string)

	if _, err := nc.QueueSubscribe("service.store.add", "service", Add); err != nil {
		log.Printf("Could not subscribe, %v", err)
	}

	if _, err := nc.QueueSubscribe("service.store.get", "service", Get); err != nil {
		log.Printf("Could not subscribe, %v", err)
	}

	sig := make(chan os.Signal, 1)
	log.Println("running...")
	<-sig
	log.Println("stop")
}

func Add(msg *nats.Msg) {
	if msg == nil {
		log.Println("Message is null")
		return
	}

	log.Println("Add called... instance:", instance)

	var request common.AddStoreRequest
	if err := json.Unmarshal(msg.Data, &request); err != nil {
		log.Printf("Could not get request, %+v", err)
		errMSG := fmt.Sprintf("could not get request, %+v", err)
		resp, _ := json.Marshal(common.AddStoreResponse{
			Err: &errMSG,
		})
		_ = msg.Respond(resp)
	}

	log.Printf("Add payload: %+v\n", request)

	store[request.Key] = request.Value
	resp, _ := json.Marshal(common.AddStoreResponse{})
	_ = msg.Respond(resp)
}

func Get(msg *nats.Msg) {
	if msg == nil {
		log.Println("Message is null")
		return
	}

	log.Println("Get called... instance:", instance)

	var request common.GetStoreRequest
	if err := json.Unmarshal(msg.Data, &request); err != nil {
		log.Printf("Could not get request, %+v", err)
		errMSG := fmt.Sprintf("could not get request, %+v", err)
		resp, _ := json.Marshal(common.GetStoreResponse{
			Err: &errMSG,
		})
		_ = msg.Respond(resp)
	}

	log.Printf("Get payload: %+v\n", request)

	value, ok := store[request.Key]
	if !ok {
		log.Printf("key %s not found\n", request.Key)
		errMSG := fmt.Sprintf("could not find key[%s]", request.Key)
		resp, _ := json.Marshal(common.GetStoreResponse{
			Err: &errMSG,
		})
		fmt.Printf("Get response payload: %s\n", resp)
		_ = msg.Respond(resp)
	}

	resp, _ := json.Marshal(common.GetStoreResponse{
		Value: value,
	})
	_ = msg.Respond(resp)
}
