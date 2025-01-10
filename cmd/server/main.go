package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"

	"github.com/mateusf7777/natx/common"
)

var store map[string]string
var instance uuid.UUID

func main() {
	instance, _ = uuid.NewUUID()

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	defer nc.Close()

	srv, err := micro.AddService(nc, micro.Config{
		Name:    "ServiceStore",
		Version: "1.0.0",
	})
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	store = make(map[string]string)

	g := srv.AddGroup("service.store", micro.WithGroupQueueGroup("store"))

	if err := g.AddEndpoint("add", micro.HandlerFunc(Add)); err != nil {
		log.Fatal(err)
	}
	if err := g.AddEndpoint("get", micro.HandlerFunc(Get)); err != nil {
		log.Fatal(err)
	}

	defer srv.Stop()

	sig := make(chan os.Signal, 1)
	log.Println("running...")
	<-sig
	log.Println("stop")
}

func Add(req micro.Request) {
	if req == nil {
		log.Println("Request is null")
		return
	}

	log.Println("Add called... instance:", instance)

	var request common.AddStoreRequest
	if err := json.Unmarshal(req.Data(), &request); err != nil {
		log.Printf("Could not get request, %+v", err)
		errMSG := fmt.Sprintf("could not get request, %+v", err)
		resp, _ := json.Marshal(common.AddStoreResponse{
			Err: &errMSG,
		})
		_ = req.Respond(resp)
	}

	log.Printf("Add payload: %+v\n", request)

	store[request.Key] = request.Value
	resp, _ := json.Marshal(common.AddStoreResponse{})
	_ = req.Respond(resp)
}

func Get(req micro.Request) {
	if req == nil {
		log.Println("Request is null")
		return
	}

	log.Println("Get called... instance:", instance)

	var request common.GetStoreRequest
	if err := json.Unmarshal(req.Data(), &request); err != nil {
		log.Printf("Could not get request, %+v", err)
		errMSG := fmt.Sprintf("could not get request, %+v", err)
		resp, _ := json.Marshal(common.GetStoreResponse{
			Err: &errMSG,
		})
		_ = req.Respond(resp)
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
		_ = req.Respond(resp)
	}

	resp, _ := json.Marshal(common.GetStoreResponse{
		Value: value,
	})
	_ = req.Respond(resp)
}
