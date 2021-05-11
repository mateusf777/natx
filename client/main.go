package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/mateusf7777/natx/common"
	"github.com/nats-io/nats.go"
)

const (
	add = "add"
	get = "get"
)

func main() {
	if len(os.Args) < 2 {
		panic("should use sub-command add or get")
	}

	if os.Args[1] == add && len(os.Args) < 4 {
		panic("add usage: add <key> <value>")
	}

	if os.Args[1] == get && len(os.Args) < 3 {
		panic("get usage: get <key>")
	}

	nc, err := nats.Connect(nats.DefaultURL, nats.UserInfo(os.Getenv("NATS_USER"), os.Getenv("NATS_PASSWORD")))
	if err != nil {
		panic(err)
	}

	switch os.Args[1] {
	case add:
		addReq := common.AddStoreRequest{
			Key:   os.Args[2],
			Value: os.Args[3],
		}
		req, _ := json.Marshal(addReq)
		msg, err := nc.RequestWithContext(context.Background(), "service.store.add", req)
		if err != nil {
			panic(err)
		}
		var resp common.AddStoreResponse
		_ = json.Unmarshal(msg.Data, &resp)
		if resp.Err != nil {
			panic(resp.Err)
		}
		fmt.Println("OK!")

	case get:
		addReq := common.GetStoreRequest{
			Key: os.Args[2],
		}
		req, _ := json.Marshal(addReq)
		msg, err := nc.RequestWithContext(context.Background(), "service.store.get", req)
		if err != nil {
			panic(err)
		}
		log.Println("Get raw payload: ", string(msg.Data))
		var resp common.GetStoreResponse
		_ = json.Unmarshal(msg.Data, &resp)
		log.Printf("Get response payload: %+v\n", resp)
		if resp.Err != nil {
			_, _ = fmt.Fprint(os.Stderr, *resp.Err)
			os.Exit(1)
		}
		fmt.Println(resp.Value)
	}

}
