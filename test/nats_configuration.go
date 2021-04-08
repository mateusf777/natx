package test

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type NatsConnection struct {
	NatsC    testcontainers.Container
	NatsConn *nats.Conn
}

func (c *NatsConnection) Terminate() {
	_ = c.NatsC.Terminate(context.Background())
}

func NewNatsConnection() (*NatsConnection, error) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "nats",
		ExposedPorts: []string{"4222/tcp"},
		WaitingFor:   wait.ForLog("Listening for client connections on 0.0.0.0:4222"),
	}
	natsC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}
	//defer natsC.Terminate(ctx)

	ip, err := natsC.Host(ctx)
	if err != nil {
		return nil, err
	}
	port, err := natsC.MappedPort(ctx, "4222")
	if err != nil {
		return nil, err
	}

	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%s", ip, port.Port()))
	if err != nil {
		return nil, err
	}

	return &NatsConnection{
		NatsC:    natsC,
		NatsConn: nc,
	}, nil
}
