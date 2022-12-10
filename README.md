## natx - project for testing and learn nats/jetstream

This project used to have testcontainers to make it easy for running the test.
Before start using it just run: 

```
docker run --network host -p 4222:4222 nats -js
```
Build: (you might need to adjust the folder creation)

```
mkdir build
go build ./cmd/client                 
go build ./cmd/consumer
go build ./cmd/producer
go build ./cmd/server
```