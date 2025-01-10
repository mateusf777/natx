## natx - project for testing and learn nats/jetstream

Before start using it just run:
```
podman run --network host -p 4222:4222 nats -js
```
or
```
docker run --network host -p 4222:4222 nats --js
```


Some tests on NATS:

```
go test ./test -v
```

Build:
```
mkdir build
cd build
go build ../cmd/client                 
go build ../cmd/consumer
go build ../cmd/producer
go build ../cmd/server
go build ../cmd/query
```

Run:

Start the producer:
```
./producer
```
In another terminal tab/window. start the consumer:
```
./consumer
```
Play with stopping consumer and producer and seeing what happens.

Start the server:
```
./server
```
In another tab/window, issues commands with the client:
```
./client add key test

./client get key
```
