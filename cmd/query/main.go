package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/go-faker/faker/v4"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type DataA struct {
	ID   string
	Name string
}

type DataB struct {
	ID   string
	Name string
	Date time.Time
}

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatalf("failed to connect, %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("failed to get jetstream, %v", err)
	}

	// The Stream Data is my database
	stream, err := js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:     "Data",
		Subjects: []string{"data.>"},
	})
	if err != nil {
		log.Fatalf("failed to get create/update stream, %v", err)
	}

	info, _ := stream.Info(context.Background())
	if info.State.Msgs < 20 {
		// Inserting data in a "virtual" table "a"
		for range 10 {
			var dataA DataA
			_ = faker.FakeData(&dataA)

			payload, _ := json.Marshal(dataA)

			// The subject is the primary key, it could be composed of other attributes for indexing
			_, _ = js.Publish(context.Background(), "data.table.a."+dataA.ID, payload)
		}

		// Inserting data in a "virtual" table "b"
		for range 10 {
			var dataB DataB
			_ = faker.FakeData(&dataB)

			payload, _ := json.Marshal(dataB)

			// The subject is the primary key, it could be composed of other attributes for indexing
			_, _ = js.Publish(context.Background(), "data.table.b."+dataB.ID, payload)
		}

	}

	pageSize := 100

	queryAOutput, _ := QueryStream[DataA](context.Background(), js, QueryStreamInput{
		Stream:   "Data",
		Filter:   []string{"data.table.a.>"},
		PageSize: pageSize,
	})

	var lastA string
	for _, a := range queryAOutput.Output {
		fmt.Printf("%v\n", a)
		lastA = a.ID
	}

	// Would need another util function like GetItemStream to avoid need to put PageSize = 1
	// like this example
	queryAOutput, _ = QueryStream[DataA](context.Background(), js, QueryStreamInput{
		Stream:   "Data",
		Filter:   []string{"data.table.a." + lastA},
		PageSize: 1,
	})

	fmt.Printf("%v\n", queryAOutput.Output[0])

	// It's possible to delete messages if we keep a reference to the sequence the message has in the stream
	// Then, we can -> stream.DeleteMsg(ctx, seq)
	// To avoid leaking nats msg seq into the data structures, the QueryStreamOutput could return "Items" that would
	// contain the metadata with the seq and the "record" itself

	// Updates would be like inserts in the same "unique" subject
	// Then, to get the latest, one would need to use the policy DeliverLastPolicy
	// But all the history is there to be retrieved

	// Another example just to confirm the "virtual" tables work
	queryBOutput, _ := QueryStream[DataB](context.Background(), js, QueryStreamInput{
		Stream:   "Data",
		Filter:   []string{"data.table.b.>"},
		PageSize: pageSize,
	})

	for _, b := range queryBOutput.Output {
		fmt.Printf("%v\n", b)
	}

}

type QueryStreamInput struct {
	Stream        string
	Filter        []string
	PageSize      int
	StartSequence uint64
}

type QueryStreamOutput[T any] struct {
	Output       []T
	NextSequence *uint64
}

func QueryStream[T any](ctx context.Context, js jetstream.JetStream, input QueryStreamInput) (QueryStreamOutput[T], error) {
	val := make([]T, 0)
	seq := input.StartSequence
	if input.StartSequence == 0 {
		seq = 1
	}

	consumer, err := js.CreateOrUpdateConsumer(ctx, input.Stream, jetstream.ConsumerConfig{
		FilterSubjects: input.Filter,
		DeliverPolicy:  jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:    seq,
	})
	if err != nil {
		log.Fatalf("failed to get create/update consumer, %v", err)
	}

	batch, err := consumer.FetchNoWait(input.PageSize + 1)
	if err != nil {
		return QueryStreamOutput[T]{}, err
	}
	if batch.Error() != nil {
		return QueryStreamOutput[T]{}, err
	}

	for msg := range batch.Messages() {
		metadata, err := msg.Metadata()
		if err != nil {
			log.Printf("%v\n", err)
			break
		}
		seq = metadata.Sequence.Stream

		if len(val) == input.PageSize {
			return QueryStreamOutput[T]{
				Output:       val,
				NextSequence: &seq,
			}, nil
		}

		var t T
		if err := json.Unmarshal(msg.Data(), &t); err != nil {
			log.Printf("%v", err)
			continue
		}
		val = append(val, t)
	}

	return QueryStreamOutput[T]{
		Output: val,
	}, nil
}
