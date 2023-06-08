package worker

import (
	"github.com/NicolasDutronc/goblins/shared/event"
	"github.com/NicolasDutronc/goblins/worker"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// CreateWorker creates a new worker
// TODO: instead of having to pass kafka bootstrap servers, it would be better to ask the server for any configuration needed
func CreateWorker(id string, parallelism int, bufferSize int, kafkaBootstrapServers string, serverUrl string) (*worker.Worker, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrapServers,
	})
	if err != nil {
		return nil, err
	}

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrapServers,
		"group.id":          "worker_group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return nil, err
	}

	conn, err := grpc.Dial(serverUrl, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return worker.NewWorker(
		id,
		parallelism,
		bufferSize,
		worker.NewInMemoryRegistry(),
		event.NewKafkaEventDispatcher(producer),
		consumer,
		conn,
		&worker.ReflectionFunctionExecutor{},
	), nil
}
