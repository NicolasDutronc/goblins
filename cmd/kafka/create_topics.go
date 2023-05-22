package main

import (
	"context"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	client, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})
	if err != nil {
		panic(err)
	}

	results, err := client.CreateTopics(context.Background(), []kafka.TopicSpecification{
		{
			Topic:         "tasks",
			NumPartitions: 10,
		},
		{
			Topic:         "events",
			NumPartitions: 10,
		},
	})
	if err != nil {
		panic(err)
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError {
			log.Fatalln(result.Error)
		}
	}

}
