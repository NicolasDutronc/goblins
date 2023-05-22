package goblins

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/protobuf/proto"
)

type taskDispatcher interface {
	sendActivityTask(workflowId, workflowRunId, activityId, activityRunId string, maxRetry, currentTry int32, input []byte) error
	sendWorkflowTask(workflowId, workflowRunId string, input []byte) error
}

type kafkaTaskDispatcher struct {
	producer *kafka.Producer
}

func newKafkaTaskDispatcher(producer *kafka.Producer) taskDispatcher {
	return &kafkaTaskDispatcher{
		producer,
	}
}

func (k *kafkaTaskDispatcher) sendActivityTask(workflowId string, workflowRunId string, activityId string, activityRunId string, maxRetries, currentTry int32, input []byte) error {
	activityTask := &Task{
		TaskType:           Task_ACTIVITY_TASK,
		WorkflowId:         workflowId,
		WorkflowRunId:      workflowRunId,
		ActivityId:         activityId,
		ActivityRunId:      activityRunId,
		ActivityMaxRetries: maxRetries,
		ActivityCurrentTry: currentTry,
		Input:              input,
	}

	return k.sendTaskToKafka(activityTask)
}

func (k *kafkaTaskDispatcher) sendWorkflowTask(workflowId string, workflowRunId string, input []byte) error {
	workflowTask := &Task{
		TaskType:      Task_WORKFLOW_TASK,
		WorkflowId:    workflowId,
		WorkflowRunId: workflowRunId,
		Input:         input,
	}

	return k.sendTaskToKafka(workflowTask)
}

func (k *kafkaTaskDispatcher) sendTaskToKafka(task *Task) error {
	serialized, err := proto.Marshal(task)
	if err != nil {
		return err
	}
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &tasksTopic,
			Partition: kafka.PartitionAny,
		},
		Value: serialized,
		// msg has no key here because it is better to distribute the work across multiple workers
		// messages with the same key go to the same partition
		// a partition is assigned to only one member of the consumer group
	}

	return k.producer.Produce(msg, nil)
}
