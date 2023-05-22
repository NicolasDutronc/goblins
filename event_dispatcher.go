package goblins

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type EventDispatcher interface {
	SendWorkflowScheduledEvent(workflowId, workflowRunId string, input []byte) error

	SendWorkflowStartedEvent(workflowId, workflowRunId string, input []byte) error

	SendWorkflowFinishedInErrorEvent(workflowId, workflowRunId string, input []byte, err error) error

	SendWorkflowFinishedInSuccessEvent(workflowId, workflowRunId string, input, output []byte) error

	SendActivityScheduledEvent(workflowId, workflowRunId, activityId, activityRunId string, maxRetries, currentTry int32, input []byte) error

	SendActivityStartedEvent(workflowId, workflowRunId, activityId, activityRunId string, maxRetries, currentTry int32, input []byte) error

	SendActivityFinishedInErrorEvent(workflowId, workflowRunId, activityId, activityRunId string, maxRetries, currentTry int32, input []byte, err error) error

	SendActivityFinishedInSuccessEvent(workflowId, workflowRunId, activityId, activityRunId string, maxRetries, currentTry int32, input, output []byte) error
}

type KafkaEventDispatcher struct {
	producer *kafka.Producer
}

func newKafkaEventDispatcher(producer *kafka.Producer) EventDispatcher {
	return &KafkaEventDispatcher{
		producer: producer,
	}
}

func (k *KafkaEventDispatcher) SendWorkflowScheduledEvent(workflowId string, workflowRunId string, input []byte) error {
	scheduledEvent := WorkflowEvent{
		EventType:     WorkflowEvent_WORKFLOW_SCHEDULED,
		EmittedAt:     timestamppb.Now(),
		WorkflowId:    workflowId,
		WorkflowRunId: workflowRunId,
		Input:         input,
	}
	return k.sendEventToKafka(&scheduledEvent)
}

func (k *KafkaEventDispatcher) SendWorkflowStartedEvent(workflowId string, workflowRunId string, input []byte) error {
	scheduledEvent := WorkflowEvent{
		EventType:     WorkflowEvent_WORKFLOW_STARTED,
		EmittedAt:     timestamppb.Now(),
		WorkflowId:    workflowId,
		WorkflowRunId: workflowRunId,
		Input:         input,
	}
	return k.sendEventToKafka(&scheduledEvent)
}

func (k *KafkaEventDispatcher) SendWorkflowFinishedInErrorEvent(workflowId string, workflowRunId string, input []byte, workflowError error) error {
	errString := workflowError.Error()
	workflowErrorEvent := WorkflowEvent{
		EventType:     WorkflowEvent_WORKFLOW_FINISHED_IN_ERROR,
		EmittedAt:     timestamppb.Now(),
		WorkflowId:    workflowId,
		WorkflowRunId: workflowRunId,
		Input:         input,
		Error:         &errString,
	}
	return k.sendEventToKafka(&workflowErrorEvent)
}

func (k *KafkaEventDispatcher) SendWorkflowFinishedInSuccessEvent(workflowId string, workflowRunId string, input []byte, output []byte) error {
	workflowSuccessEvent := WorkflowEvent{
		EventType:     WorkflowEvent_WORKFLOW_FINISHED_IN_SUCCESS,
		EmittedAt:     timestamppb.Now(),
		WorkflowId:    workflowId,
		WorkflowRunId: workflowRunId,
		Input:         input,
		Output:        output,
	}
	return k.sendEventToKafka(&workflowSuccessEvent)
}

func (k *KafkaEventDispatcher) SendActivityScheduledEvent(workflowId string, workflowRunId string, activityId string, activityRunId string, maxRetries, currentTry int32, input []byte) error {
	activityScheduledEvent := WorkflowEvent{
		EventType:          WorkflowEvent_ACTIVITY_SCHEDULED,
		EmittedAt:          timestamppb.Now(),
		WorkflowId:         workflowId,
		WorkflowRunId:      workflowRunId,
		ActivityId:         activityId,
		ActivityRunId:      activityRunId,
		ActivityMaxRetries: maxRetries,
		ActivityCurrentTry: currentTry,
		Input:              input,
	}
	return k.sendEventToKafka(&activityScheduledEvent)
}

func (k *KafkaEventDispatcher) SendActivityStartedEvent(workflowId string, workflowRunId string, activityId string, activityRunId string, maxRetries, currentTry int32, input []byte) error {
	activityStartedEvent := WorkflowEvent{
		EventType:          WorkflowEvent_ACTIVITY_STARTED,
		EmittedAt:          timestamppb.Now(),
		WorkflowId:         workflowId,
		WorkflowRunId:      workflowRunId,
		ActivityId:         activityId,
		ActivityRunId:      activityRunId,
		ActivityMaxRetries: maxRetries,
		ActivityCurrentTry: currentTry,
		Input:              input,
	}
	return k.sendEventToKafka(&activityStartedEvent)
}

func (k *KafkaEventDispatcher) SendActivityFinishedInErrorEvent(workflowId string, workflowRunId string, activityId string, activityRunId string, maxRetries, currentTry int32, input []byte, activityError error) error {
	errString := activityError.Error()
	activityErrorEvent := WorkflowEvent{
		EventType:          WorkflowEvent_ACTIVITY_FINISHED_IN_ERROR,
		EmittedAt:          timestamppb.Now(),
		WorkflowId:         workflowId,
		WorkflowRunId:      workflowRunId,
		ActivityId:         activityId,
		ActivityRunId:      activityRunId,
		ActivityMaxRetries: maxRetries,
		ActivityCurrentTry: currentTry,
		Input:              input,
		Error:              &errString,
	}
	return k.sendEventToKafka(&activityErrorEvent)
}

func (k *KafkaEventDispatcher) SendActivityFinishedInSuccessEvent(workflowId string, workflowRunId string, activityId string, activityRunId string, maxRetries, currentTry int32, input []byte, output []byte) error {
	activitySuccessEvent := WorkflowEvent{
		EventType:          WorkflowEvent_ACTIVITY_FINISHED_IN_SUCCESS,
		EmittedAt:          timestamppb.Now(),
		WorkflowId:         workflowId,
		WorkflowRunId:      workflowRunId,
		ActivityId:         activityId,
		ActivityRunId:      activityRunId,
		ActivityMaxRetries: maxRetries,
		ActivityCurrentTry: currentTry,
		Input:              input,
		Output:             output,
	}
	return k.sendEventToKafka(&activitySuccessEvent)
}

func (k *KafkaEventDispatcher) sendEventToKafka(event *WorkflowEvent) error {
	serialized, err := proto.Marshal(event)
	if err != nil {
		return err
	}
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &eventsTopic,
			Partition: kafka.PartitionAny,
		},
		Value: serialized,

		// the key here is necessary to ensure ordering of this workflow run events
		// messages with the same key go to the same partition
		// kafka guarantees ordering within partitions
		Key: []byte(event.WorkflowRunId),
	}

	return k.producer.Produce(msg, nil)
}
