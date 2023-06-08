package eventloop

import (
	"context"
	"log"

	"github.com/NicolasDutronc/goblins/shared/event"
	"github.com/NicolasDutronc/goblins/shared/task"
)

type ActivityRetryer struct {
	eventDispatcher event.EventDispatcher
	taskDispatcher  task.TaskDispatcher
	eventRepository event.EventRepository
}

func NewActivityRetryer(eventDispatcher event.EventDispatcher, taskDispatcher task.TaskDispatcher, eventRepository event.EventRepository) *ActivityRetryer {
	return &ActivityRetryer{
		eventDispatcher,
		taskDispatcher,
		eventRepository,
	}
}

func (r *ActivityRetryer) HandleEvent(ctx context.Context, workflowEvent *event.WorkflowEvent) {
	if workflowEvent.EventType != event.WorkflowEvent_ACTIVITY_FINISHED_IN_ERROR {
		return
	}

	if workflowEvent.ActivityCurrentTry == workflowEvent.ActivityMaxRetries {
		return
	}

	// in case we go through old messages, we need to ensure there are no termination events for this activity run

	events, err := r.eventRepository.GetActivityRunEvents(ctx, workflowEvent.WorkflowRunId, workflowEvent.ActivityRunId)
	if err != nil {
		log.Printf("retryer could not fetch activity run %s events: %v", workflowEvent.ActivityRunId, err)
	}

	for _, activityEvent := range events {
		if activityEvent.EventType == event.WorkflowEvent_ACTIVITY_FINISHED_IN_SUCCESS ||
			activityEvent.EventType == event.WorkflowEvent_ACTIVITY_FINISHED_IN_ERROR && activityEvent.ActivityCurrentTry == activityEvent.ActivityMaxRetries {
			return
		}
	}

	log.Printf("ACTIVITY RETRYER - retrying activity run %s", workflowEvent.ActivityRunId)

	if err := r.taskDispatcher.SendActivityTask(
		workflowEvent.WorkflowId,
		workflowEvent.ActivityRunId,
		workflowEvent.ActivityId,
		workflowEvent.ActivityRunId,
		workflowEvent.ActivityMaxRetries,
		workflowEvent.ActivityCurrentTry+1,
		workflowEvent.Input,
	); err != nil {
		log.Printf("ACTIVITY RETRYER - failed to re-schedule activity run %s: %v", workflowEvent.ActivityRunId, err)
		return
	}

	if err := r.eventDispatcher.SendActivityScheduledEvent(
		workflowEvent.WorkflowId,
		workflowEvent.WorkflowRunId,
		workflowEvent.ActivityId,
		workflowEvent.ActivityRunId,
		workflowEvent.ActivityMaxRetries,
		workflowEvent.ActivityCurrentTry+1,
		workflowEvent.Input,
	); err != nil {
		log.Printf("ACTIVITY RETRYER - failed to send activity scheduled event for activity run %s: %v", workflowEvent.ActivityRunId, err)
	}
}
