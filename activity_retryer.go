package goblins

import (
	"context"
	"log"
)

type activityRetryer struct {
	eventDispatcher EventDispatcher
	taskDispatcher  taskDispatcher
	eventRepository eventRepository
}

func (r *activityRetryer) handleEvent(ctx context.Context, event *WorkflowEvent) {
	if event.EventType != WorkflowEvent_ACTIVITY_FINISHED_IN_ERROR {
		return
	}

	if event.ActivityCurrentTry == event.ActivityMaxRetries {
		return
	}

	// in case we go through old messages, we need to ensure there are no termination events for this activity run

	events, err := r.eventRepository.getActivityRunEvents(ctx, event.ActivityRunId)
	if err != nil {
		log.Printf("retryer could not fetch activity run %s events: %v", event.ActivityRunId, err)
	}

	for _, activityEvent := range events {
		if activityEvent.EventType == WorkflowEvent_ACTIVITY_FINISHED_IN_SUCCESS ||
			activityEvent.EventType == WorkflowEvent_ACTIVITY_FINISHED_IN_ERROR && activityEvent.ActivityCurrentTry == activityEvent.ActivityMaxRetries {
			return
		}
	}

	log.Printf("ACTIVITY RETRYER - retrying activity run %s", event.ActivityRunId)

	if err := r.taskDispatcher.sendActivityTask(
		event.WorkflowId,
		event.ActivityRunId,
		event.ActivityId,
		event.ActivityRunId,
		event.ActivityMaxRetries,
		event.ActivityCurrentTry+1,
		event.Input,
	); err != nil {
		log.Printf("ACTIVITY RETRYER - failed to re-schedule activity run %s: %v", event.ActivityRunId, err)
		return
	}

	if err := r.eventDispatcher.SendActivityScheduledEvent(
		event.WorkflowId,
		event.WorkflowRunId,
		event.ActivityId,
		event.ActivityRunId,
		event.ActivityMaxRetries,
		event.ActivityCurrentTry+1,
		event.Input,
	); err != nil {
		log.Printf("ACTIVITY RETRYER - failed to send activity scheduled event for activity run %s: %v", event.ActivityRunId, err)
	}
}
