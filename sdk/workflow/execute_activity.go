package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/NicolasDutronc/goblins"
	"google.golang.org/grpc"
)

type NoTaskInContextError struct {
	ctx context.Context
}

func (n *NoTaskInContextError) Error() string {
	return fmt.Sprintf("no task in context: %v", n.ctx)
}

type NoWorkerContextInContextError struct {
	ctx context.Context
}

func (n *NoWorkerContextInContextError) Error() string {
	return fmt.Sprintf("no worker context in context: %v", n.ctx)
}

type TooManyRetriesError struct {
	workflowId string
	activityId string
}

func (t *TooManyRetriesError) Error() string {
	return fmt.Sprintf("tried too many times to execute activity %s in workflow %s", t.activityId, t.workflowId)
}

type ActivityResultFuture[T any] struct {
	isReady bool
	result  *T
	err     error

	activityRunId string
	conn          *grpc.ClientConn
	activityFunc  any
}

// Wait for the future to complete and return the result
func (f *ActivityResultFuture[T]) Get(ctx context.Context, timeout time.Duration) (*T, error, error) {
	if f.isReady {
		return f.result, f.err, nil
	}
	ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	client := goblins.NewGoblinsServiceClient(f.conn)
	response, err := client.GetActivityResult(ctxWithTimeout, &goblins.GetActivityResultRequest{
		ActivityRunId: f.activityRunId,
	})

	log.Printf("got activity result: %v", response)
	if err != nil {
		return nil, nil, err
	}
	if len(response.Error) > 0 {
		f.err = fmt.Errorf(response.Error)
		return nil, f.err, nil
	}

	var deserialized T
	if err := json.Unmarshal(response.Output, &deserialized); err != nil {
		return nil, nil, err
	}

	return &deserialized, nil, nil
}

// ExecuteActivity runs an activity function.
// The worker calling this function first requests activity run history and returns the result if it had already been run
// If not the worker requests the server to schedule the activity
func ExecuteActivity[IN, OUT any](ctx context.Context, activityId, activityRunId string, maxRetries int, input IN) *ActivityResultFuture[OUT] {
	log.Printf("starting to execute activity %s with input %v", activityId, input)

	// ExecuteActivity is meant to be run inside a workflow
	// Thus we have access to the workflow task and the workerContext through ctx (see worker.go in handleWorkflowTask)

	workflowTask := goblins.GetWorkflowTaskFromContext(ctx)
	if workflowTask == nil {
		return &ActivityResultFuture[OUT]{
			isReady: true,
			err:     &NoTaskInContextError{ctx},
		}
	}
	workerContext := goblins.GetWorkerContext(ctx)
	if workerContext == nil {
		return &ActivityResultFuture[OUT]{
			isReady: true,
			err:     &NoWorkerContextInContextError{ctx},
		}
	}

	// verify the activity is in the registry
	activityFunc, err := workerContext.GetRegistry().LookupActivity(ctx, activityId)
	if err != nil {
		return &ActivityResultFuture[OUT]{
			isReady: true,
			err:     fmt.Errorf("error looking up activity %s", activityId),
		}
	}

	if activityFunc == nil {
		return &ActivityResultFuture[OUT]{
			isReady: true,
			err:     fmt.Errorf("activity %s was not found", activityId),
		}
	}

	// fetch event history for this activity run id to skip it if it has already been done
	client := goblins.NewGoblinsServiceClient(workerContext.GetClientConn())
	var currentTry int32 = 0
	history, err := client.GetActivityRunHistory(ctx, &goblins.GetActivityRunHistoryRequest{ActivityRunId: activityRunId})
	if err != nil {
		log.Printf("could not get activity run history for activity (workflow_id: %s, workflow_run_id: %s, activity_id: %s, activity_run_id: %s)", workflowTask.WorkflowId, workflowTask.WorkflowRunId, activityId, activityRunId)
		history = &goblins.GetActivityRunHistoryResponse{
			EventList: []*goblins.WorkflowEvent{},
		}
		currentTry = 1
	}

	for _, event := range history.EventList {
		// success found, just deserialize the result and return
		if event.EventType == goblins.WorkflowEvent_ACTIVITY_FINISHED_IN_SUCCESS {
			var output OUT
			if err := json.Unmarshal(event.Output, &output); err != nil {
				return &ActivityResultFuture[OUT]{
					isReady: true,
					err:     errors.Join(fmt.Errorf("output is not json serializable : %v", event.Output), err),
				}
			}
			return &ActivityResultFuture[OUT]{
				isReady: true,
				result:  &output,
			}
		}

		// compute the max event.ActivityCurrentTry to get the real current try
		if event.EventType == goblins.WorkflowEvent_ACTIVITY_FINISHED_IN_ERROR && event.ActivityCurrentTry > currentTry {
			currentTry = event.ActivityCurrentTry

			// too many retries, abort
			if maxRetries <= int(currentTry) {
				return &ActivityResultFuture[OUT]{
					isReady:       true,
					activityRunId: event.ActivityRunId,
					err: &TooManyRetriesError{
						workflowId: event.WorkflowId,
						activityId: event.ActivityId,
					},
				}
			}
		}
	}

	// serialize input
	inputBytes, err := json.Marshal(input)
	if err != nil {
		return &ActivityResultFuture[OUT]{
			isReady: true,
			err:     errors.Join(fmt.Errorf("input is not json serializable : %v", input), err),
		}
	}

	if _, err := client.ScheduleActivity(ctx, &goblins.ScheduleActivityRequest{
		WorkflowId:         workflowTask.WorkflowId,
		WorkflowRunId:      workflowTask.WorkflowRunId,
		ActivityId:         activityId,
		ActivityRunId:      activityRunId,
		ActivityMaxRetries: int32(maxRetries),
		ActivityCurrentTry: currentTry + 1,
		Input:              inputBytes,
	}); err != nil {
		return &ActivityResultFuture[OUT]{
			isReady: true,
			err:     errors.Join(fmt.Errorf("could not schedule activity %s", activityId), err),
		}
	}

	return &ActivityResultFuture[OUT]{
		isReady:       false,
		conn:          workerContext.GetClientConn(),
		activityRunId: activityRunId,
		activityFunc:  activityFunc,
	}
}
