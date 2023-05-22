package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/NicolasDutronc/goblins"
	"github.com/NicolasDutronc/goblins/sdk/client"
	"google.golang.org/grpc"
)

type WorkflowResultFuture[T any] struct {
	isReady bool
	result  *T
	err     error

	workflowRunId string
	conn          *grpc.ClientConn
}

// Wait for the future to complete and return the result
func (f *WorkflowResultFuture[T]) Get(ctx context.Context, timeout time.Duration) (*T, error, error) {
	if f.isReady {
		return f.result, f.err, nil
	}

	ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	client := goblins.NewGoblinsServiceClient(f.conn)
	response, err := client.GetWorkflowResult(ctxWithTimeout, &goblins.GetWorkflowResultRequest{WorkflowRunId: f.workflowRunId})
	if err != nil {
		return nil, nil, err
	}

	if len(response.Error) > 0 {
		return nil, fmt.Errorf(response.Error), nil
	}

	var output T
	if err := json.Unmarshal(response.Output, &output); err != nil {
		log.Printf("failed to unmarshal response: %s", response.Output)
		return nil, nil, err
	}
	return &output, nil, nil
}

// ExecuteWorkflow
func ExecuteWorkflow[IN, OUT any](ctx context.Context, c *client.GoblinsClient, workflowId, workflowRunId string, input IN) *WorkflowResultFuture[OUT] {
	client := goblins.NewGoblinsServiceClient(c.Conn)

	workflowHistory, err := client.GetWorkflowRunHistory(ctx, &goblins.GetWorkflowRunHistoryRequest{WorkflowRunId: workflowRunId})
	if err != nil {
		return &WorkflowResultFuture[OUT]{
			isReady: true,
			err:     errors.Join(fmt.Errorf("could not get workflow run history for workflow (workflow_id: %s, workflow_run_id: %s)", workflowId, workflowRunId), err),
		}
	}
	for _, event := range workflowHistory.EventList {
		if event.EventType == goblins.WorkflowEvent_WORKFLOW_FINISHED_IN_SUCCESS {
			var output OUT
			if err := json.Unmarshal(event.Output, &output); err != nil {
				return &WorkflowResultFuture[OUT]{
					isReady: true,
					err:     errors.Join(fmt.Errorf("output is not json serializable : %v", event.Output), err),
				}
			}

			return &WorkflowResultFuture[OUT]{
				isReady: true,
				result:  &output,
			}
		}
	}

	inputBytes, err := json.Marshal(input)
	if err != nil {
		return &WorkflowResultFuture[OUT]{
			isReady: true,
			err:     errors.Join(fmt.Errorf("input is not json serializable : %v", input), err),
		}
	}

	if _, err := client.ScheduleWorkflow(ctx, &goblins.ScheduleWorkflowRequest{
		WorkflowId:    workflowId,
		WorkflowRunId: workflowRunId,
		Input:         inputBytes,
	}); err != nil {
		return &WorkflowResultFuture[OUT]{
			isReady: true,
			err:     errors.Join(fmt.Errorf("could not schdule workflow %s", workflowId), err),
		}
	}

	return &WorkflowResultFuture[OUT]{
		isReady:       false,
		workflowRunId: workflowRunId,
		conn:          c.Conn,
	}
}
