package goblins

import (
	"context"
	"fmt"
)

type ErrorActivityAlreadyRegistered struct {
	activityId string
}

func (e *ErrorActivityAlreadyRegistered) Error() string {
	return fmt.Sprintf("activity %s has already been registered", e.activityId)
}

type ErrorWorkflowAlreadyRegistered struct {
	workflowId string
}

func (e *ErrorWorkflowAlreadyRegistered) Error() string {
	return fmt.Sprintf("workflow %s has already been registered", e.workflowId)
}

type registry interface {
	registerWorkflow(ctx context.Context, workflowId string, workflowFunction any) error
	registerActivity(ctx context.Context, activityId string, activityFunction any) error
	lookupWorkflow(ctx context.Context, workflowId string) (any, error)
	LookupActivity(ctx context.Context, activityId string) (any, error)
	getAllActivities(ctx context.Context) ([]string, error)
	getAllWorkflows(ctx context.Context) ([]string, error)
}

type inMemoryRegistry struct {
	workflows  map[string]any
	activities map[string]any
}

func NewInMemoryRegistry() registry {
	return &inMemoryRegistry{
		workflows:  map[string]any{},
		activities: map[string]any{},
	}
}

// lookupActivity implements registry
func (r *inMemoryRegistry) LookupActivity(ctx context.Context, activityId string) (any, error) {
	return r.activities[activityId], nil
}

// lookupWorkflow implements registry
func (r *inMemoryRegistry) lookupWorkflow(ctx context.Context, workflowId string) (any, error) {
	return r.workflows[workflowId], nil
}

// registerActivity implements registry
func (r *inMemoryRegistry) registerActivity(ctx context.Context, activityId string, activityFunction any) error {
	if _, exists := r.activities[activityId]; exists {
		return &ErrorActivityAlreadyRegistered{activityId: activityId}
	}

	r.activities[activityId] = activityFunction
	return nil
}

// registerWorkflow implements registry
func (r *inMemoryRegistry) registerWorkflow(ctx context.Context, workflowId string, workflowFunction any) error {
	if _, exists := r.workflows[workflowId]; exists {
		return &ErrorWorkflowAlreadyRegistered{workflowId: workflowId}
	}

	r.workflows[workflowId] = workflowFunction
	return nil
}

func (r *inMemoryRegistry) getAllActivities(ctx context.Context) ([]string, error) {
	activities := []string{}
	for id := range r.activities {
		activities = append(activities, id)
	}
	return activities, nil
}

func (r *inMemoryRegistry) getAllWorkflows(ctx context.Context) ([]string, error) {
	workflows := []string{}
	for id := range r.workflows {
		workflows = append(workflows, id)
	}
	return workflows, nil
}
