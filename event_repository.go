package goblins

import (
	"context"
	"log"
	"time"

	"github.com/gocql/gocql"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type eventRepository interface {
	storeEvent(ctx context.Context, event *WorkflowEvent) error
	getWorkflowRunEvents(ctx context.Context, workflowRunId string) ([]*WorkflowEvent, error)
	getActivityRunEvents(ctx context.Context, activityRunId string) ([]*WorkflowEvent, error)
}

type monitoringRepository struct {
	eventRepository eventRepository
}

func (r *monitoringRepository) storeEvent(ctx context.Context, event *WorkflowEvent) error {
	begin := time.Now()
	err := r.eventRepository.storeEvent(ctx, event)
	log.Printf("EVENT REPOSITORY - storeEvent took %v", time.Since(begin))
	return err
}

func (r *monitoringRepository) getWorkflowRunEvents(ctx context.Context, workflowRunId string) ([]*WorkflowEvent, error) {
	begin := time.Now()
	events, err := r.eventRepository.getWorkflowRunEvents(ctx, workflowRunId)
	log.Printf("EVENT REPOSITORY - getWorkflowRunEvents took %v", time.Since(begin))
	return events, err
}

func (r *monitoringRepository) getActivityRunEvents(ctx context.Context, activityRunId string) ([]*WorkflowEvent, error) {
	begin := time.Now()
	events, err := r.eventRepository.getActivityRunEvents(ctx, activityRunId)
	log.Printf("EVENT REPOSITORY - getActivityRunEvents took %v", time.Since(begin))
	return events, err
}

type cassandraEventRepository struct {
	session *gocql.Session
}

func newCassandraEventRepository(hosts ...string) (eventRepository, error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = "goblins"
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return &cassandraEventRepository{
		session,
	}, nil
}

func (c *cassandraEventRepository) storeEvent(ctx context.Context, event *WorkflowEvent) error {

	return c.session.Query(`
	INSERT INTO events (event_type, emitted_instant, workflow_id, workflow_run_id, activity_id, activity_run_id, activity_max_retries, activity_current_try, input, output, error)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		int8(event.EventType),
		event.EmittedAt.AsTime(),
		event.WorkflowId,
		event.WorkflowRunId,
		event.ActivityId,
		event.ActivityRunId,
		event.ActivityMaxRetries,
		event.ActivityCurrentTry,
		event.Input,
		event.Output,
		event.Error,
	).Consistency(gocql.Quorum).WithContext(ctx).Exec()
}

func (c *cassandraEventRepository) getWorkflowRunEvents(ctx context.Context, workflowRunId string) ([]*WorkflowEvent, error) {
	scanner := c.session.Query(`
	SELECT event_type, emitted_instant, workflow_id, workflow_run_id, activity_id, activity_run_id, input, output, error 
	FROM events 
	WHERE workflow_run_id = ?
	`, workflowRunId).Consistency(gocql.Quorum).WithContext(ctx).Iter().Scanner()
	defer scanner.Err()

	return c.scan(ctx, scanner)
}

func (c *cassandraEventRepository) getActivityRunEvents(ctx context.Context, activityRunId string) ([]*WorkflowEvent, error) {
	scanner := c.session.Query(`
	SELECT event_type, emitted_instant, workflow_id, workflow_run_id, activity_id, activity_run_id, activity_max_retries, activity_current_retry, input, output, error 
	FROM events 
	WHERE activity_run_id = ?
	`, activityRunId).Consistency(gocql.Quorum).WithContext(ctx).Iter().Scanner()

	return c.scan(ctx, scanner)
}

func (c *cassandraEventRepository) scan(ctx context.Context, scanner gocql.Scanner) ([]*WorkflowEvent, error) {
	eventList := []*WorkflowEvent{}
	for scanner.Next() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			var (
				eventType          int8
				emittedInstant     time.Time
				workflowId         string
				workflowRunId      string
				activityId         string
				activityRunId      string
				activityMaxRetries int
				activityCurrentTry int
				input              []byte
				output             []byte
				errorMsg           string
			)

			if err := scanner.Scan(&eventType, emittedInstant, workflowId, workflowRunId, activityId, activityRunId, activityMaxRetries, activityCurrentTry, input, output, errorMsg); err != nil {
				return nil, err
			}
			eventList = append(eventList, &WorkflowEvent{
				EventType:          WorkflowEvent_EventType(eventType),
				EmittedAt:          timestamppb.New(emittedInstant),
				WorkflowId:         workflowId,
				WorkflowRunId:      workflowRunId,
				ActivityId:         activityId,
				ActivityRunId:      activityRunId,
				ActivityMaxRetries: int32(activityMaxRetries),
				ActivityCurrentTry: int32(activityCurrentTry),
				Input:              input,
				Output:             output,
				Error:              &errorMsg,
			})
		}
	}

	return eventList, nil
}
