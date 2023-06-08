package event

import (
	"context"
	"log"
	"time"

	"github.com/gocql/gocql"
	"google.golang.org/protobuf/types/known/timestamppb"
)

//go:generate mockery --name EventRepository
type EventRepository interface {
	StoreEvent(ctx context.Context, event *WorkflowEvent) error
	GetWorkflowRunEvents(ctx context.Context, workflowRunId string) ([]*WorkflowEvent, error)
	GetActivityRunEvents(ctx context.Context, workflowRunId string, activityRunId string) ([]*WorkflowEvent, error)
}

type LoggingRepository struct {
	eventRepository EventRepository
}

func NewLoggingRepository(underlyingRepository EventRepository) EventRepository {
	return &LoggingRepository{
		eventRepository: underlyingRepository,
	}
}

func (r *LoggingRepository) StoreEvent(ctx context.Context, event *WorkflowEvent) error {
	begin := time.Now()
	err := r.eventRepository.StoreEvent(ctx, event)
	log.Printf("EVENT REPOSITORY - storeEvent took %v", time.Since(begin))
	return err
}

func (r *LoggingRepository) GetWorkflowRunEvents(ctx context.Context, workflowRunId string) ([]*WorkflowEvent, error) {
	begin := time.Now()
	events, err := r.eventRepository.GetWorkflowRunEvents(ctx, workflowRunId)
	log.Printf("EVENT REPOSITORY - getWorkflowRunEvents took %v", time.Since(begin))
	return events, err
}

func (r *LoggingRepository) GetActivityRunEvents(ctx context.Context, workflowRunId string, activityRunId string) ([]*WorkflowEvent, error) {
	begin := time.Now()
	events, err := r.eventRepository.GetActivityRunEvents(ctx, workflowRunId, activityRunId)
	log.Printf("EVENT REPOSITORY - getActivityRunEvents took %v", time.Since(begin))
	return events, err
}

type CassandraEventRepository struct {
	session *gocql.Session
}

func NewCassandraEventRepository(hosts ...string) (EventRepository, error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = "goblins"
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return &CassandraEventRepository{
		session,
	}, nil
}

func NewCassandraEventRepositoryFromSession(session *gocql.Session) EventRepository {
	return &CassandraEventRepository{
		session,
	}
}

func (c *CassandraEventRepository) StoreEvent(ctx context.Context, event *WorkflowEvent) error {
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

func (c *CassandraEventRepository) GetWorkflowRunEvents(ctx context.Context, workflowRunId string) ([]*WorkflowEvent, error) {
	scanner := c.session.Query(`
	SELECT event_type, emitted_instant, workflow_id, workflow_run_id, activity_id, activity_run_id, activity_max_retries, activity_current_try, input, output, error 
	FROM events 
	WHERE workflow_run_id = ?
	`, workflowRunId).Consistency(gocql.Quorum).WithContext(ctx).Iter().Scanner()
	defer scanner.Err()

	return c.scan(ctx, scanner)
}

func (c *CassandraEventRepository) GetActivityRunEvents(ctx context.Context, workflowRunId string, activityRunId string) ([]*WorkflowEvent, error) {
	scanner := c.session.Query(`
	SELECT event_type, emitted_instant, workflow_id, workflow_run_id, activity_id, activity_run_id, activity_max_retries, activity_current_try, input, output, error 
	FROM events 
	WHERE  workflow_run_id = ? AND activity_run_id = ?
	`, workflowRunId, activityRunId).Consistency(gocql.Quorum).WithContext(ctx).Iter().Scanner()

	return c.scan(ctx, scanner)
}

func (c *CassandraEventRepository) scan(ctx context.Context, scanner gocql.Scanner) ([]*WorkflowEvent, error) {
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

			if err := scanner.Scan(&eventType, &emittedInstant, &workflowId, &workflowRunId, &activityId, &activityRunId, &activityMaxRetries, &activityCurrentTry, &input, &output, &errorMsg); err != nil {
				return nil, err
			}
			var eventError *string
			if len(errorMsg) != 0 {
				eventError = &errorMsg
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
				Error:              eventError,
			})
		}
	}

	return eventList, nil
}
