package server

import (
	"context"

	"github.com/gocql/gocql"
)

//go:generate mockery --name ServerRegistry
type ServerRegistry interface {
	RegisterActivity(ctx context.Context, activityId string) error
	RegisterWorkflow(ctx context.Context, workflowId string) error
	GetAllActivities(ctx context.Context) ([]string, error)
	GetAllWorkflows(ctx context.Context) ([]string, error)
}

type CassandraServerRegistry struct {
	session *gocql.Session
}

func NewCassandraServerRegistryFromSession(session *gocql.Session) ServerRegistry {
	return &CassandraServerRegistry{
		session,
	}
}

func NewCassandraServerRegistry(hosts ...string) (ServerRegistry, error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = "goblins"
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return &CassandraServerRegistry{
		session,
	}, nil
}

func (r *CassandraServerRegistry) RegisterActivity(ctx context.Context, activityId string) error {
	return r.session.Query(`INSERT INTO activities (activity_id) VALUES (?)`, activityId).WithContext(ctx).Exec()
}

func (r *CassandraServerRegistry) RegisterWorkflow(ctx context.Context, workflowId string) error {
	return r.session.Query(`INSERT INTO workflows (workflow_id) VALUES (?)`, workflowId).WithContext(ctx).Exec()
}

func (r *CassandraServerRegistry) GetAllActivities(ctx context.Context) ([]string, error) {
	scanner := r.session.Query(`SELECT activity_id FROM activities`).WithContext(ctx).Iter().Scanner()
	return r.scan(ctx, scanner)
}

func (r *CassandraServerRegistry) GetAllWorkflows(ctx context.Context) ([]string, error) {
	scanner := r.session.Query(`SELECT workflow_id FROM workflows`).WithContext(ctx).Iter().Scanner()
	return r.scan(ctx, scanner)
}

func (r *CassandraServerRegistry) scan(ctx context.Context, scanner gocql.Scanner) ([]string, error) {
	ids := []string{}
	for scanner.Next() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			var id string
			if err := scanner.Scan(&id); err != nil {
				return nil, err
			}
			ids = append(ids, id)
		}
	}

	return ids, nil
}
