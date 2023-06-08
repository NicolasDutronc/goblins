package server

import (
	"context"

	"github.com/gocql/gocql"
)

type serverRegistry interface {
	registerActivity(ctx context.Context, activityId string) error
	registerWorkflow(ctx context.Context, workflowId string) error
	getAllActivities(ctx context.Context) ([]string, error)
	getAllWorkflows(ctx context.Context) ([]string, error)
}

type cassandraServerRegistry struct {
	session *gocql.Session
}

func newCassandraServerRegistry(hosts ...string) (serverRegistry, error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = "goblins"
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return &cassandraServerRegistry{
		session,
	}, nil
}

func (r *cassandraServerRegistry) registerActivity(ctx context.Context, activityId string) error {
	return r.session.Query(`INSERT INTO activities (activity_id) VALUES (?)`, activityId).WithContext(ctx).Exec()
}

func (r *cassandraServerRegistry) registerWorkflow(ctx context.Context, workflowId string) error {
	return r.session.Query(`INSERT INTO workflows (workflow_id) VALUES (?)`, workflowId).WithContext(ctx).Exec()
}

func (r *cassandraServerRegistry) getAllActivities(ctx context.Context) ([]string, error) {
	scanner := r.session.Query(`SELECT activity_id FROM activities`).WithContext(ctx).Iter().Scanner()
	return r.scan(ctx, scanner)
}

func (r *cassandraServerRegistry) getAllWorkflows(ctx context.Context) ([]string, error) {
	scanner := r.session.Query(`SELECT workflow_id FROM workflows`).WithContext(ctx).Iter().Scanner()
	return r.scan(ctx, scanner)
}

func (r *cassandraServerRegistry) scan(ctx context.Context, scanner gocql.Scanner) ([]string, error) {
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
