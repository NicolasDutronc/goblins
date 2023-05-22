package main

import "github.com/gocql/gocql"

func main() {
	cluster := gocql.NewCluster("localhost:9042")
	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}

	dropKeyspace := session.Query("DROP KEYSPACE IF EXISTS goblins")
	createKeyspace := session.Query("CREATE KEYSPACE goblins WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};")

	dropEventsTable := session.Query("DROP TABLE IF EXISTS goblins.events")
	createEventsTable := session.Query(`CREATE TABLE goblins.events (
		event_type tinyint,
		emitted_instant timestamp,
		workflow_id text,
		workflow_run_id text,
		activity_id text,
		activity_run_id text,
		activity_max_retries int,
		activity_current_try int,
		error text,
		input blob,
		output blob,
		PRIMARY KEY ((workflow_run_id, activity_run_id), emitted_instant)
	) WITH CLUSTERING ORDER BY (emitted_instant DESC)`)

	dropActivitiesTable := session.Query("DROP TABLE IF EXISTS goblins.activities")
	createActivitiesTable := session.Query(`CREATE TABLE goblins.activities (
		activity_id text PRIMARY KEY
	)`)

	dropWorkflowsTable := session.Query("DROP TABLE IF EXISTS goblins.workflows")
	createWorkflowsTable := session.Query(`CREATE TABLE goblins.workflows (
		workflow_id text PRIMARY KEY
	)`)

	if err := dropEventsTable.Exec(); err != nil {
		panic(err)
	}
	if err := dropActivitiesTable.Exec(); err != nil {
		panic(err)
	}
	if err := dropWorkflowsTable.Exec(); err != nil {
		panic(err)
	}
	if err := dropKeyspace.Exec(); err != nil {
		panic(err)
	}

	if err := createKeyspace.Exec(); err != nil {
		panic(err)
	}
	if err := createEventsTable.Exec(); err != nil {
		panic(err)
	}
	if err := createActivitiesTable.Exec(); err != nil {
		panic(err)
	}
	if err := createWorkflowsTable.Exec(); err != nil {
		panic(err)
	}
}
