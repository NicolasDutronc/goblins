package event_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/NicolasDutronc/goblins/shared/event"
	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type CassandraEventRepositoryTestSuite struct {
	suite.Suite
	container  *dockertest.Resource
	appSession *gocql.Session
	repository event.EventRepository
	pool       *dockertest.Pool
}

func (s *CassandraEventRepositoryTestSuite) SetupSuite() {
	pool, err := dockertest.NewPool("")
	if err != nil {
		s.FailNow(err.Error())
	}
	s.pool = pool

	pwd, err := os.Getwd()
	if err != nil {
		s.FailNow(err.Error())
	}

	options := &dockertest.RunOptions{
		Repository: "cassandra",
		Tag:        "4",
		Name:       fmt.Sprintf("cassandra-%s", uuid.New().String()),
		Mounts: []string{
			fmt.Sprintf("%s/../../cassandra_config/cassandra.yaml:/etc/cassandra/cassandra.yaml", pwd),
			fmt.Sprintf("%s/../../cassandra_config/cassandra-env.sh:/etc/cassadra/cassandra-env.sh", pwd),
		},
	}

	resource, err := pool.RunWithOptions(options)
	if err != nil {
		s.FailNow(err.Error())
	}

	var adminSession *gocql.Session
	port, _ := strconv.Atoi(resource.GetPort("9042/tcp"))
	cassandraUrl := fmt.Sprintf("localhost:%s", resource.GetPort("9042/tcp"))
	var cluster *gocql.ClusterConfig
	if err := pool.Retry(func() error {
		cluster = gocql.NewCluster(cassandraUrl)
		cluster.ConnectTimeout = time.Second * 10
		cluster.ProtoVersion = 4
		cluster.Port = port
		adminSession, err = cluster.CreateSession()
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		s.Fail(err.Error())
	}
	defer adminSession.Close()
	log.Println("cassandra is ready")
	s.container = resource

	createKeyspace := adminSession.Query("CREATE KEYSPACE goblins WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};")
	createEventsTable := adminSession.Query(`CREATE TABLE goblins.events (
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
		PRIMARY KEY ((workflow_run_id), activity_run_id, emitted_instant)
	) WITH CLUSTERING ORDER BY (activity_run_id ASC, emitted_instant DESC)`)

	if err := createKeyspace.Exec(); err != nil {
		s.Fail(err.Error())
	}
	if err := createEventsTable.Exec(); err != nil {
		s.Fail(err.Error())
	}

	cluster.Keyspace = "goblins"
	s.appSession, err = cluster.CreateSession()
	if err != nil {
		s.Fail(err.Error())
	}
	s.repository = event.NewCassandraEventRepositoryFromSession(s.appSession)
}

func (s *CassandraEventRepositoryTestSuite) TearDownSuite() {
	s.appSession.Close()
	containerName := s.container.Container.Name
	s.container.Close()
	s.pool.RemoveContainerByName(containerName)
}

func (s *CassandraEventRepositoryTestSuite) TestStoreAndGetEvents() {
	ctx := context.Background()
	workflowEvent := &event.WorkflowEvent{
		EventType:     event.WorkflowEvent_WORKFLOW_STARTED,
		EmittedAt:     timestamppb.New(event.NewWorkflowEventTimestamp()),
		WorkflowId:    "workflow",
		WorkflowRunId: "workflow-run",
		Input:         []byte("input"),
	}
	activityEvent := &event.WorkflowEvent{
		EventType:          event.WorkflowEvent_ACTIVITY_FINISHED_IN_SUCCESS,
		EmittedAt:          timestamppb.New(event.NewWorkflowEventTimestamp()),
		WorkflowId:         "workflow",
		WorkflowRunId:      "workflow-run",
		ActivityId:         "activity",
		ActivityRunId:      "activity-run",
		ActivityMaxRetries: 5,
		ActivityCurrentTry: 3,
		Input:              []byte("input"),
		Output:             []byte("result"),
	}

	storeWorkflowEventErr := s.repository.StoreEvent(ctx, workflowEvent)
	storeActivityEventErr := s.repository.StoreEvent(ctx, activityEvent)
	workflowEvents, getWorkflowEventsErr := s.repository.GetWorkflowRunEvents(ctx, "workflow-run")
	activityEvents, getActivityEventsErr := s.repository.GetActivityRunEvents(ctx, "workflow-run", "activity-run")

	assert.Nil(s.T(), storeWorkflowEventErr)
	assert.Nil(s.T(), storeActivityEventErr)
	assert.Nil(s.T(), getWorkflowEventsErr)
	assert.Nil(s.T(), getActivityEventsErr)
	require.Len(s.T(), workflowEvents, 2)
	assert.Equal(s.T(), workflowEvent, workflowEvents[0])
	assert.Equal(s.T(), activityEvent, workflowEvents[1])
	require.Len(s.T(), activityEvents, 1)
	assert.Equal(s.T(), activityEvent, activityEvents[0])
}

func TestCassandraEventRepositoryIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(CassandraEventRepositoryTestSuite))
}
