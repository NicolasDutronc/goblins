package end_to_end_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/NicolasDutronc/goblins/sdk/client"
	"github.com/NicolasDutronc/goblins/sdk/workflow"
	"github.com/NicolasDutronc/goblins/server"
	"github.com/NicolasDutronc/goblins/server/eventloop"
	"github.com/NicolasDutronc/goblins/shared/event"
	"github.com/NicolasDutronc/goblins/shared/task"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/go-zookeeper/zk"
	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type EndToEndTest struct {
	suite.Suite
	cassandraContainer *dockertest.Resource
	zookeeperContainer *dockertest.Resource
	kafkaContainer     *dockertest.Resource
	pool               *dockertest.Pool
	networkId          string
	cancelFunc         context.CancelFunc
}

func (t *EndToEndTest) SetupSuite() {
	pool, err := dockertest.NewPool("")
	if err != nil {
		t.FailNow(err.Error())
	}
	t.pool = pool

	if err = pool.Client.Ping(); err != nil {
		t.FailNow(err.Error())
	}

	network, err := pool.Client.CreateNetwork(docker.CreateNetworkOptions{Name: "goblins_network"})
	if err != nil {
		t.FailNow(err.Error())
	}
	t.networkId = network.ID

	zookeeperName := fmt.Sprintf("zookeeper-%s", uuid.New().String())
	zookeeperOptions := &dockertest.RunOptions{
		Repository: "confluentinc/cp-zookeeper",
		Tag:        "7.4.0",
		Name:       zookeeperName,
		Env: []string{
			"ZOOKEEPER_CLIENT_PORT=2181",
		},
		ExposedPorts: []string{"2181"},
		NetworkID:    network.ID,
	}

	kafkaOptions := &dockertest.RunOptions{
		Repository: "confluentinc/cp-kafka",
		Tag:        "7.4.0",
		Name:       fmt.Sprintf("kafka-%s", uuid.New().String()),
		Env: []string{
			fmt.Sprintf("KAFKA_ZOOKEEPER_CONNECT=%s:2181", zookeeperName),
			"KAFKA_ADVERTISED_LISTENERS=INSIDE://kafka:9093,OUTSIDE://localhost:9092",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INSIDE:PLAINTEXT,OUTSIDE:PLAINTEXT",
			"KAFKA_INTER_BROKER_LISTENER_NAME=INSIDE",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
		},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9092/tcp": {{HostIP: "localhost", HostPort: "9092/tcp"}},
		},
		ExposedPorts: []string{"9092/tcp"},
		NetworkID:    network.ID,
	}

	wg := &sync.WaitGroup{}

	// start cassandra in parallel
	wg.Add(1)
	go t.setupCassandra(pool, wg, t.networkId)

	// start zookeeper
	zookeeper, err := pool.RunWithOptions(zookeeperOptions)
	if err != nil {
		t.FailNow(err.Error(), "could not run zookeeper")
	}
	zkPort := zookeeper.GetPort("2181/tcp")
	conn, _, err := zk.Connect([]string{fmt.Sprintf("localhost:%s", zkPort)}, 10*time.Second)
	if err != nil {
		t.FailNow(err.Error(), "could not connect to zookeeper")
	}
	defer conn.Close()
	if err := pool.Retry(func() error {
		switch conn.State() {
		case zk.StateHasSession, zk.StateConnected:
			return nil
		default:
			return fmt.Errorf("not yet connected to zookeeper")
		}
	}); err != nil {
		t.FailNow(err.Error(), "could not start zookeeper")
	}
	log.Println("zookeeper is ready")
	t.zookeeperContainer = zookeeper

	// start kafka
	kafkaContainer, err := pool.RunWithOptions(kafkaOptions)
	if err != nil {
		t.FailNow(err.Error(), "could not run kafka")
	}
	kafkaPort := kafkaContainer.GetPort("9092/tcp")
	var client *kafka.AdminClient
	if err := pool.Retry(func() error {
		client, err = kafka.NewAdminClient(&kafka.ConfigMap{
			"bootstrap.servers": fmt.Sprintf("localhost:%s", kafkaPort),
		})
		if err != nil {
			return err
		}
		if _, err := client.GetMetadata(nil, true, 10_000); err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.FailNow(err.Error(), "could not start kafka")
	}
	defer client.Close()
	log.Println("kafka is ready")
	t.kafkaContainer = kafkaContainer

	// setup kafka
	results, err := client.CreateTopics(context.Background(), []kafka.TopicSpecification{
		{
			Topic:         "tasks",
			NumPartitions: 10,
		},
		{
			Topic:         "events",
			NumPartitions: 10,
		},
	})
	if err != nil {
		t.Fail(err.Error(), "could not create partitions")
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError {
			t.Fail(result.Error.Error(), fmt.Sprintf("could not create %s topic", result.Topic))
		}
	}
	log.Println("kafka is setup")

	// wait for cassandra
	wg.Wait()
}

func (t *EndToEndTest) setupCassandra(pool *dockertest.Pool, wg *sync.WaitGroup, networkId string) {
	pwd, err := os.Getwd()
	if err != nil {
		t.FailNow(err.Error())
	}

	cassandraOptions := &dockertest.RunOptions{
		Repository:   "cassandra",
		Tag:          "4",
		Name:         fmt.Sprintf("cassandra-%s", uuid.New().String()),
		ExposedPorts: []string{"9042"},
		Mounts: []string{
			fmt.Sprintf("%s/../cassandra_config/cassandra.yaml:/etc/cassandra/cassandra.yaml", pwd),
			fmt.Sprintf("%s/../cassandra_config/cassandra-env.sh:/etc/cassadra/cassandra-env.sh", pwd),
		},
		NetworkID: networkId,
	}
	cassandra, err := pool.RunWithOptions(cassandraOptions)
	if err != nil {
		t.FailNow(err.Error(), "could not run cassandra")
	}
	var cassandraAdminSession *gocql.Session
	cassandraPort, _ := strconv.Atoi(cassandra.GetPort("9042/tcp"))
	cassandraUrl := fmt.Sprintf("localhost:%s", cassandra.GetPort("9042/tcp"))
	var cluster *gocql.ClusterConfig
	if err := pool.Retry(func() error {
		cluster = gocql.NewCluster(cassandraUrl)
		cluster.ConnectTimeout = time.Second * 10
		cluster.ProtoVersion = 4
		cluster.Port = cassandraPort
		cassandraAdminSession, err = cluster.CreateSession()
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.FailNow(err.Error(), "could not start cassandra")
	}
	defer cassandraAdminSession.Close()
	log.Println("cassandra is ready")
	t.cassandraContainer = cassandra

	// setup cassandra
	dropKeyspace := cassandraAdminSession.Query("DROP KEYSPACE IF EXISTS goblins")
	createKeyspace := cassandraAdminSession.Query("CREATE KEYSPACE goblins WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};")

	dropEventsTable := cassandraAdminSession.Query("DROP TABLE IF EXISTS goblins.events")
	createEventsTable := cassandraAdminSession.Query(`CREATE TABLE goblins.events (
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

	dropActivitiesTable := cassandraAdminSession.Query("DROP TABLE IF EXISTS goblins.activities")
	createActivitiesTable := cassandraAdminSession.Query(`CREATE TABLE goblins.activities (
		activity_id text PRIMARY KEY
	)`)

	dropWorkflowsTable := cassandraAdminSession.Query("DROP TABLE IF EXISTS goblins.workflows")
	createWorkflowsTable := cassandraAdminSession.Query(`CREATE TABLE goblins.workflows (
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
		t.Fail(err.Error(), "could not create keyspace")
	}
	if err := createEventsTable.Exec(); err != nil {
		t.Fail(err.Error(), "could not create events table")
	}
	if err := createActivitiesTable.Exec(); err != nil {
		t.Fail(err.Error(), "could not create activities table")
	}
	if err := createWorkflowsTable.Exec(); err != nil {
		t.Fail(err.Error(), "could not create workflows table")
	}
	log.Println("cassandra is setup")
	wg.Done()
}

func (t *EndToEndTest) TearDownSuite() {
	cassandraName := t.cassandraContainer.Container.Name
	kafkaName := t.kafkaContainer.Container.Name
	zookeeperName := t.zookeeperContainer.Container.Name
	t.cassandraContainer.Close()
	t.kafkaContainer.Close()
	t.zookeeperContainer.Close()
	t.pool.RemoveContainerByName(cassandraName)
	t.pool.RemoveContainerByName(zookeeperName)
	t.pool.RemoveContainerByName(kafkaName)
	t.pool.Client.RemoveNetwork(t.networkId)
}

func (t *EndToEndTest) SetupTest() {
	cassandraUrl := fmt.Sprintf("127.0.0.1:%s", t.cassandraContainer.GetPort("9042/tcp"))
	cluster := gocql.NewCluster(cassandraUrl)
	cluster.Keyspace = "goblins"
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fail(err.Error())
	}

	kafkaUrl := fmt.Sprintf("127.0.0.1:%s", t.kafkaContainer.GetPort("9092/tcp"))
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaUrl,
	})
	if err != nil {
		t.Fail(err.Error())
	}
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaUrl,
		"group.id":          "goblins_servers",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		t.Fail(err.Error())
	}
	serverRegistry := server.NewCassandraServerRegistryFromSession(session)
	eventDispatcher := event.NewKafkaEventDispatcher(producer)
	taskDispatcher := task.NewKafkaTaskDispatcher(producer)
	eventLoop := eventloop.NewKafkaEventLoop(consumer)
	eventRepository := event.NewCassandraEventRepositoryFromSession(session)
	loggingEventRepository := event.NewLoggingRepository(eventRepository)
	srv := server.NewGoblinsServer(
		serverRegistry,
		eventDispatcher,
		taskDispatcher,
		eventLoop,
		loggingEventRepository,
		producer,
	)
	loggingSrv := server.NewLoggingServer(srv)

	ctx, cancel := context.WithCancel(context.Background())
	t.cancelFunc = func() {
		cancel()
		session.Close()
	}

	// starting the server on port 9 3/4
	go func() {
		if err := server.StartServer(ctx, loggingSrv, 9075); err != nil {
			t.FailNow(err.Error(), "server stopped")
		}
	}()

	// start a worker
	go func() {
		if err := RunWorker(ctx, fmt.Sprintf("127.0.0.1:%s", t.kafkaContainer.GetPort("9092/tcp")), "localhost:9075"); err != nil {
			t.FailNow(err.Error(), "worker stopped")
		}
	}()
}

func (t *EndToEndTest) TearDownTest() {
	t.cancelFunc()
}

func (t *EndToEndTest) TestShouldExecuteWorkflow() {
	ctx := context.Background()
	client, err := client.Dial("localhost:9075")
	if err != nil {
		t.FailNow(err.Error(), "could not connect to server")
	}

	workflowRunId := uuid.New()
	future := workflow.ExecuteWorkflow[*GreetWorkflowInput, GreetWorkflowOutput](ctx, client, "greet", workflowRunId.String(), &GreetWorkflowInput{Name: "Green Got"})
	result, workflowErr, err := future.Get(ctx, 10*time.Second)

	assert.Nil(t.T(), workflowErr)
	assert.Nil(t.T(), err)
	assert.Equal(t.T(), "Hello Green Got", result.Result)
}

func TestEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end test")
	}
	suite.Run(t, new(EndToEndTest))
}
