package goblins

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	reflect "reflect"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type worker struct {
	// name of the worker for debugging purposes
	id string

	// nbGoroutines is the number of concurrent goroutines that will process incoming tasks
	nbGoroutines int

	// taskQSize is the size of the task buffer
	taskQSize int

	registry        registry
	eventDispatcher EventDispatcher
	consumer        *kafka.Consumer
	conn            *grpc.ClientConn
}

// NewWorker creates a new worker
// TODO: instead of having to pass kafka bootstrap servers, it would be better to ask the server for any configuration needed
func NewWorker(id string, registry registry, parallelism int, bufferSize int, kafkaBootstrapServers string, serverUrl string) (*worker, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrapServers,
	})
	if err != nil {
		return nil, err
	}

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrapServers,
		"group.id":          "worker_group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return nil, err
	}

	conn, err := grpc.Dial(serverUrl, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &worker{
		id:           id,
		nbGoroutines: parallelism,
		registry:     registry,
		taskQSize:    bufferSize,
		eventDispatcher: &KafkaEventDispatcher{
			producer,
		},
		consumer: consumer,
		conn:     conn,
	}, nil
}

// Run starts the worker process
// It spawns goroutines that listen to the taskQ channel and process incoming tasks
// It starts a blocking loop that reads kafka messages and puts them in the taskQ
func (w *worker) Run(ctx context.Context) error {
	log.Printf("worker %s starting\n", w.id)
	// register workflows and activities
	client := NewGoblinsServiceClient(w.conn)

	log.Printf("worker %s registering activities\n", w.id)
	activities, err := w.registry.getAllActivities(ctx)
	if err != nil {
		return err
	}
	for _, activity := range activities {
		if _, err := client.RegisterActivity(ctx, &RegisterActivityRequest{
			ActivityId: activity,
		}); err != nil {
			log.Printf("activity %s could not be registered: %v\n", activity, err)
		}
	}

	log.Printf("worker %s registering workflows\n", w.id)
	workflows, err := w.registry.getAllWorkflows(ctx)
	if err != nil {
		return err
	}
	for _, workflow := range workflows {
		if _, err := client.RegisterWorkflow(ctx, &RegisterWorkflowRequest{
			WorkflowId: workflow,
		}); err != nil {
			log.Printf("workflow %s could not be registered: %v\n", workflow, err)
		}
	}

	// subscribe to the task topic
	if err := w.consumer.Subscribe(tasksTopic, nil); err != nil {
		return errors.Join(fmt.Errorf("worker could not subscribe to tasks topic"), err)
	}

	taskQ := make(chan *kafka.Message, w.taskQSize)
	wg := sync.WaitGroup{}
	wg.Add(w.nbGoroutines)

	// start goroutines
	for i := 0; i < w.nbGoroutines; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					log.Println("stopping goroutine")
					wg.Done()
					return

				case msg := <-taskQ:
					var task Task
					if err := proto.Unmarshal(msg.Value, &task); err != nil {
						log.Printf("could not deserialize msg: %v (%v)\n", err, msg)
						continue
					}
					if task.TaskType == Task_WORKFLOW_TASK {
						if err := w.handleWorkflowTask(ctx, &task); err != nil {
							log.Printf("workflow task %v execution raised an error: %v\n", &task, err)
							if publishedErr := w.eventDispatcher.SendWorkflowFinishedInErrorEvent(task.WorkflowId, task.WorkflowRunId, task.Input, err); publishedErr != nil {
								log.Printf("failed to send workflow error event: %v", publishedErr)
							}
						}
					} else {
						if err := w.handleActivityTask(ctx, &task); err != nil {
							log.Printf("activity task %v execution raised an error: %v\n", &task, err)
							if publishedErr := w.eventDispatcher.SendActivityFinishedInErrorEvent(task.WorkflowId, task.WorkflowRunId, task.ActivityId, task.ActivityRunId, task.ActivityMaxRetries, task.ActivityCurrentTry, task.Input, err); publishedErr != nil {
								log.Printf("failed to send activity error event")
							}
						}
					}
				}
			}
		}()
	}

	// start consumer loop
	log.Printf("worker %s listening to kafka\n", w.id)
	for {
		select {
		case <-ctx.Done():
			log.Println("stopping consumer loop")
			log.Println("waiting for goroutines to finish")
			wg.Wait()
			return nil
		default:
			msg, err := w.consumer.ReadMessage(10 * time.Millisecond)
			if err != nil {
				// no new message
				if err.(kafka.Error).IsTimeout() {
					continue
				}

				log.Printf("consumer could not read messages: %v\n", err)
				continue
			}

			log.Printf("received task message: %v\n", msg)
			taskQ <- msg

			if _, err := w.consumer.CommitMessage(msg); err != nil {
				log.Println("error committing msg")
			}
		}
	}

}

func (w *worker) checkFunction(f any, fName string) error {
	if f == nil {
		return fmt.Errorf("%s cannot be null", fName)
	}
	fValue := reflect.ValueOf(f)
	if fValue.Kind() != reflect.Func {
		return fmt.Errorf("%s must be a function", fName)
	}
	if fValue.Type().NumIn() != 2 {
		return fmt.Errorf("%s must have exactly 2 arguments", fName)
	}
	arg1 := fValue.Type().In(0)
	if !arg1.Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) {
		return fmt.Errorf("first argument of %s must be a context.Context", fName)
	}
	if fValue.Type().NumOut() != 2 {
		return fmt.Errorf("%s must return 2 values", fName)
	}
	res2 := fValue.Type().Out(1)
	if !res2.Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		return fmt.Errorf("second return value of %s must be an error", fName)
	}

	return nil
}

func (w *worker) RegisterActivity(ctx context.Context, activityId string, activityFunction any) error {
	if err := w.checkFunction(activityFunction, fmt.Sprintf("function of activity %s", activityId)); err != nil {
		return err
	}

	return w.registry.registerActivity(ctx, activityId, activityFunction)
}

func (w *worker) RegisterWorkflow(ctx context.Context, workflowId string, workflowFunction any) error {
	if err := w.checkFunction(workflowFunction, fmt.Sprintf("function of workflow %s", workflowId)); err != nil {
		return err
	}
	return w.registry.registerWorkflow(ctx, workflowId, workflowFunction)
}

// execFunction handles the reflection stuff to run the activity or workflow function
func (w *worker) execFunction(ctx context.Context, f any, inputBytes []byte) (any, error) {
	fn := reflect.ValueOf(f)
	var deserialized map[string]any
	if err := json.Unmarshal(inputBytes, &deserialized); err != nil {
		return nil, err
	}

	argType := fn.Type().In(1).Elem()
	input := reflect.New(argType)
	for i := 0; i < argType.NumField(); i++ {
		input.Elem().Field(i).Set(reflect.ValueOf(deserialized[argType.Field(i).Name]))
	}

	res := fn.Call([]reflect.Value{reflect.ValueOf(ctx), input})

	var err error
	if errResult := res[1].Interface(); errResult != nil {
		err = errResult.(error)
	}
	return res[0].Interface(), err
}

func (w *worker) handleActivityTask(ctx context.Context, task *Task) error {
	log.Printf("worker %s starting to process activity task %v", w.id, task)
	if err := w.eventDispatcher.SendActivityStartedEvent(task.WorkflowId, task.WorkflowRunId, task.ActivityId, task.ActivityRunId, task.ActivityMaxRetries, task.ActivityCurrentTry, task.Input); err != nil {
		return err
	}

	activityFunc, err := w.registry.LookupActivity(ctx, task.ActivityId)
	if err != nil {
		return err
	}

	result, err := w.execFunction(ctx, activityFunc, task.Input)
	if err != nil {
		return err
	}

	resultBytes, err := json.Marshal(result)
	if err != nil {
		return err
	}

	return w.eventDispatcher.SendActivityFinishedInSuccessEvent(task.WorkflowId, task.WorkflowRunId, task.ActivityId, task.ActivityRunId, task.ActivityMaxRetries, task.ActivityCurrentTry, task.Input, resultBytes)
}

func (w *worker) handleWorkflowTask(ctx context.Context, task *Task) error {
	log.Printf("worker %s starting to process workflow task %v", w.id, task)
	if err := w.eventDispatcher.SendWorkflowStartedEvent(task.WorkflowId, task.WorkflowRunId, task.Input); err != nil {
		return err
	}
	workflowFunc, err := w.registry.lookupWorkflow(ctx, task.WorkflowId)
	if err != nil {
		return err
	}

	workerContext := &workerContext{
		w.registry,
		w.conn,
		w.eventDispatcher,
	}
	workflowCtx := context.WithValue(context.WithValue(ctx, contextTaskKey, task), contextWorkerContextKey, workerContext)
	result, err := w.execFunction(workflowCtx, workflowFunc, task.Input)
	if err != nil {
		return err
	}

	resultBytes, err := json.Marshal(result)
	if err != nil {
		return err
	}

	return w.eventDispatcher.SendWorkflowFinishedInSuccessEvent(task.WorkflowId, task.WorkflowRunId, task.Input, resultBytes)
}
