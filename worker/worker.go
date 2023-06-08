package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/NicolasDutronc/goblins/shared/event"
	"github.com/NicolasDutronc/goblins/shared/goblins_service"
	"github.com/NicolasDutronc/goblins/shared/task"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type Worker struct {
	// name of the worker for debugging purposes
	id string

	// nbGoroutines is the number of concurrent goroutines that will process incoming tasks
	nbGoroutines int

	// taskQSize is the size of the task buffer
	taskQSize int

	registry        Registry
	eventDispatcher event.EventDispatcher
	consumer        *kafka.Consumer
	conn            *grpc.ClientConn
	fnExecutor      FunctionExecutor
}

func NewWorker(id string, nbGoRoutines, taskQSize int, registry Registry, eventDispatcher event.EventDispatcher, consumer *kafka.Consumer, conn *grpc.ClientConn, fnExecutor FunctionExecutor) *Worker {
	return &Worker{
		id:              id,
		nbGoroutines:    nbGoRoutines,
		taskQSize:       taskQSize,
		registry:        registry,
		eventDispatcher: eventDispatcher,
		consumer:        consumer,
		conn:            conn,
		fnExecutor:      fnExecutor,
	}
}

// Run starts the worker process
// It spawns goroutines that listen to the taskQ channel and process incoming tasks
// It starts a blocking loop that reads kafka messages and puts them in the taskQ
func (w *Worker) Run(ctx context.Context) error {
	log.Printf("worker %s starting\n", w.id)
	// register workflows and activities
	client := goblins_service.NewGoblinsServiceClient(w.conn)

	log.Printf("worker %s registering activities\n", w.id)
	activities, err := w.registry.GetAllActivities(ctx)
	if err != nil {
		return err
	}
	for _, activity := range activities {
		if _, err := client.RegisterActivity(ctx, &goblins_service.RegisterActivityRequest{
			ActivityId: activity,
		}); err != nil {
			log.Printf("activity %s could not be registered: %v\n", activity, err)
		}
	}

	log.Printf("worker %s registering workflows\n", w.id)
	workflows, err := w.registry.GetAllWorkflows(ctx)
	if err != nil {
		return err
	}
	for _, workflow := range workflows {
		if _, err := client.RegisterWorkflow(ctx, &goblins_service.RegisterWorkflowRequest{
			WorkflowId: workflow,
		}); err != nil {
			log.Printf("workflow %s could not be registered: %v\n", workflow, err)
		}
	}

	// subscribe to the task topic
	if err := w.consumer.Subscribe(task.TaskTopic, nil); err != nil {
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
					var t task.Task
					if err := proto.Unmarshal(msg.Value, &t); err != nil {
						log.Printf("could not deserialize msg: %v (%v)\n", err, msg)
						continue
					}
					if t.TaskType == task.Task_WORKFLOW_TASK {
						if err := w.HandleWorkflowTask(ctx, &t); err != nil {
							log.Printf("workflow task %v execution raised an error: %v\n", &t, err)
							if publishedErr := w.eventDispatcher.SendWorkflowFinishedInErrorEvent(t.WorkflowId, t.WorkflowRunId, t.Input, err); publishedErr != nil {
								log.Printf("failed to send workflow error event: %v", publishedErr)
							}
						}
					} else {
						if err := w.HandleActivityTask(ctx, &t); err != nil {
							log.Printf("activity task %v execution raised an error: %v\n", &t, err)
							if publishedErr := w.eventDispatcher.SendActivityFinishedInErrorEvent(t.WorkflowId, t.WorkflowRunId, t.ActivityId, t.ActivityRunId, t.ActivityMaxRetries, t.ActivityCurrentTry, t.Input, err); publishedErr != nil {
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

func (w *Worker) RegisterActivity(ctx context.Context, activityId string, activityFunction any) error {
	if err := w.fnExecutor.CheckFunction(activityFunction, fmt.Sprintf("function of activity %s", activityId)); err != nil {
		return err
	}

	return w.registry.RegisterActivity(ctx, activityId, activityFunction)
}

func (w *Worker) RegisterWorkflow(ctx context.Context, workflowId string, workflowFunction any) error {
	if err := w.fnExecutor.CheckFunction(workflowFunction, fmt.Sprintf("function of workflow %s", workflowId)); err != nil {
		return err
	}
	return w.registry.RegisterWorkflow(ctx, workflowId, workflowFunction)
}

func (w *Worker) HandleActivityTask(ctx context.Context, task *task.Task) error {
	log.Printf("worker %s starting to process activity task %v", w.id, task)
	if err := w.eventDispatcher.SendActivityStartedEvent(task.WorkflowId, task.WorkflowRunId, task.ActivityId, task.ActivityRunId, task.ActivityMaxRetries, task.ActivityCurrentTry, task.Input); err != nil {
		return err
	}

	activityFunc, err := w.registry.LookupActivity(ctx, task.ActivityId)
	if err != nil {
		return err
	}

	result, err := w.fnExecutor.ExecFunction(ctx, activityFunc, task.Input)
	if err != nil {
		return err
	}

	resultBytes, err := json.Marshal(result)
	if err != nil {
		return err
	}

	return w.eventDispatcher.SendActivityFinishedInSuccessEvent(task.WorkflowId, task.WorkflowRunId, task.ActivityId, task.ActivityRunId, task.ActivityMaxRetries, task.ActivityCurrentTry, task.Input, resultBytes)
}

func (w *Worker) HandleWorkflowTask(ctx context.Context, task *task.Task) error {
	log.Printf("worker %s starting to process workflow task %v", w.id, task)
	if err := w.eventDispatcher.SendWorkflowStartedEvent(task.WorkflowId, task.WorkflowRunId, task.Input); err != nil {
		return err
	}
	workflowFunc, err := w.registry.LookupWorkflow(ctx, task.WorkflowId)
	if err != nil {
		return err
	}

	workerContext := &workerContext{
		w.registry,
		w.conn,
		w.eventDispatcher,
	}
	workflowCtx := context.WithValue(context.WithValue(ctx, contextTaskKey, task), contextWorkerContextKey, workerContext)
	result, err := w.fnExecutor.ExecFunction(workflowCtx, workflowFunc, task.Input)
	if err != nil {
		return err
	}

	resultBytes, err := json.Marshal(result)
	if err != nil {
		return err
	}

	return w.eventDispatcher.SendWorkflowFinishedInSuccessEvent(task.WorkflowId, task.WorkflowRunId, task.Input, resultBytes)
}
