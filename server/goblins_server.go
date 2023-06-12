package server

import (
	context "context"
	"errors"
	"fmt"
	"log"
	"net"

	"github.com/NicolasDutronc/goblins/server/eventloop"
	"github.com/NicolasDutronc/goblins/shared/event"
	"github.com/NicolasDutronc/goblins/shared/goblins_service"
	"github.com/NicolasDutronc/goblins/shared/task"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	grpc "google.golang.org/grpc"
)

type GoblinsServer interface {
	goblins_service.GoblinsServiceServer
	Start(context.Context)
}

func StartServer(ctx context.Context, srv GoblinsServer, port int) error {
	srv.Start(ctx)

	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		return err
	}

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	goblins_service.RegisterGoblinsServiceServer(grpcServer, srv)

	go func() {
		// blocks until the context is cancelled
		<-ctx.Done()
		grpcServer.GracefulStop()

	}()

	log.Printf("server starts listening on port %d\n", port)
	return grpcServer.Serve(listener)
}

type goblinsServerImpl struct {
	goblins_service.UnimplementedGoblinsServiceServer
	registry        ServerRegistry
	eventDispatcher event.EventDispatcher
	taskDispatcher  task.TaskDispatcher
	eventLoop       eventloop.EventLoop
	eventRepository event.EventRepository
	producer        *kafka.Producer
}

func NewGoblinsServer(
	registry ServerRegistry,
	eventDispatcher event.EventDispatcher,
	taskDispatcher task.TaskDispatcher,
	eventLoop eventloop.EventLoop,
	eventRepository event.EventRepository,
	producer *kafka.Producer,
) GoblinsServer {
	return &goblinsServerImpl{
		goblins_service.UnimplementedGoblinsServiceServer{},
		registry,
		eventDispatcher,
		taskDispatcher,
		eventLoop,
		eventRepository,
		producer,
	}
}

func CreateGoblinsServer(kafkaBootstrapServers string, cassandraHosts ...string) (GoblinsServer, error) {
	registry, err := NewCassandraServerRegistry(cassandraHosts...)
	if err != nil {
		return nil, err
	}
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrapServers,
	})
	if err != nil {
		return nil, err
	}
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrapServers,
		"group.id":          "goblins_servers",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return nil, err
	}
	eventDispatcher := event.NewKafkaEventDispatcher(producer)
	taskDispatcher := task.NewKafkaTaskDispatcher(producer)
	eventLoop := eventloop.NewKafkaEventLoop(consumer)
	eventRepository, err := event.NewCassandraEventRepository(cassandraHosts...)
	if err != nil {
		return nil, err
	}

	return NewGoblinsServer(registry, eventDispatcher, taskDispatcher, eventLoop, eventRepository, producer), nil
}

func (s *goblinsServerImpl) Start(ctx context.Context) {
	// delivery report handler for produced messages
	go func() {
		for e := range s.producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("delivery failed: %v\n", ev.TopicPartition)
				} else {
					log.Printf("delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// register handlers and start the event loop
	s.eventLoop.RegisterSystemHandler(eventloop.NewEventWriter(s.eventRepository))
	s.eventLoop.RegisterSystemHandler(eventloop.NewActivityRetryer(s.eventDispatcher, s.taskDispatcher, s.eventRepository))
	s.eventLoop.RegisterSystemHandler(eventloop.EventHandlerFunc(func(ctx context.Context, event *event.WorkflowEvent) {
		log.Printf("EVENT LOOP - event processed: %v\n", event)
	}))
	go s.eventLoop.Run(ctx)
}

func (s *goblinsServerImpl) RegisterActivity(ctx context.Context, req *goblins_service.RegisterActivityRequest) (*goblins_service.RegisterActivityResponse, error) {
	if err := s.registry.RegisterActivity(ctx, req.ActivityId); err != nil {
		return nil, err
	}
	return &goblins_service.RegisterActivityResponse{}, nil
}

func (s *goblinsServerImpl) RegisterWorkflow(ctx context.Context, req *goblins_service.RegisterWorkflowRequest) (*goblins_service.RegisterWorkflowResponse, error) {
	if err := s.registry.RegisterWorkflow(ctx, req.WorkflowId); err != nil {
		return nil, err
	}
	return &goblins_service.RegisterWorkflowResponse{}, nil
}

func (s *goblinsServerImpl) ScheduleActivity(ctx context.Context, req *goblins_service.ScheduleActivityRequest) (*goblins_service.ScheduleActivityResponse, error) {
	if err := s.taskDispatcher.SendActivityTask(req.WorkflowId, req.WorkflowRunId, req.ActivityId, req.ActivityRunId, req.ActivityMaxRetries, req.ActivityCurrentTry, req.Input); err != nil {
		return nil, err
	}

	if err := s.eventDispatcher.SendActivityScheduledEvent(req.WorkflowId, req.WorkflowRunId, req.ActivityId, req.ActivityRunId, req.ActivityMaxRetries, req.ActivityCurrentTry, req.Input); err != nil {
		return nil, err
	}

	return &goblins_service.ScheduleActivityResponse{}, nil
}

func (s *goblinsServerImpl) ScheduleWorkflow(ctx context.Context, req *goblins_service.ScheduleWorkflowRequest) (*goblins_service.ScheduleWorkflowResponse, error) {
	if err := s.taskDispatcher.SendWorkflowTask(req.WorkflowId, req.WorkflowRunId, req.Input); err != nil {
		return nil, err
	}

	if err := s.eventDispatcher.SendWorkflowScheduledEvent(req.WorkflowId, req.WorkflowRunId, req.Input); err != nil {
		return nil, err
	}

	return &goblins_service.ScheduleWorkflowResponse{}, nil
}

func (s *goblinsServerImpl) GetActivityResult(ctx context.Context, req *goblins_service.GetActivityResultRequest) (*goblins_service.GetActivityResultResponse, error) {
	resultChan := make(chan *event.WorkflowEvent, 1)

	// register a hook to get the termination event of the activity
	unregister := s.eventLoop.RegisterSystemHandler(
		eventloop.EventHandlerFunc(func(ctx context.Context, we *event.WorkflowEvent) {
			if we.EventType == event.WorkflowEvent_ACTIVITY_FINISHED_IN_SUCCESS ||
				we.EventType == event.WorkflowEvent_ACTIVITY_FINISHED_IN_ERROR &&
					we.ActivityMaxRetries == we.ActivityCurrentTry {
				resultChan <- we
			}
		}))
	defer unregister()

	// the event might have already been processed so we search for it in the database
	// whatever returns first between the database query and the event callback is returned
	fetchContext, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-fetchContext.Done():
			return
		default:
			events, _ := s.eventRepository.GetActivityRunEvents(fetchContext, req.WorkflowRunId, req.ActivityRunId)
			for _, activityEvent := range events {
				if activityEvent.EventType == event.WorkflowEvent_ACTIVITY_FINISHED_IN_SUCCESS ||
					activityEvent.EventType == event.WorkflowEvent_ACTIVITY_FINISHED_IN_ERROR &&
						activityEvent.ActivityMaxRetries == activityEvent.ActivityCurrentTry {
					resultChan <- activityEvent
					return
				}
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			close(resultChan)
			cancel()
			return &goblins_service.GetActivityResultResponse{
				Error: errors.Join(fmt.Errorf("context cancelled for GetActivityResult %v", req), ctx.Err()).Error(),
			}, nil
		case event := <-resultChan:
			cancel()
			if event.Error != nil {
				return &goblins_service.GetActivityResultResponse{
					Error: *event.Error,
				}, nil
			}
			return &goblins_service.GetActivityResultResponse{
				Output: event.Output,
			}, nil
		}
	}
}

func (s *goblinsServerImpl) GetWorkflowResult(ctx context.Context, req *goblins_service.GetWorkflowResultRequest) (*goblins_service.GetWorkflowResultResponse, error) {
	resultChan := make(chan *event.WorkflowEvent, 1)

	// register a hook to get the termination event of the workflow
	unregister := s.eventLoop.RegisterSystemHandler(
		eventloop.EventHandlerFunc(func(ctx context.Context, we *event.WorkflowEvent) {
			if we.EventType == event.WorkflowEvent_WORKFLOW_FINISHED_IN_SUCCESS ||
				we.EventType == event.WorkflowEvent_WORKFLOW_FINISHED_IN_ERROR {
				resultChan <- we
			}
		}))
	defer unregister()

	// the event might have already been processed so we search for it in the database
	// whatever returns first between the database query and the event callback is returned
	fetchContext, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-fetchContext.Done():
			return
		default:
			events, _ := s.eventRepository.GetWorkflowRunEvents(ctx, req.WorkflowRunId)
			for _, workflowEvent := range events {
				if workflowEvent.EventType == event.WorkflowEvent_WORKFLOW_FINISHED_IN_SUCCESS ||
					workflowEvent.EventType == event.WorkflowEvent_WORKFLOW_FINISHED_IN_ERROR {
					resultChan <- workflowEvent
					return
				}
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			close(resultChan)
			cancel()
			return &goblins_service.GetWorkflowResultResponse{
				Error: errors.Join(fmt.Errorf("context cancelled for GetWorkflowResult %v", req), ctx.Err()).Error(),
			}, nil
		case event := <-resultChan:
			cancel()
			if event.Error != nil {
				return &goblins_service.GetWorkflowResultResponse{
					Error: *event.Error,
				}, nil
			}
			return &goblins_service.GetWorkflowResultResponse{
				Output: event.Output,
			}, nil
		}
	}
}

func (s *goblinsServerImpl) GetActivityRunHistory(ctx context.Context, req *goblins_service.GetActivityRunHistoryRequest) (*goblins_service.GetActivityRunHistoryResponse, error) {
	eventList, err := s.eventRepository.GetActivityRunEvents(ctx, req.WorkflowRunId, req.ActivityRunId)
	if err != nil {
		return nil, err
	}

	return &goblins_service.GetActivityRunHistoryResponse{
		EventList: eventList,
	}, nil
}

func (s *goblinsServerImpl) GetWorkflowRunHistory(ctx context.Context, req *goblins_service.GetWorkflowRunHistoryRequest) (*goblins_service.GetWorkflowRunHistoryResponse, error) {
	eventList, err := s.eventRepository.GetWorkflowRunEvents(ctx, req.WorkflowRunId)
	if err != nil {
		return nil, err
	}
	return &goblins_service.GetWorkflowRunHistoryResponse{
		EventList: eventList,
	}, nil
}
