package goblins

import (
	context "context"
	"errors"
	"fmt"
	"log"
	"net"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	grpc "google.golang.org/grpc"
)

type goblinsServer interface {
	GoblinsServiceServer
	start(context.Context)
}

func StartServer(ctx context.Context, srv goblinsServer, port int) error {
	srv.start(ctx)

	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		return err
	}

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	RegisterGoblinsServiceServer(grpcServer, srv)
	log.Printf("server starts listening on port %d\n", port)
	return grpcServer.Serve(listener)
}

type goblinsServerImpl struct {
	UnimplementedGoblinsServiceServer
	registry        serverRegistry
	eventDispatcher EventDispatcher
	taskDispatcher  taskDispatcher
	eventLoop       *eventLoop
	eventRepository eventRepository
	producer        *kafka.Producer
}

func NewGoblinsServer(kafkaBootstrapServers string, cassandraHosts ...string) (goblinsServer, error) {
	registry, err := newCassandraServerRegistry(cassandraHosts...)
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
	eventDispatcher := newKafkaEventDispatcher(producer)
	taskDispatcher := newKafkaTaskDispatcher(producer)
	eventLoop := newEventLoop(consumer)
	eventRepository, err := newCassandraEventRepository(cassandraHosts...)
	if err != nil {
		return nil, err
	}

	return &goblinsServerImpl{
		UnimplementedGoblinsServiceServer{},
		registry,
		eventDispatcher,
		taskDispatcher,
		eventLoop,
		&monitoringRepository{eventRepository: eventRepository},
		producer,
	}, nil
}

func (s *goblinsServerImpl) start(ctx context.Context) {
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
	s.eventLoop.registerSystemHandler(&eventPusher{eventRepository: s.eventRepository})
	s.eventLoop.registerSystemHandler(&activityRetryer{
		s.eventDispatcher,
		s.taskDispatcher,
		s.eventRepository,
	})
	s.eventLoop.registerSystemHandler(eventHandlerFunc(func(ctx context.Context, event *WorkflowEvent) {
		log.Printf("EVENT LOOP - event processed: %v\n", event)
	}))
	go s.eventLoop.run(ctx)
}

func (s *goblinsServerImpl) RegisterActivity(ctx context.Context, req *RegisterActivityRequest) (*RegisterActivityResponse, error) {
	if err := s.registry.registerActivity(ctx, req.ActivityId); err != nil {
		return nil, err
	}
	return &RegisterActivityResponse{}, nil
}

func (s *goblinsServerImpl) RegisterWorkflow(ctx context.Context, req *RegisterWorkflowRequest) (*RegisterWorkflowResponse, error) {
	if err := s.registry.registerWorkflow(ctx, req.WorkflowId); err != nil {
		return nil, err
	}
	return &RegisterWorkflowResponse{}, nil
}

func (s *goblinsServerImpl) ScheduleActivity(ctx context.Context, req *ScheduleActivityRequest) (*ScheduleActivityResponse, error) {
	if err := s.taskDispatcher.sendActivityTask(req.WorkflowId, req.WorkflowRunId, req.ActivityId, req.ActivityRunId, req.ActivityMaxRetries, req.ActivityCurrentTry, req.Input); err != nil {
		return nil, err
	}

	if err := s.eventDispatcher.SendActivityScheduledEvent(req.WorkflowId, req.WorkflowRunId, req.ActivityId, req.ActivityRunId, req.ActivityMaxRetries, req.ActivityCurrentTry, req.Input); err != nil {
		return nil, err
	}

	return &ScheduleActivityResponse{}, nil
}

func (s *goblinsServerImpl) ScheduleWorkflow(ctx context.Context, req *ScheduleWorkflowRequest) (*ScheduleWorkflowResponse, error) {
	if err := s.taskDispatcher.sendWorkflowTask(req.WorkflowId, req.WorkflowRunId, req.Input); err != nil {
		return nil, err
	}

	if err := s.eventDispatcher.SendWorkflowScheduledEvent(req.WorkflowId, req.WorkflowRunId, req.Input); err != nil {
		return nil, err
	}

	return &ScheduleWorkflowResponse{}, nil
}

func (s *goblinsServerImpl) GetActivityResult(ctx context.Context, req *GetActivityResultRequest) (*GetActivityResultResponse, error) {
	resultChan := make(chan *WorkflowEvent, 1)

	// register a hook to get the termination event of the activity
	unregister := s.eventLoop.registerSystemHandler(
		eventHandlerFunc(func(ctx context.Context, we *WorkflowEvent) {
			if we.EventType == WorkflowEvent_ACTIVITY_FINISHED_IN_SUCCESS ||
				we.EventType == WorkflowEvent_ACTIVITY_FINISHED_IN_ERROR &&
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
			events, _ := s.eventRepository.getActivityRunEvents(fetchContext, req.ActivityRunId)
			for _, event := range events {
				if event.EventType == WorkflowEvent_ACTIVITY_FINISHED_IN_SUCCESS ||
					event.EventType == WorkflowEvent_ACTIVITY_FINISHED_IN_ERROR &&
						event.ActivityMaxRetries == event.ActivityCurrentTry {
					resultChan <- event
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
			return &GetActivityResultResponse{
				Error: errors.Join(fmt.Errorf("context cancelled for GetActivityResult %v", req), ctx.Err()).Error(),
			}, nil
		case event := <-resultChan:
			cancel()
			if event.Error != nil {
				return &GetActivityResultResponse{
					Error: *event.Error,
				}, nil
			}
			return &GetActivityResultResponse{
				Output: event.Output,
			}, nil
		}
	}
}

func (s *goblinsServerImpl) GetWorkflowResult(ctx context.Context, req *GetWorkflowResultRequest) (*GetWorkflowResultResponse, error) {
	resultChan := make(chan *WorkflowEvent, 1)

	// register a hook to get the termination event of the workflow
	unregister := s.eventLoop.registerSystemHandler(
		eventHandlerFunc(func(ctx context.Context, we *WorkflowEvent) {
			if we.EventType == WorkflowEvent_WORKFLOW_FINISHED_IN_SUCCESS ||
				we.EventType == WorkflowEvent_WORKFLOW_FINISHED_IN_ERROR {
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
			events, _ := s.eventRepository.getWorkflowRunEvents(ctx, req.WorkflowRunId)
			for _, event := range events {
				if event.EventType == WorkflowEvent_WORKFLOW_FINISHED_IN_SUCCESS ||
					event.EventType == WorkflowEvent_WORKFLOW_FINISHED_IN_ERROR {
					resultChan <- event
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
			return &GetWorkflowResultResponse{
				Error: errors.Join(fmt.Errorf("context cancelled for GetWorkflowResult %v", req), ctx.Err()).Error(),
			}, nil
		case event := <-resultChan:
			cancel()
			if event.Error != nil {
				return &GetWorkflowResultResponse{
					Error: *event.Error,
				}, nil
			}
			return &GetWorkflowResultResponse{
				Output: event.Output,
			}, nil
		}
	}
}

func (s *goblinsServerImpl) GetActivityRunHistory(ctx context.Context, req *GetActivityRunHistoryRequest) (*GetActivityRunHistoryResponse, error) {
	eventList, err := s.eventRepository.getActivityRunEvents(ctx, req.ActivityRunId)
	if err != nil {
		return nil, err
	}

	return &GetActivityRunHistoryResponse{
		EventList: eventList,
	}, nil
}

func (s *goblinsServerImpl) GetWorkflowRunHistory(ctx context.Context, req *GetWorkflowRunHistoryRequest) (*GetWorkflowRunHistoryResponse, error) {
	eventList, err := s.eventRepository.getWorkflowRunEvents(ctx, req.WorkflowRunId)
	if err != nil {
		return nil, err
	}
	return &GetWorkflowRunHistoryResponse{
		EventList: eventList,
	}, nil
}
