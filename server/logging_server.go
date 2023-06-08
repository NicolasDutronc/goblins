package server

import (
	"context"
	"log"
	"time"

	"github.com/NicolasDutronc/goblins/shared/goblins_service"
)

type LoggingServer struct {
	goblins_service.UnimplementedGoblinsServiceServer
	server GoblinsServer
}

func NewLoggingServer(server GoblinsServer) GoblinsServer {
	return &LoggingServer{
		server: server,
	}
}

func (s *LoggingServer) Start(ctx context.Context) {
	s.server.Start(ctx)
}

func (s *LoggingServer) RegisterActivity(ctx context.Context, req *goblins_service.RegisterActivityRequest) (*goblins_service.RegisterActivityResponse, error) {
	begin := time.Now()
	resp, err := s.server.RegisterActivity(ctx, req)
	log.Printf("SERVER - RegisterActivity took %v\n", time.Since(begin))
	return resp, err
}

func (s *LoggingServer) RegisterWorkflow(ctx context.Context, req *goblins_service.RegisterWorkflowRequest) (*goblins_service.RegisterWorkflowResponse, error) {
	begin := time.Now()
	resp, err := s.server.RegisterWorkflow(ctx, req)
	log.Printf("SERVER - RegisterWorkflow took %v\n", time.Since(begin))
	return resp, err
}

func (s *LoggingServer) ScheduleActivity(ctx context.Context, req *goblins_service.ScheduleActivityRequest) (*goblins_service.ScheduleActivityResponse, error) {
	begin := time.Now()
	resp, err := s.server.ScheduleActivity(ctx, req)
	log.Printf("SERVER - ScheduleActivity took %v\n", time.Since(begin))
	return resp, err
}

func (s *LoggingServer) ScheduleWorkflow(ctx context.Context, req *goblins_service.ScheduleWorkflowRequest) (*goblins_service.ScheduleWorkflowResponse, error) {
	begin := time.Now()
	resp, err := s.server.ScheduleWorkflow(ctx, req)
	log.Printf("SERVER - ScheduleWorkflow took %v\n", time.Since(begin))
	return resp, err
}

func (s *LoggingServer) GetActivityResult(ctx context.Context, req *goblins_service.GetActivityResultRequest) (*goblins_service.GetActivityResultResponse, error) {
	begin := time.Now()
	resp, err := s.server.GetActivityResult(ctx, req)
	log.Printf("SERVER - GetActivityResult took %v\n", time.Since(begin))
	return resp, err
}

func (s *LoggingServer) GetWorkflowResult(ctx context.Context, req *goblins_service.GetWorkflowResultRequest) (*goblins_service.GetWorkflowResultResponse, error) {
	begin := time.Now()
	resp, err := s.server.GetWorkflowResult(ctx, req)
	log.Printf("SERVER - GetWorkflowResult took %v\n", time.Since(begin))
	return resp, err
}

func (s *LoggingServer) GetActivityRunHistory(ctx context.Context, req *goblins_service.GetActivityRunHistoryRequest) (*goblins_service.GetActivityRunHistoryResponse, error) {
	begin := time.Now()
	resp, err := s.server.GetActivityRunHistory(ctx, req)
	log.Printf("SERVER - GetActivityRunHistory took %v\n", time.Since(begin))
	return resp, err
}

func (s *LoggingServer) GetWorkflowRunHistory(ctx context.Context, req *goblins_service.GetWorkflowRunHistoryRequest) (*goblins_service.GetWorkflowRunHistoryResponse, error) {
	begin := time.Now()
	resp, err := s.server.GetWorkflowRunHistory(ctx, req)
	log.Printf("SERVER - GetWorkflowRunHistory took %v\n", time.Since(begin))
	return resp, err
}
