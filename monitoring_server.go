package goblins

import (
	"context"
	"log"
	"time"
)

type monitoringServer struct {
	UnimplementedGoblinsServiceServer
	server goblinsServer
}

func NewMonitoringServer(server goblinsServer) goblinsServer {
	return &monitoringServer{
		server: server,
	}
}

func (s *monitoringServer) start(ctx context.Context) {
	s.server.start(ctx)
}

func (s *monitoringServer) RegisterActivity(ctx context.Context, req *RegisterActivityRequest) (*RegisterActivityResponse, error) {
	begin := time.Now()
	resp, err := s.server.RegisterActivity(ctx, req)
	log.Printf("SERVER - RegisterActivity took %v\n", time.Since(begin))
	return resp, err
}

func (s *monitoringServer) RegisterWorkflow(ctx context.Context, req *RegisterWorkflowRequest) (*RegisterWorkflowResponse, error) {
	begin := time.Now()
	resp, err := s.server.RegisterWorkflow(ctx, req)
	log.Printf("SERVER - RegisterWorkflow took %v\n", time.Since(begin))
	return resp, err
}

func (s *monitoringServer) ScheduleActivity(ctx context.Context, req *ScheduleActivityRequest) (*ScheduleActivityResponse, error) {
	begin := time.Now()
	resp, err := s.server.ScheduleActivity(ctx, req)
	log.Printf("SERVER - ScheduleActivity took %v\n", time.Since(begin))
	return resp, err
}

func (s *monitoringServer) ScheduleWorkflow(ctx context.Context, req *ScheduleWorkflowRequest) (*ScheduleWorkflowResponse, error) {
	begin := time.Now()
	resp, err := s.server.ScheduleWorkflow(ctx, req)
	log.Printf("SERVER - ScheduleWorkflow took %v\n", time.Since(begin))
	return resp, err
}

func (s *monitoringServer) GetActivityResult(ctx context.Context, req *GetActivityResultRequest) (*GetActivityResultResponse, error) {
	begin := time.Now()
	resp, err := s.server.GetActivityResult(ctx, req)
	log.Printf("SERVER - GetActivityResult took %v\n", time.Since(begin))
	return resp, err
}

func (s *monitoringServer) GetWorkflowResult(ctx context.Context, req *GetWorkflowResultRequest) (*GetWorkflowResultResponse, error) {
	begin := time.Now()
	resp, err := s.server.GetWorkflowResult(ctx, req)
	log.Printf("SERVER - GetWorkflowResult took %v\n", time.Since(begin))
	return resp, err
}

func (s *monitoringServer) GetActivityRunHistory(ctx context.Context, req *GetActivityRunHistoryRequest) (*GetActivityRunHistoryResponse, error) {
	begin := time.Now()
	resp, err := s.server.GetActivityRunHistory(ctx, req)
	log.Printf("SERVER - GetActivityRunHistory took %v\n", time.Since(begin))
	return resp, err
}

func (s *monitoringServer) GetWorkflowRunHistory(ctx context.Context, req *GetWorkflowRunHistoryRequest) (*GetWorkflowRunHistoryResponse, error) {
	begin := time.Now()
	resp, err := s.server.GetWorkflowRunHistory(ctx, req)
	log.Printf("SERVER - GetWorkflowRunHistory took %v\n", time.Since(begin))
	return resp, err
}
