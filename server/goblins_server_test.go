package server_test

import (
	"context"
	"testing"

	"github.com/NicolasDutronc/goblins/server"
	"github.com/NicolasDutronc/goblins/server/eventloop"
	"github.com/NicolasDutronc/goblins/shared/event"
	"github.com/NicolasDutronc/goblins/shared/goblins_service"
	"github.com/NicolasDutronc/goblins/shared/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type GoblinsServerTestSuite struct {
	suite.Suite
	server          server.GoblinsServer
	registry        *server.MockServerRegistry
	taskDipatcher   *task.MockTaskDispatcher
	eventDispatcher *event.MockEventDispatcher
	eventLoop       *eventloop.MockEventLoop
	eventRepository *event.MockEventRepository
}

func (s *GoblinsServerTestSuite) SetupTest() {
	s.registry = server.NewMockServerRegistry(s.T())
	s.taskDipatcher = task.NewMockTaskDispatcher(s.T())
	s.eventDispatcher = event.NewMockEventDispatcher(s.T())
	s.eventLoop = eventloop.NewMockEventLoop(s.T())
	s.eventRepository = event.NewMockEventRepository(s.T())
	s.server = server.NewGoblinsServer(
		s.registry,
		s.eventDispatcher,
		s.taskDipatcher,
		s.eventLoop,
		s.eventRepository,
		nil,
	)
}

func (s *GoblinsServerTestSuite) Test_should_register_activity_in_registry() {
	ctx := context.Background()
	req := &goblins_service.RegisterActivityRequest{
		ActivityId: "test-activity",
	}
	expected := &goblins_service.RegisterActivityResponse{}
	s.registry.On("RegisterActivity", ctx, "test-activity").Return(nil)

	actual, err := s.server.RegisterActivity(ctx, req)

	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), actual)
	assert.Equal(s.T(), expected, actual)
	s.registry.AssertExpectations(s.T())
}

func (s *GoblinsServerTestSuite) Test_should_return_an_error_when_registering_an_activity_returns_an_error() {
	ctx := context.Background()
	req := &goblins_service.RegisterActivityRequest{
		ActivityId: "test-activity",
	}
	s.registry.On("RegisterActivity", ctx, "test-activity").Return(assert.AnError)

	actual, actualErr := s.server.RegisterActivity(ctx, req)

	assert.Nil(s.T(), actual)
	assert.NotNil(s.T(), actualErr)
	assert.Equal(s.T(), assert.AnError, actualErr)
	s.registry.AssertExpectations(s.T())
}

func (s *GoblinsServerTestSuite) Test_should_register_workflow_in_registry() {
	ctx := context.Background()
	req := &goblins_service.RegisterWorkflowRequest{
		WorkflowId: "test-workflow",
	}
	expected := &goblins_service.RegisterWorkflowResponse{}
	s.registry.On("RegisterWorkflow", ctx, "test-workflow").Return(nil)

	actual, err := s.server.RegisterWorkflow(ctx, req)

	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), actual)
	assert.Equal(s.T(), expected, actual)
	s.registry.AssertExpectations(s.T())
}

func (s *GoblinsServerTestSuite) Test_should_return_an_error_when_registering_a_workflow_returns_an_error() {
	ctx := context.Background()
	req := &goblins_service.RegisterWorkflowRequest{
		WorkflowId: "test-workflow",
	}
	s.registry.On("RegisterWorkflow", ctx, "test-workflow").Return(assert.AnError)

	actual, actualErr := s.server.RegisterWorkflow(ctx, req)

	assert.Nil(s.T(), actual)
	assert.NotNil(s.T(), actualErr)
	assert.Equal(s.T(), assert.AnError, actualErr)
	s.registry.AssertExpectations(s.T())
}

func (s *GoblinsServerTestSuite) Test_should_dispatch_a_task_and_an_event_when_scheduling_an_activity() {
	ctx := context.Background()
	req := &goblins_service.ScheduleActivityRequest{
		WorkflowId:         "test-workflow",
		WorkflowRunId:      "test-workflow-run",
		ActivityId:         "test-activity",
		ActivityRunId:      "test-activity-run",
		ActivityMaxRetries: 3,
		ActivityCurrentTry: 0,
		Input:              []byte("input"),
	}
	s.taskDipatcher.On(
		"SendActivityTask",
		"test-workflow",
		"test-workflow-run",
		"test-activity",
		"test-activity-run",
		int32(3),
		int32(0),
		[]byte("input"),
	).Return(nil)
	s.eventDispatcher.On(
		"SendActivityScheduledEvent",
		"test-workflow",
		"test-workflow-run",
		"test-activity",
		"test-activity-run",
		int32(3),
		int32(0),
		[]byte("input"),
	).Return(nil)
	expected := &goblins_service.ScheduleActivityResponse{}

	actual, err := s.server.ScheduleActivity(ctx, req)

	assert.Nil(s.T(), err)
	assert.Equal(s.T(), expected, actual)
	s.taskDipatcher.AssertExpectations(s.T())
	s.eventDispatcher.AssertExpectations(s.T())
}

func (s *GoblinsServerTestSuite) Test_should_dispatch_a_task_and_an_event_when_scheduling_a_workflow() {
	ctx := context.Background()
	req := &goblins_service.ScheduleWorkflowRequest{
		WorkflowId:    "test-workflow",
		WorkflowRunId: "test-workflow-run",
		Input:         []byte("input"),
	}
	s.taskDipatcher.On(
		"SendWorkflowTask",
		"test-workflow",
		"test-workflow-run",
		[]byte("input"),
	).Return(nil)
	s.eventDispatcher.On(
		"SendWorkflowScheduledEvent",
		"test-workflow",
		"test-workflow-run",
		[]byte("input"),
	).Return(nil)
	expected := &goblins_service.ScheduleWorkflowResponse{}

	actual, err := s.server.ScheduleWorkflow(ctx, req)

	assert.Nil(s.T(), err)
	assert.Equal(s.T(), expected, actual)
	s.taskDipatcher.AssertExpectations(s.T())
	s.eventDispatcher.AssertExpectations(s.T())
}

func TestGoblinsServerTestSuite(t *testing.T) {
	suite.Run(t, new(GoblinsServerTestSuite))
}
