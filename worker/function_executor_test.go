package worker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/NicolasDutronc/goblins/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type FunctionExecutorTestSuite struct {
	suite.Suite
	functionExecutor worker.FunctionExecutor
}

func (s *FunctionExecutorTestSuite) SetupTest() {
	s.functionExecutor = &worker.ReflectionFunctionExecutor{}
}

func (s *FunctionExecutorTestSuite) TestRejectNilFunction() {
	actual := s.functionExecutor.CheckFunction(nil, "test")

	assert.EqualError(s.T(), actual, "test cannot be null")
}

func (s *FunctionExecutorTestSuite) TestRejectNotAFunction() {
	actual := s.functionExecutor.CheckFunction("string", "test")

	assert.EqualError(s.T(), actual, "test must be a function")
}

func (s *FunctionExecutorTestSuite) TestRejectFunctionWithLessThanTwoArgs() {
	actual := s.functionExecutor.CheckFunction(func(a any) {}, "test")

	assert.EqualError(s.T(), actual, "test must have exactly 2 arguments")
}

func (s *FunctionExecutorTestSuite) TestRejectFunctionWithMoreThanTwoArgs() {
	actual := s.functionExecutor.CheckFunction(func(a any, b any, c any) {}, "test")

	assert.EqualError(s.T(), actual, "test must have exactly 2 arguments")
}

func (s *FunctionExecutorTestSuite) TestRejectFunctionWithNonContextFirstArg() {
	actual := s.functionExecutor.CheckFunction(func(notContext any, b any) {}, "test")

	assert.EqualError(s.T(), actual, "first argument of test must be a context.Context")
}

func (s *FunctionExecutorTestSuite) TestRejectFunctionWithLessThanTwoReturnValues() {
	actual := s.functionExecutor.CheckFunction(func(a context.Context, b any) any { return nil }, "test")

	assert.EqualError(s.T(), actual, "test must return 2 values")
}

func (s *FunctionExecutorTestSuite) TestRejectFunctionWithMoreThanTwoReturnValues() {
	actual := s.functionExecutor.CheckFunction(func(a context.Context, b any) (any, any, any) { return nil, nil, nil }, "test")

	assert.EqualError(s.T(), actual, "test must return 2 values")
}

func (s *FunctionExecutorTestSuite) TestRejectFunctionWithNonErrorSecondReturnValue() {
	actual := s.functionExecutor.CheckFunction(func(a context.Context, b any) (any, any) { return nil, nil }, "test")

	assert.EqualError(s.T(), actual, "second return value of test must be an error")
}

func (s *FunctionExecutorTestSuite) TestValidFunction() {
	actual := s.functionExecutor.CheckFunction(func(a context.Context, b any) (any, error) { return nil, nil }, "test")

	assert.Nil(s.T(), actual)
}

func (s *FunctionExecutorTestSuite) TestShouldReturnFunctionOutput() {
	type Input struct {
		Field string
	}
	type Output struct {
		Field string
	}
	f := func(ctx context.Context, input *Input) (*Output, error) {
		return &Output{
			input.Field,
		}, nil
	}
	input, err := json.Marshal(&Input{"input"})
	if err != nil {
		s.FailNow(err.Error())
	}

	actual, err := s.functionExecutor.ExecFunction(context.Background(), f, input)

	assert.Nil(s.T(), err)
	assert.IsType(s.T(), &Output{}, actual)
	assert.Equal(s.T(), "input", actual.(*Output).Field)
}

func (s *FunctionExecutorTestSuite) TestShouldReturnError() {
	type Input struct {
	}
	type Output struct {
	}
	f := func(ctx context.Context, input *Input) (*Output, error) {
		return nil, fmt.Errorf("some error")
	}
	input, err := json.Marshal(&Input{})
	if err != nil {
		s.FailNow(err.Error())
	}

	_, actualErr := s.functionExecutor.ExecFunction(context.Background(), f, input)

	assert.NotNil(s.T(), actualErr)
	assert.EqualError(s.T(), actualErr, "some error")
}

func TestFunctionExecutorTestSuite(t *testing.T) {
	suite.Run(t, new(FunctionExecutorTestSuite))
}
