// Code generated by mockery v2.28.2. DO NOT EDIT.

package server

import (
	context "context"

	mock "github.com/stretchr/testify/mock"
)

// MockServerRegistry is an autogenerated mock type for the ServerRegistry type
type MockServerRegistry struct {
	mock.Mock
}

type MockServerRegistry_Expecter struct {
	mock *mock.Mock
}

func (_m *MockServerRegistry) EXPECT() *MockServerRegistry_Expecter {
	return &MockServerRegistry_Expecter{mock: &_m.Mock}
}

// GetAllActivities provides a mock function with given fields: ctx
func (_m *MockServerRegistry) GetAllActivities(ctx context.Context) ([]string, error) {
	ret := _m.Called(ctx)

	var r0 []string
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context) ([]string, error)); ok {
		return rf(ctx)
	}
	if rf, ok := ret.Get(0).(func(context.Context) []string); ok {
		r0 = rf(ctx)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(ctx)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockServerRegistry_GetAllActivities_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetAllActivities'
type MockServerRegistry_GetAllActivities_Call struct {
	*mock.Call
}

// GetAllActivities is a helper method to define mock.On call
//   - ctx context.Context
func (_e *MockServerRegistry_Expecter) GetAllActivities(ctx interface{}) *MockServerRegistry_GetAllActivities_Call {
	return &MockServerRegistry_GetAllActivities_Call{Call: _e.mock.On("GetAllActivities", ctx)}
}

func (_c *MockServerRegistry_GetAllActivities_Call) Run(run func(ctx context.Context)) *MockServerRegistry_GetAllActivities_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context))
	})
	return _c
}

func (_c *MockServerRegistry_GetAllActivities_Call) Return(_a0 []string, _a1 error) *MockServerRegistry_GetAllActivities_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockServerRegistry_GetAllActivities_Call) RunAndReturn(run func(context.Context) ([]string, error)) *MockServerRegistry_GetAllActivities_Call {
	_c.Call.Return(run)
	return _c
}

// GetAllWorkflows provides a mock function with given fields: ctx
func (_m *MockServerRegistry) GetAllWorkflows(ctx context.Context) ([]string, error) {
	ret := _m.Called(ctx)

	var r0 []string
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context) ([]string, error)); ok {
		return rf(ctx)
	}
	if rf, ok := ret.Get(0).(func(context.Context) []string); ok {
		r0 = rf(ctx)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(ctx)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockServerRegistry_GetAllWorkflows_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetAllWorkflows'
type MockServerRegistry_GetAllWorkflows_Call struct {
	*mock.Call
}

// GetAllWorkflows is a helper method to define mock.On call
//   - ctx context.Context
func (_e *MockServerRegistry_Expecter) GetAllWorkflows(ctx interface{}) *MockServerRegistry_GetAllWorkflows_Call {
	return &MockServerRegistry_GetAllWorkflows_Call{Call: _e.mock.On("GetAllWorkflows", ctx)}
}

func (_c *MockServerRegistry_GetAllWorkflows_Call) Run(run func(ctx context.Context)) *MockServerRegistry_GetAllWorkflows_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context))
	})
	return _c
}

func (_c *MockServerRegistry_GetAllWorkflows_Call) Return(_a0 []string, _a1 error) *MockServerRegistry_GetAllWorkflows_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockServerRegistry_GetAllWorkflows_Call) RunAndReturn(run func(context.Context) ([]string, error)) *MockServerRegistry_GetAllWorkflows_Call {
	_c.Call.Return(run)
	return _c
}

// RegisterActivity provides a mock function with given fields: ctx, activityId
func (_m *MockServerRegistry) RegisterActivity(ctx context.Context, activityId string) error {
	ret := _m.Called(ctx, activityId)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string) error); ok {
		r0 = rf(ctx, activityId)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockServerRegistry_RegisterActivity_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'RegisterActivity'
type MockServerRegistry_RegisterActivity_Call struct {
	*mock.Call
}

// RegisterActivity is a helper method to define mock.On call
//   - ctx context.Context
//   - activityId string
func (_e *MockServerRegistry_Expecter) RegisterActivity(ctx interface{}, activityId interface{}) *MockServerRegistry_RegisterActivity_Call {
	return &MockServerRegistry_RegisterActivity_Call{Call: _e.mock.On("RegisterActivity", ctx, activityId)}
}

func (_c *MockServerRegistry_RegisterActivity_Call) Run(run func(ctx context.Context, activityId string)) *MockServerRegistry_RegisterActivity_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string))
	})
	return _c
}

func (_c *MockServerRegistry_RegisterActivity_Call) Return(_a0 error) *MockServerRegistry_RegisterActivity_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockServerRegistry_RegisterActivity_Call) RunAndReturn(run func(context.Context, string) error) *MockServerRegistry_RegisterActivity_Call {
	_c.Call.Return(run)
	return _c
}

// RegisterWorkflow provides a mock function with given fields: ctx, workflowId
func (_m *MockServerRegistry) RegisterWorkflow(ctx context.Context, workflowId string) error {
	ret := _m.Called(ctx, workflowId)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string) error); ok {
		r0 = rf(ctx, workflowId)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockServerRegistry_RegisterWorkflow_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'RegisterWorkflow'
type MockServerRegistry_RegisterWorkflow_Call struct {
	*mock.Call
}

// RegisterWorkflow is a helper method to define mock.On call
//   - ctx context.Context
//   - workflowId string
func (_e *MockServerRegistry_Expecter) RegisterWorkflow(ctx interface{}, workflowId interface{}) *MockServerRegistry_RegisterWorkflow_Call {
	return &MockServerRegistry_RegisterWorkflow_Call{Call: _e.mock.On("RegisterWorkflow", ctx, workflowId)}
}

func (_c *MockServerRegistry_RegisterWorkflow_Call) Run(run func(ctx context.Context, workflowId string)) *MockServerRegistry_RegisterWorkflow_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string))
	})
	return _c
}

func (_c *MockServerRegistry_RegisterWorkflow_Call) Return(_a0 error) *MockServerRegistry_RegisterWorkflow_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockServerRegistry_RegisterWorkflow_Call) RunAndReturn(run func(context.Context, string) error) *MockServerRegistry_RegisterWorkflow_Call {
	_c.Call.Return(run)
	return _c
}

type mockConstructorTestingTNewMockServerRegistry interface {
	mock.TestingT
	Cleanup(func())
}

// NewMockServerRegistry creates a new instance of MockServerRegistry. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewMockServerRegistry(t mockConstructorTestingTNewMockServerRegistry) *MockServerRegistry {
	mock := &MockServerRegistry{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
