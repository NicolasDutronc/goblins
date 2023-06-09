// Code generated by mockery v2.28.2. DO NOT EDIT.

package worker

import (
	context "context"

	mock "github.com/stretchr/testify/mock"
)

// MockFunctionExecutor is an autogenerated mock type for the FunctionExecutor type
type MockFunctionExecutor struct {
	mock.Mock
}

type MockFunctionExecutor_Expecter struct {
	mock *mock.Mock
}

func (_m *MockFunctionExecutor) EXPECT() *MockFunctionExecutor_Expecter {
	return &MockFunctionExecutor_Expecter{mock: &_m.Mock}
}

// CheckFunction provides a mock function with given fields: function, functionName
func (_m *MockFunctionExecutor) CheckFunction(function interface{}, functionName string) error {
	ret := _m.Called(function, functionName)

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}, string) error); ok {
		r0 = rf(function, functionName)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockFunctionExecutor_CheckFunction_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'CheckFunction'
type MockFunctionExecutor_CheckFunction_Call struct {
	*mock.Call
}

// CheckFunction is a helper method to define mock.On call
//   - function interface{}
//   - functionName string
func (_e *MockFunctionExecutor_Expecter) CheckFunction(function interface{}, functionName interface{}) *MockFunctionExecutor_CheckFunction_Call {
	return &MockFunctionExecutor_CheckFunction_Call{Call: _e.mock.On("CheckFunction", function, functionName)}
}

func (_c *MockFunctionExecutor_CheckFunction_Call) Run(run func(function interface{}, functionName string)) *MockFunctionExecutor_CheckFunction_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(interface{}), args[1].(string))
	})
	return _c
}

func (_c *MockFunctionExecutor_CheckFunction_Call) Return(_a0 error) *MockFunctionExecutor_CheckFunction_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockFunctionExecutor_CheckFunction_Call) RunAndReturn(run func(interface{}, string) error) *MockFunctionExecutor_CheckFunction_Call {
	_c.Call.Return(run)
	return _c
}

// ExecFunction provides a mock function with given fields: ctx, function, inputBytes
func (_m *MockFunctionExecutor) ExecFunction(ctx context.Context, function interface{}, inputBytes []byte) (interface{}, error) {
	ret := _m.Called(ctx, function, inputBytes)

	var r0 interface{}
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, interface{}, []byte) (interface{}, error)); ok {
		return rf(ctx, function, inputBytes)
	}
	if rf, ok := ret.Get(0).(func(context.Context, interface{}, []byte) interface{}); ok {
		r0 = rf(ctx, function, inputBytes)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(interface{})
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, interface{}, []byte) error); ok {
		r1 = rf(ctx, function, inputBytes)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockFunctionExecutor_ExecFunction_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ExecFunction'
type MockFunctionExecutor_ExecFunction_Call struct {
	*mock.Call
}

// ExecFunction is a helper method to define mock.On call
//   - ctx context.Context
//   - function interface{}
//   - inputBytes []byte
func (_e *MockFunctionExecutor_Expecter) ExecFunction(ctx interface{}, function interface{}, inputBytes interface{}) *MockFunctionExecutor_ExecFunction_Call {
	return &MockFunctionExecutor_ExecFunction_Call{Call: _e.mock.On("ExecFunction", ctx, function, inputBytes)}
}

func (_c *MockFunctionExecutor_ExecFunction_Call) Run(run func(ctx context.Context, function interface{}, inputBytes []byte)) *MockFunctionExecutor_ExecFunction_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(interface{}), args[2].([]byte))
	})
	return _c
}

func (_c *MockFunctionExecutor_ExecFunction_Call) Return(_a0 interface{}, _a1 error) *MockFunctionExecutor_ExecFunction_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockFunctionExecutor_ExecFunction_Call) RunAndReturn(run func(context.Context, interface{}, []byte) (interface{}, error)) *MockFunctionExecutor_ExecFunction_Call {
	_c.Call.Return(run)
	return _c
}

type mockConstructorTestingTNewMockFunctionExecutor interface {
	mock.TestingT
	Cleanup(func())
}

// NewMockFunctionExecutor creates a new instance of MockFunctionExecutor. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewMockFunctionExecutor(t mockConstructorTestingTNewMockFunctionExecutor) *MockFunctionExecutor {
	mock := &MockFunctionExecutor{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
