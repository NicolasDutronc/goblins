package worker

import (
	"context"
	"encoding/json"
	"fmt"
	reflect "reflect"
)

//go:generate mockery --name FunctionExecutor
type FunctionExecutor interface {
	CheckFunction(function any, functionName string) error
	ExecFunction(ctx context.Context, function any, inputBytes []byte) (any, error)
}

type ReflectionFunctionExecutor struct {
}

func (e *ReflectionFunctionExecutor) CheckFunction(function any, functionName string) error {
	if function == nil {
		return fmt.Errorf("%s cannot be null", functionName)
	}
	fValue := reflect.ValueOf(function)
	if fValue.Kind() != reflect.Func {
		return fmt.Errorf("%s must be a function", functionName)
	}
	if fValue.Type().NumIn() != 2 {
		return fmt.Errorf("%s must have exactly 2 arguments", functionName)
	}
	arg1 := fValue.Type().In(0)
	if !arg1.Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) {
		return fmt.Errorf("first argument of %s must be a context.Context", functionName)
	}
	if fValue.Type().NumOut() != 2 {
		return fmt.Errorf("%s must return 2 values", functionName)
	}
	res2 := fValue.Type().Out(1)
	if !res2.Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		return fmt.Errorf("second return value of %s must be an error", functionName)
	}

	return nil
}

func (e *ReflectionFunctionExecutor) ExecFunction(ctx context.Context, function any, inputBytes []byte) (any, error) {
	fn := reflect.ValueOf(function)
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
