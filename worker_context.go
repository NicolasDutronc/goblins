package goblins

import (
	"context"

	"google.golang.org/grpc"
)

type taskContextId string

const (
	contextTaskKey          = taskContextId("task")
	contextWorkerContextKey = taskContextId("workerContext")
)

type workerContext struct {
	registry        registry
	conn            *grpc.ClientConn
	eventDispatcher EventDispatcher
}

func (w *workerContext) GetRegistry() registry {
	return w.registry
}

func (w *workerContext) GetClientConn() *grpc.ClientConn {
	return w.conn
}

func (w *workerContext) GetEventDispatcher() EventDispatcher {
	return w.eventDispatcher
}

func GetWorkflowTaskFromContext(ctx context.Context) *Task {
	if ctx.Err() != nil {
		// context is done
		return nil
	}
	task, ok := ctx.Value(contextTaskKey).(*Task)
	if !ok {
		return nil
	}
	return task
}

func GetWorkerContext(ctx context.Context) *workerContext {
	if ctx.Err() != nil {
		// context is done
		return nil
	}
	workerContext, ok := ctx.Value(contextWorkerContextKey).(*workerContext)
	if !ok {
		return nil
	}
	return workerContext
}
