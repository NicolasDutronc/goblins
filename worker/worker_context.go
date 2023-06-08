package worker

import (
	"context"

	"github.com/NicolasDutronc/goblins/shared/event"
	"github.com/NicolasDutronc/goblins/shared/task"
	"google.golang.org/grpc"
)

type taskContextId string

const (
	contextTaskKey          = taskContextId("task")
	contextWorkerContextKey = taskContextId("workerContext")
)

type workerContext struct {
	registry        Registry
	conn            *grpc.ClientConn
	eventDispatcher event.EventDispatcher
}

func (w *workerContext) GetRegistry() Registry {
	return w.registry
}

func (w *workerContext) GetClientConn() *grpc.ClientConn {
	return w.conn
}

func (w *workerContext) GetEventDispatcher() event.EventDispatcher {
	return w.eventDispatcher
}

func GetWorkflowTaskFromContext(ctx context.Context) *task.Task {
	if ctx.Err() != nil {
		// context is done
		return nil
	}
	task, ok := ctx.Value(contextTaskKey).(*task.Task)
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
