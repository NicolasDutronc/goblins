package goblins

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

type eventHandler interface {
	handleEvent(context.Context, *WorkflowEvent)
}

type eventHandlerFunc func(context.Context, *WorkflowEvent)

func (f eventHandlerFunc) handleEvent(ctx context.Context, e *WorkflowEvent) {
	f(ctx, e)
}

type eventLoop struct {
	consumer       *kafka.Consumer
	systemHandlers []eventHandler
	handlers       map[uuid.UUID]eventHandler
	handlersMutex  sync.Mutex
}

func newEventLoop(consumer *kafka.Consumer) *eventLoop {
	return &eventLoop{
		consumer:       consumer,
		systemHandlers: []eventHandler{},
		handlers:       map[uuid.UUID]eventHandler{},
		handlersMutex:  sync.Mutex{},
	}
}

func (e *eventLoop) registerSystemHandler(handler eventHandler) func() {
	e.handlersMutex.Lock()
	index := len(e.systemHandlers)
	e.systemHandlers = append(e.systemHandlers, handler)
	e.handlersMutex.Unlock()

	return func() {
		e.handlersMutex.Lock()
		e.systemHandlers = append(e.systemHandlers[:index], e.systemHandlers[index+1:]...)
		e.handlersMutex.Unlock()
	}
}

func (e *eventLoop) RegisterHandler(id uuid.UUID, callback eventHandler) {
	begin := time.Now()
	e.handlersMutex.Lock()
	e.handlers[id] = callback
	e.handlersMutex.Unlock()
	log.Printf("handler registration took %v", time.Since(begin))
}

func (e *eventLoop) UnregisterHandler(id uuid.UUID) {
	begin := time.Now()
	e.handlersMutex.Lock()
	delete(e.handlers, id)
	e.handlersMutex.Unlock()
	log.Printf("handler unregistration took %v", time.Since(begin))
}

func (e *eventLoop) run(ctx context.Context) error {
	log.Println("event loop started")
	if err := e.consumer.Subscribe(eventsTopic, nil); err != nil {
		return errors.Join(fmt.Errorf("worker could not subscribe to event topic"), err)
	}

	for {
		select {
		case <-ctx.Done():
			log.Println("stopping consumer loop")
			return nil
		default:
			msg, err := e.consumer.ReadMessage(10 * time.Millisecond)
			if err != nil {
				// no new message
				if err.(kafka.Error).IsTimeout() {
					continue
				}

				log.Printf("consumer could not read messages: %v\n", err)
				continue
			}

			var event WorkflowEvent
			if err := proto.Unmarshal(msg.Value, &event); err != nil {
				log.Printf("failed to deserialize %v : %v\n", msg, err)
				continue
			}

			begin := time.Now()
			e.handlersMutex.Lock()
			for _, handler := range e.systemHandlers {
				handler.handleEvent(ctx, &event)
			}
			for _, callback := range e.handlers {
				callback.handleEvent(ctx, &event)
			}
			e.handlersMutex.Unlock()
			log.Printf("executing handler took %v", time.Since(begin))

			if _, err := e.consumer.CommitMessage(msg); err != nil {
				log.Println("error committing msg")
			}
		}
	}
}
