package eventloop

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/NicolasDutronc/goblins/shared/event"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

type EventHandler interface {
	HandleEvent(context.Context, *event.WorkflowEvent)
}

type EventHandlerFunc func(context.Context, *event.WorkflowEvent)

func (f EventHandlerFunc) HandleEvent(ctx context.Context, e *event.WorkflowEvent) {
	f(ctx, e)
}

//go:generate mockery --name EventLoop
type EventLoop interface {
	RegisterSystemHandler(handler EventHandler) func()
	RegisterHandler(id uuid.UUID, callback EventHandler)
	UnregisterHandler(id uuid.UUID)
	Run(ctx context.Context) error
}

type KafkaEventLoop struct {
	consumer       *kafka.Consumer
	systemHandlers []EventHandler
	handlers       map[uuid.UUID]EventHandler
	handlersMutex  sync.Mutex
}

func NewKafkaEventLoop(consumer *kafka.Consumer) EventLoop {
	return &KafkaEventLoop{
		consumer:       consumer,
		systemHandlers: []EventHandler{},
		handlers:       map[uuid.UUID]EventHandler{},
		handlersMutex:  sync.Mutex{},
	}
}

func (e *KafkaEventLoop) RegisterSystemHandler(handler EventHandler) func() {
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

func (e *KafkaEventLoop) RegisterHandler(id uuid.UUID, callback EventHandler) {
	begin := time.Now()
	e.handlersMutex.Lock()
	e.handlers[id] = callback
	e.handlersMutex.Unlock()
	log.Printf("handler registration took %v", time.Since(begin))
}

func (e *KafkaEventLoop) UnregisterHandler(id uuid.UUID) {
	begin := time.Now()
	e.handlersMutex.Lock()
	delete(e.handlers, id)
	e.handlersMutex.Unlock()
	log.Printf("handler unregistration took %v", time.Since(begin))
}

func (e *KafkaEventLoop) Run(ctx context.Context) error {
	log.Println("event loop started")
	if err := e.consumer.Subscribe(event.EventTopic, nil); err != nil {
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

			var event event.WorkflowEvent
			if err := proto.Unmarshal(msg.Value, &event); err != nil {
				log.Printf("failed to deserialize %v : %v\n", msg, err)
				continue
			}

			begin := time.Now()
			e.handlersMutex.Lock()
			for _, handler := range e.systemHandlers {
				handler.HandleEvent(ctx, &event)
			}
			for _, callback := range e.handlers {
				callback.HandleEvent(ctx, &event)
			}
			e.handlersMutex.Unlock()
			log.Printf("executing handler took %v", time.Since(begin))

			if _, err := e.consumer.CommitMessage(msg); err != nil {
				log.Println("error committing msg")
			}
		}
	}
}
