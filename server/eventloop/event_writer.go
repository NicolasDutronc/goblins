package eventloop

import (
	"context"
	"log"

	"github.com/NicolasDutronc/goblins/shared/event"
)

type EventWriter struct {
	eventRepository event.EventRepository
}

func NewEventWriter(eventRepository event.EventRepository) *EventWriter {
	return &EventWriter{
		eventRepository,
	}
}

func (p *EventWriter) HandleEvent(ctx context.Context, event *event.WorkflowEvent) {
	if err := p.eventRepository.StoreEvent(ctx, event); err != nil {
		log.Printf("could not store event %v", event)
	}
}
