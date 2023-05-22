package goblins

import (
	"context"
	"log"
)

type eventPusher struct {
	eventRepository eventRepository
}

func (p *eventPusher) handleEvent(ctx context.Context, event *WorkflowEvent) {
	if err := p.eventRepository.storeEvent(ctx, event); err != nil {
		log.Printf("could not store event %v", event)
	}
}
