package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/NicolasDutronc/goblins/sdk/worker"
	"github.com/NicolasDutronc/goblins/sdk/workflow"

	"github.com/google/uuid"
)

type GreetActivityInput struct {
	Name string
}

type GreetActivityOutput struct {
	Result string
}

type GreetWorkflowInput struct {
	Name string
}

type GreetWorkflowOutput struct {
	Result string
}

func main() {
	ctx := context.Background()
	worker, err := worker.CreateWorker("example", 10, 5, "localhost:9092", "localhost:9075")
	if err != nil {
		log.Fatalln(err)
	}

	if err := worker.RegisterActivity(ctx, "greetActivity", GreetActivity); err != nil {
		log.Fatalln(err)
	}

	if err := worker.RegisterWorkflow(ctx, "greet", GreetWorkflow); err != nil {
		log.Fatalln(err)
	}

	log.Fatalln(worker.Run(ctx))
}

func GreetActivity(ctx context.Context, input *GreetActivityInput) (*GreetActivityOutput, error) {
	// simulate failure 90% of the time
	if rand.Float64() > 0.1 {
		return nil, fmt.Errorf("no luck this time")
	}
	return &GreetActivityOutput{
		Result: fmt.Sprintf("Hello %s", input.Name),
	}, nil
}

func GreetWorkflow(ctx context.Context, name *GreetWorkflowInput) (*GreetWorkflowOutput, error) {
	activityRunId := uuid.New()
	future := workflow.ExecuteActivity[*GreetWorkflowInput, GreetActivityOutput](ctx, "greetActivity", activityRunId.String(), 10, name)
	result, activityErr, err := future.Get(ctx, 60*time.Second)
	if err != nil {
		return nil, err
	}

	if activityErr != nil {
		log.Printf("activity resulted in error: %v", activityErr)
		return nil, activityErr
	}

	return &GreetWorkflowOutput{
		Result: result.Result,
	}, nil
}
