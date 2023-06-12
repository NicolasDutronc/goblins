package end_to_end_test

import (
	"context"
	"fmt"
	"log"
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

type ErrorActivityInput struct {
}

type ErrorActivityOutput struct {
}

type ErrorWorkflowInput struct {
}

type ErrorWorkflowOutput struct {
}

func RunWorker(ctx context.Context, kafkaUrl, serverUrl string) error {
	worker, err := worker.CreateWorker("example", 10, 5, kafkaUrl, serverUrl)
	if err != nil {
		log.Fatalln(err)
	}

	if err := worker.RegisterActivity(ctx, "greetActivity", GreetActivity); err != nil {
		log.Fatalln(err)
	}

	if err := worker.RegisterWorkflow(ctx, "greet", GreetWorkflow); err != nil {
		log.Fatalln(err)
	}

	if err := worker.RegisterActivity(ctx, "errorActivity", ErrorActivity); err != nil {
		log.Fatalln(err)
	}

	if err := worker.RegisterWorkflow(ctx, "errorWorkflow", ErrorWorkflow); err != nil {
		log.Fatalln(err)
	}

	log.Println("worker started")
	return worker.Run(ctx)
}

func GreetActivity(ctx context.Context, input *GreetActivityInput) (*GreetActivityOutput, error) {
	log.Println("starting activity")
	return &GreetActivityOutput{
		Result: fmt.Sprintf("Hello %s", input.Name),
	}, nil
}

func GreetWorkflow(ctx context.Context, name *GreetWorkflowInput) (*GreetWorkflowOutput, error) {
	log.Println("starting workflow")
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

func ErrorActivity(ctx context.Context, input *ErrorActivityInput) (*ErrorActivityOutput, error) {
	log.Println("starting activity")
	return nil, fmt.Errorf("activity error")
}

func ErrorWorkflow(ctx context.Context, input *ErrorWorkflowInput) (*ErrorWorkflowOutput, error) {
	log.Println("starting workflow")
	activityRunId := uuid.New()
	future := workflow.ExecuteActivity[*ErrorActivityInput, ErrorActivityOutput](ctx, "errorActivity", activityRunId.String(), 3, &ErrorActivityInput{})
	_, activityErr, err := future.Get(ctx, 60*time.Second)
	if err != nil {
		return nil, err
	}

	if activityErr != nil {
		log.Printf("activity resulted in error: %v", activityErr)
		return nil, activityErr
	}

	return &ErrorWorkflowOutput{}, nil
}
