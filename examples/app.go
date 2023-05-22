package main

import (
	"context"
	"log"
	"time"

	"github.com/NicolasDutronc/goblins/sdk/client"
	"github.com/NicolasDutronc/goblins/sdk/workflow"
	"github.com/google/uuid"
)

type GreetWorkflowInput struct {
	Name string
}

type GreetWorkflowOutput struct {
	Result string
}

func main() {
	begin := time.Now()
	ctx := context.Background()
	client, err := client.Dial("localhost:9075")
	if err != nil {
		panic(err)
	}

	workflowRunId := uuid.New()
	future := workflow.ExecuteWorkflow[*GreetWorkflowInput, GreetWorkflowOutput](ctx, client, "greet", workflowRunId.String(), &GreetWorkflowInput{Name: "Green Got"})
	result, workflowErr, err := future.Get(ctx, 10*time.Second)
	if err != nil {
		panic(err)
	}
	if workflowErr != nil {
		log.Printf("workflow error: %s\n", workflowErr)
		return
	}

	log.Printf("workflow result after %v: %s\n", time.Since(begin), result.Result)
}
