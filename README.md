# goblins

Goblins is a workflow orchestration engine

## Getting started

### Start containers

```bash
docker compose up -d
```

### Create Cassandra tables

The command may fail it is run too early after the container start. In this case just wait a few seconds and try again.

```bash
go run cmd/cassandra/create_tables.go
```

### Create Kafka topics

The command may fail it is run too early after the container start. In this case just wait a few seconds and try again.

```bash
go run cmd/kafka/create_topics.go
```

### Run the server

```bash
go run /cmd/server/main.go
```

### Run an example

In a separate terminal start a worker (or multiple workers):

```bash
go run examples/worker/worker.go
```

In a separate terminal run the client app:

```bash
go run/examples/app.go
```

## How it works

### Overview

Similar to [temporal.io](https://temporal.io/), developers write worker programs and client application programs. Worker programs are composed of activities and workflows. They are both functions and workflows call activities.

When the client app requests the server to execute a workflow, it sends a workflow task on the `tasks` Kafka topic. The workers form a Kafka consumer group which listens to this topic and process tasks as they come in a load balanced fashion.
During the processing of the task, events are sent to the `events` Kafka topic to trace the execution.

- workflow | activity `scheduled` event: by the server once the task has been sent to the workers.
- workflow | activity `started` event: by the worker once it has started processing it.
- workflow | activity `finished` in success | error : by the worker once the task execution has reached a termination point whether it has succeedeed or not.

These events are processed by an event loop that runs in the server in a separate goroutine. Registered handlers are run for each event. 3 handlers are registered at all time:

- the event pusher: it stores events in the Cassandra `events` table. In a multi-server setup (not implemented), servers would elect a leader so that only the leader would run the event pusher to ensure only one server writes events to the database. It can be achieved with an external service like Zookeeper or Etcd.
- the activity retryer: it re-schedules activities that have finished in error and have not yet been retried too many times. This one would also need synchronisation between multiple servers.
- a simple handler that logs every event for debugging purposes.

In addition, when the client requests a workflow's results or a worker running a workflow requests an activity's results, the server registers a temporary handler that listens to the events looking for a `finished` event.

The event might have already been processed by the event loop. So it is also searched for in the database. Whatever finds the event first wins.

The advantage of this design is that events contain all the information. The server is thus stateless. When a server fails and comes back online, it just picks up events where it stopped.

It is the same for workers. If a workflow or an activity is re-scheduled due to a worker failure, the newly assigned worker will ask the server for all the events concerning this workflow or activity run and skip finished steps.

### Data transfer

Communication between the server and workers and between the server and client apps is done through gRPC and protobufs. Developers must design json serializable structs for workflows and activities input and output. Data is then serialized with protobufs.

### Why Cassandra ?

The `events` table is an append only table and the request patterns are known in advance: events are queried by either workflow run or activity run.

### Why Kafka ?

To be honest I don't know the alternatives very well. But Kafka provides data durability and makes it easy to balance the load between workers thanks to consumer groups.

### Testing

This project uses [mockery](https://vektra.github.io/mockery/) to generate mocks for unit testing.

Run `go generate ./...` to regenerate mocks.

Run `go test ./... -short` to run unit tests.

Run `go test ./... -run Integration` to run integration tests.

Run `go test ./... -run EndToEnd` to run end to end tests.

### Reminder

```bash
protoc --go_out=./ --go_opt=paths=source_relative shared/event/event.proto
```

```bash
protoc --go_out=./ --go_opt=paths=source_relative shared/task/task.proto
```

```bash
protoc --go_out=./ --go_opt=paths=source_relative --go-grpc_out=./ --go-grpc_opt=paths=source_relative shared/goblins_service/goblins_service.proto
```
