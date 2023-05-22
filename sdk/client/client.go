package client

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GoblinsClient struct {
	Conn *grpc.ClientConn
}

func Dial(goblinsServer string) (*GoblinsClient, error) {
	conn, err := grpc.Dial(goblinsServer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &GoblinsClient{
		Conn: conn,
	}, nil
}
