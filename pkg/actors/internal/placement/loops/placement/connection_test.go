/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package placement

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/dapr/dapr/pkg/actors/internal/placement/loops"
	streamloop "github.com/dapr/dapr/pkg/actors/internal/placement/loops/stream"
	"github.com/dapr/dapr/pkg/actors/internal/placement/loops/stream/transport"
	transportv1 "github.com/dapr/dapr/pkg/actors/internal/placement/loops/stream/transport/v1"
	v1pb "github.com/dapr/dapr/pkg/proto/placement/v1"
	loopfake "github.com/dapr/kit/events/loop/fake"
)

type connectionConnector struct{ conn *grpc.ClientConn }

func (c *connectionConnector) Connect(context.Context) (*grpc.ClientConn, error) {
	return c.conn, nil
}
func (c *connectionConnector) Address() string { return "test-placement" }

func TestTryConnectClosesFailedConnection(t *testing.T) {
	t.Parallel()

	for _, streamErr := range []error{errors.New("stream unavailable"), context.Canceled} {
		t.Run(streamErr.Error(), func(t *testing.T) {
			t.Parallel()
			for range 10 {
				conn, err := grpc.NewClient("passthrough:///test-placement", grpc.WithTransportCredentials(insecure.NewCredentials()))
				require.NoError(t, err)
				t.Cleanup(func() { conn.Close() })
				p := &placement{
					connector: &connectionConnector{conn: conn},
					streamFactory: func(context.Context, *grpc.ClientConn) (transport.Transport, error) {
						return nil, streamErr
					},
				}
				stream, returnedConn, err := p.tryConnect(t.Context(), t.Context())
				require.ErrorIs(t, err, streamErr)
				assert.Nil(t, stream)
				assert.Nil(t, returnedConn)
				assert.Equal(t, connectivity.Shutdown, conn.GetState(), "failed attempts must release the gRPC channel")
			}
		})
	}
}

// This server deliberately keeps the receive side open after the client
// half-closes. Session teardown must not depend on the peer ending the RPC.
type waitingPlacementServer struct {
	v1pb.UnimplementedPlacementServer
	connected chan struct{}
}

func (s *waitingPlacementServer) ReportDaprStatus(stream v1pb.Placement_ReportDaprStatusServer) error {
	close(s.connected)
	<-stream.Context().Done()
	return stream.Context().Err()
}

func TestPlacementStreamCloseUnblocksRecv(t *testing.T) {
	t.Parallel()
	listener := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { listener.Close() })
	server := grpc.NewServer()
	t.Cleanup(server.Stop)
	service := &waitingPlacementServer{connected: make(chan struct{})}
	v1pb.RegisterPlacementServer(server, service)
	go func() { _ = server.Serve(listener) }()

	conn, err := grpc.NewClient("passthrough:///test-placement",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), time.Second*5)
	defer cancel()
	p := &placement{
		connector: &connectionConnector{conn: conn},
		streamFactory: func(ctx context.Context, conn *grpc.ClientConn) (transport.Transport, error) {
			channel, streamErr := v1pb.NewPlacementClient(conn).ReportDaprStatus(ctx)
			if streamErr != nil {
				return nil, streamErr
			}
			return transportv1.New(transportv1.Options{Channel: channel}), nil
		},
	}
	channel, returnedConn, err := p.tryConnect(ctx, ctx)
	require.NoError(t, err)
	select {
	case <-service.connected:
	case <-ctx.Done():
		t.Fatal("placement server did not receive the stream")
	}
	assert.Same(t, conn, returnedConn)
	assert.NotEqual(t, connectivity.Shutdown, conn.GetState())
	stream := streamloop.New(ctx, streamloop.Options{
		Channel:       channel,
		Conn:          returnedConn,
		PlacementLoop: loopfake.New[loops.EventPlace](),
	})
	runDone := make(chan error, 1)
	go func() { runDone <- stream.Run(ctx) }()
	closeDone := make(chan struct{})
	go func() {
		stream.Close(&loops.Shutdown{Error: errors.New("connection lost")})
		close(closeDone)
	}()
	select {
	case <-closeDone:
		require.NoError(t, ctx.Err(), "shutdown must unblock Recv without waiting for the RPC deadline")
	case <-ctx.Done():
		t.Fatal("stream teardown did not unblock Recv")
	}
	require.NoError(t, <-runDone)
	assert.Equal(t, connectivity.Shutdown, conn.GetState())
}
