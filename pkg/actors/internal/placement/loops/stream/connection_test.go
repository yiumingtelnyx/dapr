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

package stream

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/dapr/dapr/pkg/actors/internal/placement/loops"
	"github.com/dapr/dapr/pkg/actors/internal/placement/loops/stream/transport"
)

type closingTransport struct {
	transport.Transport
	closeSend func() error
}

func (c *closingTransport) CloseSend() error { return c.closeSend() }

func TestShutdownClosesConnection(t *testing.T) {
	t.Parallel()

	for _, closeErr := range []error{nil, errors.New("stream already closed")} {
		name := "close succeeds"
		if closeErr != nil {
			name = "close fails"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Repeat the session lifecycle to cover reconnects with fresh channels.
			for range 10 {
				conn, err := grpc.NewClient("passthrough:///test-placement", grpc.WithTransportCredentials(insecure.NewCredentials()))
				require.NoError(t, err)
				t.Cleanup(func() { conn.Close() })
				var closed bool
				s := &stream{
					channel: &closingTransport{closeSend: func() error {
						closed = true
						return closeErr
					}},
					conn: conn,
				}
				assert.NotEqual(t, connectivity.Shutdown, conn.GetState(), "an active stream must retain its connection")
				s.handleShutdown(&loops.Shutdown{Error: errors.New("connection lost")})
				assert.True(t, closed)
				assert.Equal(t, connectivity.Shutdown, conn.GetState(), "stream teardown must release the gRPC channel")
			}
		})
	}
}
