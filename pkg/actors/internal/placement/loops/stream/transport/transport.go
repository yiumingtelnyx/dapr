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

// Package transport abstracts the placement stream wire protocol behind a
// protocol-neutral interface so the placement client loops are identical for
// the v1 (standalone placement service, ReportDaprStatus) and v2 (scheduler,
// ReportActorTypes) protocols.
package transport

import (
	"github.com/dapr/dapr/pkg/actors/internal/placement/loops"
)

// Transport is a single established placement stream. It does not own the
// underlying gRPC connection; use a Session to manage the connection lifetime.
type Transport interface {
	// Recv blocks for the next placement order from the server. It returns
	// an error for malformed or unknown messages, which closes the stream.
	Recv() (*loops.Order, error)

	// SendReport reports the host presence and hosted actor types.
	SendReport(*loops.Report) error

	// SendAck acknowledges a placement order phase.
	SendAck(*loops.Ack) error

	// CloseSend closes only the sending side of the stream. It does not
	// release the underlying connection or guarantee that Recv returns.
	CloseSend() error
}

// Session owns a placement stream and its dedicated gRPC connection.
// The stream loop must call Close when the session ends, before waiting for
// its receive loop. Both placement protocol versions use this contract.
type Session interface {
	Transport

	// Close ends the RPC and closes the underlying connection, releasing
	// its background goroutines and unblocking Recv even if the peer does
	// not respond to CloseSend. It must close the connection even when
	// closing the sending side fails.
	Close() error
}
